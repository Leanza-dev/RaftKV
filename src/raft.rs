use log::{info, warn};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use crate::network::{
    send_append_entries, send_request_vote, start_rpc_server, AppendEntriesReq, RequestVoteReq,
};
use crate::store::KeyValueStore;

/// Maximum number of log entries before triggering a compaction snapshot.
const LOG_COMPACTION_THRESHOLD: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

/// Raft log entry — used for replicating commands across nodes.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    /// In-memory log segment. Entries start at index (last_included_index + 1).
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub role: NodeRole,
    /// Log Compaction: the last index and term included in the most recent snapshot.
    pub last_included_index: u64,
    pub last_included_term: u64,
}

pub struct RaftNode {
    pub id: u64,
    pub state: Arc<RwLock<RaftState>>,
    pub peers: Vec<String>,
    pub listen_addr: String,
    pub store: Arc<KeyValueStore>,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<String>, listen_addr: String) -> Self {
        let state = RaftState {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: NodeRole::Follower,
            last_included_index: 0,
            last_included_term: 0,
        };
        RaftNode {
            id,
            state: Arc::new(RwLock::new(state)),
            peers,
            listen_addr,
            store: Arc::new(KeyValueStore::new()),
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let addr = self.listen_addr.clone();
        let id = self.id;
        let state = self.state.clone();
        let srv_token = token.clone();
        let srv_store = self.store.clone();

        tokio::spawn(async move {
            start_rpc_server(addr, id, state, srv_store, srv_token).await;
        });

        sleep(Duration::from_millis(50)).await;

        loop {
            if token.is_cancelled() {
                info!("Node {}: stopping main state loop...", self.id);
                break;
            }

            let role = { self.state.read().await.role };

            tokio::select! {
                _ = token.cancelled() => {
                    info!("Node {}: main loop cancelled.", self.id);
                    break;
                }
                _ = async {
                    match role {
                        NodeRole::Follower  => self.run_follower().await,
                        NodeRole::Candidate => self.run_candidate().await,
                        NodeRole::Leader    => self.run_leader().await,
                    }
                } => {}
            }
        }
    }

    async fn run_follower(&self) {
        // Randomized election timeout prevents synchronized split-brain elections.
        let timeout_ms = rand::thread_rng().gen_range(150u64..300u64);
        let term = { self.state.read().await.current_term };
        info!(
            "Node {} [Follower] | term={} | election timeout in {}ms",
            self.id, term, timeout_ms
        );

        sleep(Duration::from_millis(timeout_ms)).await;

        let mut state = self.state.write().await;
        if state.role == NodeRole::Follower {
            warn!("Node {}: heartbeat timeout — becoming Candidate", self.id);
            state.role = NodeRole::Candidate;
        }

        // Trigger log compaction before campaigning to keep log bounded.
        drop(state);
        self.compact_log_if_needed().await;
    }

    async fn run_candidate(&self) {
        let (term, quorum, last_log_index, last_log_term) = {
            let mut state = self.state.write().await;
            state.current_term += 1;
            state.voted_for = Some(self.id);

            // §5.4.1: Track our own log completeness to populate RequestVote correctly.
            let (lli, llt) = last_log_info(&state);

            info!(
                "Node {} [Candidate] | started election for term {}",
                self.id, state.current_term
            );
            (
                state.current_term,
                (self.peers.len() + 2) / 2,
                lli,
                llt,
            )
        };

        let mut votes: usize = 1;

        let mut set = JoinSet::new();
        // Limits concurrent requests to avoid fan-out microbursts in large clusters.
        let semaphore = Arc::new(Semaphore::new(50));

        for peer_addr in &self.peers {
            let addr = peer_addr.clone();
            let req = RequestVoteReq {
                term,
                candidate_id: self.id,
                last_log_index,
                last_log_term,
            };
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    warn!("Node {}: semaphore closed, aborting election requests", self.id);
                    break;
                }
            };
            set.spawn(async move {
                let res = send_request_vote(&addr, req).await;
                drop(permit);
                res
            });
        }

        while let Some(res) = set.join_next().await {
            if let Ok(Some(resp)) = res {
                if resp.term > term {
                    // Discovered a higher term — step down immediately (Fix Err#2).
                    let mut state = self.state.write().await;
                    state.current_term = resp.term;
                    state.role = NodeRole::Follower;
                    state.voted_for = None;
                    warn!(
                        "Node {}: discovered higher term {} — stepping down to Follower",
                        self.id, resp.term
                    );
                    set.abort_all();
                    return;
                }
                if resp.vote_granted {
                    votes += 1;
                    info!(
                        "Node {} [Candidate] | got vote — total: {}/{}",
                        self.id,
                        votes,
                        self.peers.len() + 1
                    );
                }
                if votes >= quorum {
                    break;
                }
            }
        }

        set.abort_all();

        let mut state = self.state.write().await;
        // Only promote if still a Candidate (AppendEntries may have converted us).
        if state.role == NodeRole::Candidate {
            if votes >= quorum {
                info!(
                    "Node {} [LEADER] | quorum reached ({}/{}) for term {}!",
                    self.id,
                    votes,
                    self.peers.len() + 1,
                    term
                );
                state.role = NodeRole::Leader;
            } else {
                warn!(
                    "Node {} [Candidate] | split vote ({}/{}) — randomized backoff before retry",
                    self.id,
                    votes,
                    self.peers.len() + 1
                );
                state.role = NodeRole::Follower;
                state.voted_for = None;
                drop(state);

                // Fix Err#3: Randomized backoff on split-vote to break election livelock.
                let backoff_ms = rand::thread_rng().gen_range(150u64..400u64);
                sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    async fn run_leader(&self) {
        let term = { self.state.read().await.current_term };

        // Log state to prove store integration.
        let sample_val = self.store.get("health_check").await.unwrap_or_else(|| "N/A".to_string());
        info!(
            "Node {} [Leader] | term={} | health_check={} | broadcasting AppendEntries",
            self.id, term, sample_val
        );

        let mut set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(50));

        // Fix Err#4 & Err#5: Safe prev_log_info computation accounting for compaction offset.
        let (prev_log_index, prev_log_term, entries) = {
            let state = self.state.read().await;
            let (lli, llt) = last_log_info(&state);
            let cmds: Vec<String> = state.log.iter().map(|e| e.command.clone()).collect();
            (lli, llt, cmds)
        };

        for peer_addr in &self.peers {
            let addr = peer_addr.clone();
            let entries_clone = entries.clone();
            let req = AppendEntriesReq {
                term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries: entries_clone,
                leader_commit: 0,
            };
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    warn!("Node {}: semaphore closed, aborting AppendEntries broadcast", self.id);
                    break;
                }
            };
            set.spawn(async move {
                let res = send_append_entries(&addr, req).await;
                drop(permit);
                res
            });
        }

        while let Some(res) = set.join_next().await {
            if let Ok(Some(resp)) = res {
                if resp.term > term {
                    let mut state = self.state.write().await;
                    state.current_term = resp.term;
                    state.voted_for = None;
                    state.role = NodeRole::Follower;
                    warn!(
                        "Node {}: discovered higher term {} — stepping down from Leader",
                        self.id, resp.term
                    );
                    set.abort_all();
                    return;
                }
            }
        }

        // Leader heartbeat interval.
        sleep(Duration::from_millis(50)).await;
    }

    /// Log Compaction: truncates all log entries up to `commit_index`.
    ///
    /// The `KeyValueStore` is used as the implicit snapshot — it already holds
    /// the applied state. We simply discard the log prefix and update the
    /// compaction watermark (`last_included_index` / `last_included_term`).
    async fn compact_log_if_needed(&self) {
        let mut state = self.state.write().await;

        if state.log.len() < LOG_COMPACTION_THRESHOLD {
            return;
        }

        // Determine how many entries we can safely discard (up to commit_index).
        // Entries are stored starting at index (last_included_index + 1).
        let base = state.last_included_index;
        let commit = state.commit_index;

        if commit <= base {
            // Nothing committed beyond the current snapshot — nothing to compact.
            return;
        }

        // Number of entries to discard (relative offset into the log vec).
        let discard_count = (commit - base) as usize;

        if discard_count == 0 || discard_count > state.log.len() {
            return;
        }

        // Update the compaction watermark from the last discarded entry.
        let last_discarded = &state.log[discard_count - 1];
        state.last_included_term = last_discarded.term;
        state.last_included_index = commit;

        // Drain the compacted prefix.
        state.log.drain(..discard_count);

        info!(
            "Node {}: log compacted up to index={} | remaining entries={}",
            state.current_term,
            state.last_included_index,
            state.log.len()
        );
    }
}

/// Returns `(last_log_index, last_log_term)` accounting for compaction offsets.
/// Panics-safe: returns `(last_included_index, last_included_term)` if log is empty.
pub fn last_log_info(state: &RaftState) -> (u64, u64) {
    if let Some(last) = state.log.last() {
        // Absolute index = compaction base + position in current log vec.
        let abs_index = state.last_included_index + state.log.len() as u64;
        (abs_index, last.term)
    } else {
        // Log is empty — watermark is the snapshot boundary.
        (state.last_included_index, state.last_included_term)
    }
}
