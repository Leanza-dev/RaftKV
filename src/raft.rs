use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, Duration};
use tokio::task::JoinSet;
use rand::Rng;
use log::{info, warn};
use tokio_util::sync::CancellationToken;

use crate::network::{
    send_request_vote, send_append_entries,
    start_rpc_server,
    RequestVoteReq, AppendEntriesReq,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    pub role: NodeRole,
}

pub struct RaftNode {
    pub id: u64,
    pub state: Arc<RwLock<RaftState>>,
    pub peers: Vec<String>,
    pub listen_addr: String,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<String>, listen_addr: String) -> Self {
        let state = RaftState {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: NodeRole::Follower,
        };
        RaftNode {
            id,
            state: Arc::new(RwLock::new(state)),
            peers,
            listen_addr,
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let addr = self.listen_addr.clone();
        let id = self.id;
        let state = self.state.clone();
        let srv_token = token.clone();
        
        tokio::spawn(async move {
            start_rpc_server(addr, id, state, srv_token).await;
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
        let timeout_ms = rand::thread_rng().gen_range(150..300);
        let term = { self.state.read().await.current_term };
        info!("Node {} [Follower] | term={} | election timeout in {}ms", self.id, term, timeout_ms);
        
        sleep(Duration::from_millis(timeout_ms)).await;

        let mut state = self.state.write().await;
        if state.role == NodeRole::Follower {
            warn!("Node {}: heartbeat timeout — becoming Candidate", self.id);
            state.role = NodeRole::Candidate;
        }
    }

    async fn run_candidate(&self) {
        let (term, quorum) = {
            let mut state = self.state.write().await;
            state.current_term += 1;
            state.voted_for = Some(self.id);
            info!("Node {} [Candidate] | started election for term {}", self.id, state.current_term);
            (state.current_term, (self.peers.len() + 2) / 2)
        };

        let mut votes: usize = 1;

        let mut set = JoinSet::new();
        // Limita requisições concorrentes para evitar fan-out microbursts em grandes clusters
        let semaphore = Arc::new(Semaphore::new(50));

        for peer_addr in &self.peers {
            let addr = peer_addr.clone();
            let req = RequestVoteReq {
                term,
                candidate_id: self.id,
                last_log_index: 0,
                last_log_term: 0,
            };
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            set.spawn(async move {
                let res = send_request_vote(&addr, req).await;
                drop(permit); // Libera o slot no pool
                res
            });
        }

        while let Some(res) = set.join_next().await {
            if let Ok(Some(resp)) = res {
                if resp.vote_granted {
                    votes += 1;
                    info!("Node {} [Candidate] | got vote — total: {}/{}", self.id, votes, self.peers.len() + 1);
                }
                if resp.term > term {
                    let mut state = self.state.write().await;
                    state.current_term = resp.term;
                    state.role = NodeRole::Follower;
                    warn!("Node {}: discovered higher term {} — stepping down", self.id, resp.term);
                    return;
                }
                if votes >= quorum {
                    // Early exit if we reached quorum!
                    break;
                }
            }
        }
        
        // Abort any remaining pending vote requests
        set.abort_all();

        let mut state = self.state.write().await;
        // Check if role is still Candidate (might have received AppendEntries while awaiting)
        if state.role == NodeRole::Candidate {
            if votes >= quorum {
                info!("Node {} [LEADER] | quorum reached ({}/{}) — I am the new LEADER for term {}!", self.id, votes, self.peers.len() + 1, term);
                state.role = NodeRole::Leader;
                
                // Isolamento de I/O pesado (Write-Ahead Log fsync) para evitar Thread Starvation
                let _ = tokio::task::spawn_blocking(move || {
                    // Simulação de fsync em disco
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }).await;
            } else {
                warn!("Node {} [Candidate] | election failed ({}/{}) — back to Follower", self.id, votes, self.peers.len() + 1);
                state.role = NodeRole::Follower;
                state.voted_for = None;
            }
        }
    }

    async fn run_leader(&self) {
        let term = { self.state.read().await.current_term };
        info!("Node {} [Leader] | sending AppendEntries (heartbeat) for term {}", self.id, term);

        let mut set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(50)); // Controle de microbursts

        for peer_addr in &self.peers {
            let addr = peer_addr.clone();
            let req = AppendEntriesReq {
                term,
                leader_id: self.id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            };
            let permit = semaphore.clone().acquire_owned().await.unwrap();
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
                    state.role = NodeRole::Follower;
                    warn!("Node {}: discovered higher term {} — stepping down from Leader", self.id, resp.term);
                    return;
                }
            }
        }

        sleep(Duration::from_millis(50)).await;
    }
}
