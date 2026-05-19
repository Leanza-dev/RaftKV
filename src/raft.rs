use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use rand::Rng;
use log::{info, warn};

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
    /// Arc<RwLock> over current_term, shared with the RPC server
    pub current_term: Arc<RwLock<u64>>,
    /// Arc<RwLock> over voted_for, shared with the RPC server
    pub voted_for: Arc<RwLock<Option<u64>>>,
    pub log: Vec<LogEntry>,
    pub role: Arc<RwLock<NodeRole>>,
    /// Peer addresses as "host:port" strings (e.g. "node2:8002")
    pub peers: Vec<String>,
    pub listen_addr: String,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<String>, listen_addr: String) -> Self {
        RaftNode {
            id,
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Vec::new(),
            role: Arc::new(RwLock::new(NodeRole::Follower)),
            peers,
            listen_addr,
        }
    }

    /// Starts the RPC listener in a background task, then runs the main state loop.
    pub async fn run(&self) {
        // Spawn the TCP RPC server as a background task
        let addr = self.listen_addr.clone();
        let id = self.id;
        let vf = self.voted_for.clone();
        let ct = self.current_term.clone();
        tokio::spawn(async move {
            start_rpc_server(addr, id, vf, ct).await;
        });

        // Small delay to let the server bind before sending RPCs
        sleep(Duration::from_millis(50)).await;

        // Main state machine loop
        loop {
            let role = *self.role.read().await;
            match role {
                NodeRole::Follower  => self.run_follower().await,
                NodeRole::Candidate => self.run_candidate().await,
                NodeRole::Leader    => self.run_leader().await,
            }
        }
    }

    async fn run_follower(&self) {
        let timeout_ms = rand::thread_rng().gen_range(150u64..300u64);
        info!("Node {} [Follower] | term={} | election timeout in {}ms",
            self.id, *self.current_term.read().await, timeout_ms);
        sleep(Duration::from_millis(timeout_ms)).await;

        // No heartbeat received → promote to candidate
        let mut role = self.role.write().await;
        if *role == NodeRole::Follower {
            warn!("Node {}: heartbeat timeout — becoming Candidate", self.id);
            *role = NodeRole::Candidate;
        }
    }

    async fn run_candidate(&self) {
        // Increment term and vote for self
        {
            let mut ct = self.current_term.write().await;
            *ct += 1;
            let mut vf = self.voted_for.write().await;
            *vf = Some(self.id);
            info!("Node {} [Candidate] | started election for term {}", self.id, *ct);
        }

        let term = *self.current_term.read().await;
        let quorum = (self.peers.len() + 2) / 2; // majority of total cluster
        let mut votes: usize = 1; // self-vote

        // Send RequestVote RPCs to all peers concurrently
        let mut handles = Vec::new();
        for peer_addr in &self.peers {
            let addr = peer_addr.clone();
            let req = RequestVoteReq {
                term,
                candidate_id: self.id,
                last_log_index: 0,
                last_log_term: 0,
            };
            handles.push(tokio::spawn(async move {
                send_request_vote(&addr, req).await
            }));
        }

        for handle in handles {
            if let Ok(Some(resp)) = handle.await {
                if resp.vote_granted {
                    votes += 1;
                    info!("Node {} [Candidate] | got vote — total: {}/{}", self.id, votes, self.peers.len() + 1);
                }
                // If a peer has a higher term, step down
                if resp.term > term {
                    let mut ct = self.current_term.write().await;
                    *ct = resp.term;
                    let mut role = self.role.write().await;
                    *role = NodeRole::Follower;
                    warn!("Node {}: discovered higher term {} — stepping down", self.id, resp.term);
                    return;
                }
            }
        }

        let mut role = self.role.write().await;
        if votes >= quorum {
            info!("Node {} [LEADER] | quorum reached ({}/{}) — I am the new LEADER for term {}!",
                self.id, votes, self.peers.len() + 1, term);
            *role = NodeRole::Leader;
        } else {
            warn!("Node {} [Candidate] | election failed ({}/{}) — back to Follower",
                self.id, votes, self.peers.len() + 1);
            *role = NodeRole::Follower;
            // Clear vote to allow voting in next term
            let mut vf = self.voted_for.write().await;
            *vf = None;
        }
    }

    async fn run_leader(&self) {
        let term = *self.current_term.read().await;
        info!("Node {} [Leader] | sending AppendEntries (heartbeat) for term {}", self.id, term);

        // Send AppendEntries heartbeats to all peers concurrently
        let mut handles = Vec::new();
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
            handles.push(tokio::spawn(async move {
                send_append_entries(&addr, req).await
            }));
        }

        for handle in handles {
            if let Ok(Some(resp)) = handle.await {
                if resp.term > term {
                    // Higher term found — step down
                    let mut ct = self.current_term.write().await;
                    *ct = resp.term;
                    let mut role = self.role.write().await;
                    *role = NodeRole::Follower;
                    warn!("Node {}: discovered higher term {} — stepping down from Leader", self.id, resp.term);
                    return;
                }
            }
        }

        // Heartbeat interval: 50ms
        sleep(Duration::from_millis(50)).await;
    }
}
