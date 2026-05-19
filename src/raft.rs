use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use rand::Rng;
use log::{info, warn};

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
    pub peers: Vec<u64>,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        RaftNode {
            id,
            state: Arc::new(RwLock::new(RaftState {
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
                role: NodeRole::Follower,
            })),
            peers,
        }
    }

    /// O coração da máquina de estados do Raft. 
    /// Roda continuamente gerenciando os timeouts de eleição.
    pub async fn run(&self) {
        loop {
            let role = {
                let state = self.state.read().await;
                state.role
            };

            match role {
                NodeRole::Follower => self.run_follower().await,
                NodeRole::Candidate => self.run_candidate().await,
                NodeRole::Leader => self.run_leader().await,
            }
        }
    }

    async fn run_follower(&self) {
        let timeout = rand::thread_rng().gen_range(150..300);
        info!("Node {} (Follower): Aguardando {}ms", self.id, timeout);
        sleep(Duration::from_millis(timeout)).await;
        
        // Timeout atingido, transita para Candidate
        let mut state = self.state.write().await;
        if state.role == NodeRole::Follower {
            warn!("Node {} heartbeat timeout. Tornando-se Candidate.", self.id);
            state.role = NodeRole::Candidate;
        }
    }

    async fn run_candidate(&self) {
        {
            let mut state = self.state.write().await;
            state.current_term += 1;
            state.voted_for = Some(self.id);
            info!("Node {} iniciou eleição para term {}", self.id, state.current_term);
        }

        // Simulação de RequestVote RPC
        let quorum = (self.peers.len() + 1) / 2 + 1;
        let mut votes = 1; // Vota em si mesmo

        // Na vida real, enviamos requests via rede (gRPC) assincronamente
        for peer in &self.peers {
            info!("Node {} pedindo voto para Node {}", self.id, peer);
            votes += 1; // Simplificado para fins de demonstração
        }

        if votes >= quorum {
            let mut state = self.state.write().await;
            info!("Node {} recebeu quorum ({}/{}). É o novo LEADER!", self.id, votes, self.peers.len() + 1);
            state.role = NodeRole::Leader;
        } else {
            sleep(Duration::from_millis(150)).await;
            let mut state = self.state.write().await;
            state.role = NodeRole::Follower; // Retorna a Follower se a eleição falhar
        }
    }

    async fn run_leader(&self) {
        info!("Node {} (Leader): Enviando Heartbeats (AppendEntries)", self.id);
        sleep(Duration::from_millis(50)).await;
        // Na vida real, o líder manda logs periodicamente para manter autoridade
    }
}
