use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    // Em produção real, este seria um Vec<LogEntry>
    // Simplificando o payload da rede para este MVP
    pub entries: Vec<String>,
    pub leader_commit: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteReq {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteResp {
    pub term: u64,
    pub vote_granted: bool,
}

/// No mundo real, usaríamos um framework gRPC como o Tonic ou um socket TCP/UDP bruto.
/// Aqui definimos a interface que o Node deve implementar.
pub trait RaftNetwork {
    fn send_append_entries(&self, peer_id: u64, req: AppendEntriesReq);
    fn send_request_vote(&self, peer_id: u64, req: RequestVoteReq);
}
