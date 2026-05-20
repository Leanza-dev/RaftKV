use crate::raft::RaftState;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum RpcMessage {
    RequestVote(RequestVoteReq),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntriesReq),
    AppendEntriesResp(AppendEntriesResp),
}

pub async fn send_request_vote(peer_addr: &str, req: RequestVoteReq) -> Option<RequestVoteResp> {
    let timeout = Duration::from_millis(150);
    let stream = match tokio::time::timeout(timeout, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            warn!("RequestVote: failed to connect to {}: {}", peer_addr, e);
            return None;
        }
        Err(_) => {
            warn!("RequestVote: connection timeout to {}", peer_addr);
            return None;
        }
    };
    send_rpc_and_recv::<RequestVoteResp>(stream, RpcMessage::RequestVote(req)).await
}

pub async fn send_append_entries(
    peer_addr: &str,
    req: AppendEntriesReq,
) -> Option<AppendEntriesResp> {
    let timeout = Duration::from_millis(100);
    let stream = match tokio::time::timeout(timeout, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            warn!("AppendEntries: failed to connect to {}: {}", peer_addr, e);
            return None;
        }
        Err(_) => {
            warn!("AppendEntries: connection timeout to {}", peer_addr);
            return None;
        }
    };
    send_rpc_and_recv::<AppendEntriesResp>(stream, RpcMessage::AppendEntries(req)).await
}

async fn send_rpc_and_recv<R>(mut stream: TcpStream, msg: RpcMessage) -> Option<R>
where
    R: for<'de> Deserialize<'de>,
{
    let payload = bincode::serialize(&msg).ok()?;
    let len = payload.len() as u32;

    stream.write_all(&len.to_be_bytes()).await.ok()?;
    stream.write_all(&payload).await.ok()?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    if resp_len > 1024 * 1024 {
        warn!("Response payload too large: {} bytes", resp_len);
        return None;
    }

    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await.ok()?;
    bincode::deserialize(&resp_buf).ok()
}

pub async fn start_rpc_server(
    listen_addr: String,
    node_id: u64,
    state: std::sync::Arc<tokio::sync::RwLock<RaftState>>,
    token: CancellationToken,
) {
    let listener = match TcpListener::bind(&listen_addr).await {
        Ok(l) => {
            info!("Node {}: RPC server listening on {}", node_id, listen_addr);
            l
        }
        Err(e) => {
            error!(
                "Node {}: failed to bind RPC listener on {}: {}",
                node_id, listen_addr, e
            );
            return;
        }
    };

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("Node {}: shutting down RPC server", node_id);
                break;
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, peer)) => {
                        let st = state.clone();
                        tokio::spawn(async move {
                            handle_rpc_connection(stream, peer.to_string(), node_id, st).await;
                        });
                    }
                    Err(e) => {
                        error!("Node {}: accept error: {}", node_id, e);
                    }
                }
            }
        }
    }
}

async fn handle_rpc_connection(
    mut stream: TcpStream,
    peer: String,
    node_id: u64,
    state: std::sync::Arc<tokio::sync::RwLock<RaftState>>,
) {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let req_len = u32::from_be_bytes(len_buf) as usize;

    if req_len > 1024 * 1024 {
        warn!(
            "Node {}: payload too large from {}: {} bytes (OOM Protection)",
            node_id, peer, req_len
        );
        return;
    }

    let mut req_buf = vec![0u8; req_len];
    if stream.read_exact(&mut req_buf).await.is_err() {
        return;
    }

    let msg: RpcMessage = match bincode::deserialize(&req_buf) {
        Ok(m) => m,
        Err(e) => {
            warn!("Node {}: malformed RPC from {}: {}", node_id, peer, e);
            return;
        }
    };

    let resp_msg = match msg {
        RpcMessage::RequestVote(req) => {
            // Atomic mutation logic
            let mut st = state.write().await;

            let vote_granted = req.term >= st.current_term
                && (st.voted_for.is_none() || st.voted_for == Some(req.candidate_id));
            if vote_granted {
                st.voted_for = Some(req.candidate_id);
                if req.term > st.current_term {
                    st.current_term = req.term;
                }
                info!(
                    "Node {}: granted vote to Node {} for term {}",
                    node_id, req.candidate_id, req.term
                );
            } else {
                info!(
                    "Node {}: denied vote to Node {} (already voted or stale term)",
                    node_id, req.candidate_id
                );
            }
            RpcMessage::RequestVoteResp(RequestVoteResp {
                term: st.current_term,
                vote_granted,
            })
        }
        RpcMessage::AppendEntries(req) => {
            let mut st = state.write().await;
            let success = req.term >= st.current_term;
            if success {
                if req.term > st.current_term {
                    st.current_term = req.term;
                    st.voted_for = None;
                }
                // When append entries is received successfully, we should step down to Follower
                // if we are a Candidate. This is handled gracefully next time the main loop
                // evaluates the state, but we ensure consistency here.
                st.role = crate::raft::NodeRole::Follower;
            }
            RpcMessage::AppendEntriesResp(AppendEntriesResp {
                term: st.current_term,
                success,
            })
        }
        _ => {
            warn!("Node {}: unexpected RPC type from {}", node_id, peer);
            return;
        }
    };

    if let Ok(payload) = bincode::serialize(&resp_msg) {
        let len = payload.len() as u32;
        let _ = stream.write_all(&len.to_be_bytes()).await;
        let _ = stream.write_all(&payload).await;
    }
}
