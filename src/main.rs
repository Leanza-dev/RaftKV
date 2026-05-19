mod raft;
mod store;
mod network;

use std::env;
use log::info;
use raft::RaftNode;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // Read configuration from environment variables
    // NODE_ID:      numeric identifier for this node (e.g. "1")
    // LISTEN_ADDR:  address this node binds its RPC server to (e.g. "0.0.0.0:8001")
    // PEERS:        comma-separated peer RPC addresses (e.g. "node2:8002,node3:8003")
    let node_id: u64 = env::var("NODE_ID")
        .expect("NODE_ID env var is required")
        .parse()
        .expect("NODE_ID must be a valid u64");

    let listen_addr = env::var("LISTEN_ADDR")
        .expect("LISTEN_ADDR env var is required");

    let peers: Vec<String> = env::var("PEERS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();

    info!("Starting RaftKV Node {} | listen={} | peers={:?}", node_id, listen_addr, peers);

    let node = RaftNode::new(node_id, peers, listen_addr);
    node.run().await;
}
