mod network;
mod raft;
mod store;

use log::info;
use raft::RaftNode;
use std::env;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let node_id: u64 = env::var("NODE_ID")
        .expect("NODE_ID env var is required")
        .parse()
        .expect("NODE_ID must be a valid u64");

    let listen_addr = env::var("LISTEN_ADDR").expect("LISTEN_ADDR env var is required");

    let peers: Vec<String> = env::var("PEERS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();

    info!(
        "Starting RaftKV Node {} | listen={} | peers={:?}",
        node_id, listen_addr, peers
    );

    let token = CancellationToken::new();
    let node = RaftNode::new(node_id, peers, listen_addr);

    let run_token = token.clone();
    let handle = tokio::spawn(async move {
        node.run(run_token).await;
    });

    // Graceful Shutdown
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("SIGINT received. Shutting down gracefully...");
            token.cancel();
            let _ = handle.await;
            info!("Graceful shutdown complete");
        }
        Err(err) => {
            log::error!("Unable to listen for shutdown signal: {}", err);
        }
    }
}
