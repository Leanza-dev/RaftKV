mod raft;
mod store;
mod network;

use log::info;
use raft::RaftNode;

#[tokio::main]
async fn main() {
    // Inicializa o logger padrão (env_logger)
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Iniciando Cluster RaftKV...");

    // Mockando os nós do cluster para a demonstração
    let my_id = 3;
    let peers = vec![1, 2, 4, 5];

    let node = RaftNode::new(my_id, peers);

    info!("Node {} inicializado com sucesso. Entrando em loop de consenso...", my_id);
    
    // O node entra no loop infinito rodando o motor de estados Raft (Follower -> Candidate -> Leader)
    node.run().await;
}
