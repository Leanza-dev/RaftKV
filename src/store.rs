use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory data storage.
/// We use an asynchronous RwLock (Read-Write Lock) to allow multiple concurrent
/// reads (high performance) while ensuring exclusive writes.
///
/// Currently, it serves as an architectural stub awaiting integration with RaftNode.
/// The storage layer will be connected to the replication log when
/// AppendEntries with actual entries is implemented (see Roadmap in README).
pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
}

impl KeyValueStore {
    pub fn new() -> Self {
        KeyValueStore {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set(&self, key: String, value: String) {
        let mut store = self.data.write().await;
        store.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let store = self.data.read().await;
        store.get(key).cloned()
    }
}
