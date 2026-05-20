use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// O armazenamento de dados em memória.
/// Usamos RwLock (Read-Write Lock) assíncrono para permitir múltiplas leituras
/// concorrentes (alta performance) mas exclusividade na escrita.
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
