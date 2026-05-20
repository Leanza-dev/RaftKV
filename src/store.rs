use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// O armazenamento de dados em memória.
/// Usamos RwLock (Read-Write Lock) assíncrono para permitir múltiplas leituras
/// concorrentes (alta performance) mas exclusividade na escrita.
///
/// Atualmente é um stub arquitetural aguardando integração com o RaftNode.
/// A camada de armazenamento será conectada ao log de replicação quando
/// o AppendEntries com entradas reais for implementado (ver Roadmap no README).
#[allow(dead_code)]
pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
}

#[allow(dead_code)]
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
