# Skill: Concurrency Safety Auditor (Rust)

Esta habilidade orienta a IA na validação técnica de concorrência preemptiva e tratamento seguro de dados compartilhados entre múltiplas threads assíncronas no **RaftKV**.

---

## 🎯 Objetivo de Auditoria

Prevenir condições de corrida (race conditions) em estados voláteis da máquina de estados do Raft (ex: `current_term`, `voted_for`, `role`) e garantir a robustez de múltiplos leitores simultâneos à camada de armazenamento (Key-Value Store).

---

## 🔍 Regras de Verificação Tática

Ao auditar a concorrência assíncrona do RaftKV, analise:

1.  **Race Conditions em Mutexes:**
    *   Garanta que a escrita em variáveis de estado centrais (como o termo de eleição) seja atômica.
    *   Identifique se a leitura seguida de escrita ocorre sob a mesma lock de escrita para evitar race conditions clássicas do tipo "check-then-act".
2.  **Locks Seguros Across Await Points:**
    *   Certifique-se de que guards de locks concorrentes síncronos da biblioteca padrão (`std::sync::MutexGuard`) nunca sejam mantidos ativos entre pontos de `.await`. Fazer isso causa pânico no runtime do Tokio devido à falta da trait `Send` em guards síncronas.
    *   Exija o uso do `tokio::sync::Mutex` ou do `tokio::sync::RwLock` quando o acesso ao recurso precisar ser mantido através de chamadas assíncronas de rede.
3.  **Randomização de Timers de Eleição:**
    *   Audite se o timeout de eleição do nó é recalculado aleatoriamente (ex: entre 150ms e 300ms) a cada ciclo de reset de heartbeat para impossibilitar que dois nós iniciem campanhas na exata mesma janela de milissegundos e causem split-votes permanentes.

---

## 💬 Prompt de Sistema para o Auditor

> "Você é um auditor de concorrência sênior especializado no ecossistema assíncrono do Rust. Analise o código fornecido, mapeie os pontos de sincronização e concorrência cruzada, verifique a corretude de todas as guards de locks em await points e garanta que a execução assíncrona não crie cenários de starvation ou deadlocks de recursos."
