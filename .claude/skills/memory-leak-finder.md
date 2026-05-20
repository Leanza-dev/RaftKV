# Skill: Memory Leak Finder (Rust/Tokio)

Esta habilidade orienta a IA na auditoria do uso de memória dinâmica, alocação de buffers de rede e gerenciamento do ciclo de vida de tasks assíncronas no ecossistema Rust + Tokio do **RaftKV**.

---

## 🎯 Objetivo de Auditoria

Evitar acúmulo de tarefas órfãs e conexões TCP ativas suspensas que drenem memória RAM e portas de rede do Sistema Operacional ao rodar o cluster distribuído por longos períodos.

---

## 🔍 Regras de Verificação Tática

Ao auditar código Rust neste projeto, procure ativamente:

1.  **Garantia de Aborto das Futures no Tokio:**
    *   Sempre que disparar threads de rede assíncronas concorrentes de forma dinâmica, utilize o `tokio::task::JoinSet`.
    *   Verifique se o aborto (`set.abort_all()`) está sendo chamado no instante em que o quorum é atingido ou quando o nó muda de estado.
2.  **Ciclo de Vida de Conexões TCP (`Drop` Semantics):**
    *   Certifique-se de que buffers dinâmicos de transmissão de bytes (ex: `Vec<u8>`) ou streams (`tokio::net::TcpStream`) não fiquem retidos em loops infinitos.
    *   O ciclo de processamento deve ser encapsulado em estruturas que garantam que, ao sair do escopo ou ao ter sua task cancelada, o socket seja destruído de forma determinística.
3.  **Locks Bloqueantes no Heap:**
    *   Evite alocar estruturas pesadas dentro de instâncias de `std::sync::Mutex` em loops críticos de rede.
    *   Prefira o `tokio::sync::RwLock` assíncrono e verifique se as guards (`write().await` ou `read().await`) são descartadas no menor tempo possível para não reter ponteiros no heap.

---

## 💬 Prompt de Sistema para o Auditor

> "Você é um especialista em auditoria de gerenciamento de recursos de baixo nível em Rust. Revise o código anexado procurando por alocações suspensas, goroutines/tasks órfãs do Tokio que continuam rodando após a eleição, ou referências mantidas desnecessariamente além dos limites das funções. Recomende o uso de escopos explícitos de descarte (`drop(...)`) e estruturas RAII seguras."
