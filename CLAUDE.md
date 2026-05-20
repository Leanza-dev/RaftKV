# CLAUDE.md - RaftKV AI Guide

Este documento estabelece as diretrizes de governança, arquitetura e desenvolvimento para qualquer agente de IA operando no repositório **RaftKV**. Como Tech Lead deste projeto, exijo conformidade absoluta com as regras abaixo.

---

## 🏗️ Diretrizes de Arquitetura (Rust & Tokio)

O **RaftKV** é uma implementação assíncrona do algoritmo de consenso distribuído Raft escrita em Rust de alta performance.

*   **Runtime Assíncrono:** Uso exclusivo de `tokio` para rede e timers de eleição.
*   **Segurança de Concorrência:** Locks assíncronos (`tokio::sync::RwLock` ou `Mutex`) em vez de locks síncronos da stdlib para evitar bloqueio de threads de worker do Tokio.
*   **Paralelismo Resiliente:** Uso de `tokio::task::JoinSet` para dispatch concorrente de RPCs (`RequestVote` e `AppendEntries`) com timeouts estritos.

---

## 🚫 Regras Inquebráveis (Guardrails)

1.  **Exigência Semântica RAII:** Todo recurso do SO (Sockets TCP, Arquivos de Log) deve ser encapsulado em tipos que implementam `Drop` para liberação automática e determinística em caso de aborto de Tasks.
2.  **I/O Bloqueante Proibido:** Nenhuma chamada que faça bloqueio síncrono (ex: leitura de arquivos sem tokio, sockets síncronos, sleep da biblioteca padrão) pode rodar diretamente na thread do executor. Use obrigatoriamente `tokio::task::spawn_blocking`.
3.  **Locks Lock-Free ou Curtos:** Locks concorrentes devem ser adquiridos pelo menor tempo possível. Nunca mantenha um Lock de Escrita (`write().await`) ativo através de chamadas RPC de rede (`await`).
4.  **Tratamento de Erros Explícito:** Proibido o uso de `.unwrap()` ou `.expect()` em código de produção. Trate todos os erros com `Result` e propague-os usando o operador `?` ou trate-os localmente de forma resiliente.

---

## 🛠️ Comandos Frequentes

*   **Build/Check:** `cargo check` (validação rápida de tipos)
*   **Compilar:** `cargo build --release`
*   **Executar Testes:** `cargo test`
*   **Formatação:** `cargo fmt`
*   **Linting:** `cargo clippy -- -D warnings`
*   **Execução Manual:** `cargo run` (requer variáveis de ambiente descritas no README)
