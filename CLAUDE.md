# CLAUDE.md - RaftKV AI Guide

This document establishes the governance, architectural, and developmental guidelines for any AI agent operating within the **RaftKV** repository. As the Tech Lead of this project, absolute compliance with the rules below is mandatory.

---

## 🏗️ Architectural Guidelines (Rust & Tokio)

**RaftKV** is an asynchronous implementation of the Raft distributed consensus algorithm, written in high-performance Rust.

*   **Asynchronous Runtime:** Exclusive use of `tokio` for networking and election timers.
*   **Concurrency Safety:** Asynchronous locks (`tokio::sync::RwLock` or `Mutex`) instead of stdlib synchronous locks to prevent blocking Tokio worker threads.
*   **Resilient Parallelism:** Use of `tokio::task::JoinSet` for concurrent dispatch of RPCs (`RequestVote` and `AppendEntries`) with strict timeouts.

---

## 🚫 Unbreakable Rules (Guardrails)

1.  **RAII Semantic Requirement:** Every OS resource (TCP Sockets, Log Files) must be encapsulated in types that implement `Drop` for automatic and deterministic release upon Task abort.
2.  **No Blocking I/O:** No synchronous blocking calls (e.g., standard file reads, synchronous sockets, standard library sleep) may run directly on the executor thread. Use `tokio::task::spawn_blocking` without exception.
3.  **Lock-Free or Short Locks:** Concurrent locks must be held for the shortest possible duration. Never hold a Write Lock (`write().await`) across network RPC calls (`await`).
4.  **Explicit Error Handling:** The use of `.unwrap()` or `.expect()` is prohibited in production code. Handle all errors using `Result` and propagate them using the `?` operator or handle them locally and resiliently.

---

## 🛠️ Frequent Commands

*   **Build/Check:** `cargo check` (fast type validation)
*   **Compile:** `cargo build --release`
*   **Run Tests:** `cargo test`
*   **Format:** `cargo fmt`
*   **Linting:** `cargo clippy -- -D warnings`
*   **Manual Execution:** `cargo run` (requires environment variables described in the README)
