# RaftKV 🦀

**A high-performance, purely async implementation of the Raft Consensus Algorithm powering a distributed Key-Value store in Rust.**

![Raft](https://img.shields.io/badge/Algorithm-Raft-blue.svg)
![Rust](https://img.shields.io/badge/Language-Rust-orange.svg)
![Tokio](https://img.shields.io/badge/Async-Tokio-purple.svg)

## 📌 Architecture Overview

RaftKV is a project designed to demonstrate a deep understanding of Distributed Systems, Fault Tolerance, and Systems Programming using Rust. It implements the core mechanics of the [Raft Consensus Algorithm](https://raft.github.io/):

1. **Leader Election:** Randomized timeout-driven elections. Nodes transit between `Follower`, `Candidate`, and `Leader` states flawlessly.
2. **Log Replication:** Strong consistency models ensuring that once a log is replicated to a quorum, it is committed safely.
3. **In-Memory Store:** A thread-safe, high-concurrency Key-Value store guarded by asynchronous `RwLock` primitives.

### Why Rust?
Rust's ownership model and "fearless concurrency" make it the perfect candidate for building a distributed system. By leveraging the `tokio` asynchronous runtime, RaftKV handles thousands of concurrent RPC connections without fear of data races or memory leaks.

## 🛠 Project Structure

- `src/raft.rs`: The state machine. Handles heartbeats, term increments, and transitions.
- `src/store.rs`: The underlying `HashMap` protected by atomic primitives for high-throughput reads/writes.
- `src/network.rs`: The serialization layer (using `serde`) and RPC definitions for `AppendEntries` and `RequestVote`.
- `src/main.rs`: The entry point simulating the lifecycle of a node within the cluster.

## 🚀 Getting Started

Since this is a Rust project, you need the Cargo toolchain installed.

### 1. Clone the repository
```bash
git clone https://github.com/Leanza-dev/raftkv.git
cd raftkv
```

### 2. Build the project
```bash
cargo build --release
```

### 3. Run the node
```bash
RUST_LOG=info cargo run
```

*When executed, you will see the Node initializing, reaching the election timeout, transitioning to Candidate, requesting votes, and finally establishing itself as the Leader to begin sending Heartbeats.*

---
*Developed by Pedro Leanza as part of the Advanced Distributed Systems portfolio.*
