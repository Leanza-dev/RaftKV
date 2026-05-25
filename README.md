# RaftKV 🦀

**An implementation of the Raft consensus algorithm in Rust — leader election, log replication, and log compaction over real TCP sockets.**

![Raft](https://img.shields.io/badge/Algorithm-Raft-blue.svg)
![Rust](https://img.shields.io/badge/Language-Rust-orange.svg)
![Tokio](https://img.shields.io/badge/Async-Tokio-purple.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

---

## Background

I read the Raft paper (Ongaro & Ousterhout, 2014) for a distributed systems deep-dive and decided the only way to actually understand it was to implement it. The paper is unusually readable for a CS paper, but there's a big gap between "I understand the algorithm" and "I can make three nodes actually agree on a leader."

This project is me crossing that gap.

---

## What's implemented

Each node is a full state machine that transitions autonomously between three roles:

| State | Behavior |
|---|---|
| **Follower** | Waits for heartbeats. On timeout, starts an election. |
| **Candidate** | Increments term, votes for itself, sends `RequestVote` to peers via TCP. |
| **Leader** | Broadcasts `AppendEntries` heartbeats every 50ms to maintain authority. |

Election timeouts are randomized (150–300ms) to prevent split votes — the paper's insight is that probabilistic staggering is both simpler and more reliable than deterministic coordination.

**Log compaction is also implemented.** Once the in-memory log exceeds 100 entries, committed entries are discarded and the snapshot watermark (`last_included_index`, `last_included_term`) is updated. The `KeyValueStore` serves as the implicit snapshot state.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    RAFT CLUSTER                     │
│                                                     │
│   ┌──────────┐    TCP     ┌──────────┐             │
│   │  Node 1  │◄──────────►│  Node 2  │             │
│   │ (Leader) │            │(Follower)│             │
│   └────┬─────┘            └──────────┘             │
│        │         TCP                               │
│        └──────────────────►┌──────────┐            │
│                             │  Node 3  │            │
│                             │(Follower)│            │
│                             └──────────┘            │
└─────────────────────────────────────────────────────┘
```

### State Machine (`src/raft.rs`)

The `RaftNode` runs a loop that dispatches to `run_follower`, `run_candidate`, or `run_leader` based on current role. Role transitions happen inside the loop — the architecture makes illegal state sequences visible at the type level.

### Network (`src/network.rs`)

All RPC communication uses `tokio::net` TCP sockets. Messages are `serde_json`-serialized and newline-delimited. `RequestVote` and `AppendEntries` RPCs are sent concurrently via `JoinSet`, with a `Semaphore` to bound fan-out in large clusters.

### Storage (`src/store.rs`)

An in-memory KV store behind `tokio::sync::RwLock` — multiple concurrent reads, exclusive writes.

---

## Running

### Prerequisites
- Rust toolchain ([rustup.rs](https://rustup.rs/))
- Docker + Docker Compose

### 3-Node Cluster (recommended)

```bash
git clone https://github.com/Leanza-dev/RaftKV.git
cd RaftKV
docker compose up --build
```

Watch the logs to see the full election lifecycle: followers timing out, a candidate requesting votes, quorum reached, leader broadcasting heartbeats.

### Single Node

```bash
RUST_LOG=info NODE_ID=1 PEERS=127.0.0.1:8002,127.0.0.1:8003 LISTEN_ADDR=127.0.0.1:8001 cargo run
```

---

## Project Structure

```
raftkv/
├── src/
│   ├── main.rs       # Entry point — reads NODE_ID, PEERS, LISTEN_ADDR from env
│   ├── raft.rs       # State machine: Follower → Candidate → Leader
│   ├── network.rs    # TCP RPC client/server for RequestVote & AppendEntries
│   └── store.rs      # Async RwLock-backed Key-Value store
├── Cargo.toml
└── docker-compose.yml
```

---

## Roadmap

- [ ] Persistent log (disk-backed WAL)
- [ ] Full log replication with real entries (not just heartbeats)
- [ ] Client HTTP API for GET/SET
- [ ] Dynamic membership changes
- [ ] Chaos testing: node crashes and network partitions mid-election
