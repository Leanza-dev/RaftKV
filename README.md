# RaftKV 🦀

> 🇺🇸 English | [🇧🇷 Português](./README.pt-BR.md)

**A high-performance, asynchronous implementation of the Raft Consensus Algorithm powering a distributed Key-Value store in Rust.**

![Raft](https://img.shields.io/badge/Algorithm-Raft-blue.svg)
![Rust](https://img.shields.io/badge/Language-Rust-orange.svg)
![Tokio](https://img.shields.io/badge/Async-Tokio-purple.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

---

## Architecture Overview

RaftKV implements the core mechanics of the [Raft Consensus Algorithm](https://raft.github.io/) across a cluster of nodes communicating over real **TCP sockets**. Each node is a full state machine capable of autonomously running leader elections and maintaining log consistency.

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

Every node transitions autonomously between three roles:

| State | Behavior |
|---|---|
| **Follower** | Waits for heartbeats. On timeout, promotes itself to Candidate. |
| **Candidate** | Increments term, votes for itself, sends `RequestVote` RPCs to all peers via TCP. |
| **Leader** | Sends periodic `AppendEntries` (heartbeat) RPCs to maintain authority and replicate log. |

Election timeouts are randomized (150–300ms) to prevent split votes.

### Network Layer (`src/network.rs`)

All inter-node communication happens over **raw TCP connections** using `tokio::net`. Messages are serialized with `serde_json` and newline-delimited for framing.

- **`RequestVote` RPC**: Candidate opens a TCP connection to each peer, sends a serialized request, and awaits a `vote_granted` boolean response.
- **`AppendEntries` RPC**: Leader periodically sends heartbeats to all followers to maintain authority and trigger log replication.

### Storage Layer (`src/store.rs`)

A thread-safe, high-concurrency in-memory Key-Value store backed by `tokio::sync::RwLock`:
- **Multiple concurrent reads** allowed without blocking.
- **Exclusive writes** guaranteed via async write locks — no data races.

---

## Getting Started

### Prerequisites
- Rust toolchain (`rustup`) — [install here](https://rustup.rs/)
- Docker + Docker Compose (for multi-node demo)

### Run a Single Node

```bash
git clone https://github.com/Leanza-dev/RaftKV.git
cd raftkv
RUST_LOG=info NODE_ID=1 PEERS=127.0.0.1:8002,127.0.0.1:8003 LISTEN_ADDR=127.0.0.1:8001 cargo run
```

### Run a 3-Node Cluster (Recommended)

```bash
docker compose up --build
```

Watch the logs to observe the full Raft lifecycle:
1. All nodes start as **Followers**
2. One node hits election timeout → transitions to **Candidate**
3. Candidate sends `RequestVote` to peers via TCP
4. Quorum reached → node becomes **Leader**
5. Leader broadcasts `AppendEntries` heartbeats every 50ms

---

## Project Structure

```
raftkv/
├── src/
│   ├── main.rs       # Entry point — reads NODE_ID, PEERS, LISTEN_ADDR from env
│   ├── raft.rs       # State machine (Follower → Candidate → Leader)
│   ├── network.rs    # TCP RPC client/server for RequestVote & AppendEntries
│   └── store.rs      # Async RwLock-backed Key-Value store
├── Cargo.toml        # Dependencies: tokio, serde_json, rand, log
└── docker-compose.yml # 3-node cluster configuration
```

---

## Roadmap

- [ ] Persistent log (disk-backed WAL)
- [ ] Log replication with actual entries (not just heartbeats)
- [ ] Client-facing HTTP API for GET/SET operations
- [ ] Membership changes (add/remove nodes at runtime)

---

*Developed by Pedro Leanza — Distributed Systems & Systems Programming.*
