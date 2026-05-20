# Engineering at Scale: RaftKV Architecture

Welcome to the RaftKV *Architecture Decision Record* (ADR). This document details the design decisions that allow us to achieve election latencies in the millisecond range and resilience to network partitions, focusing on memory safety and extreme concurrency.

## 1. Dynamic Concurrency with `JoinSet`
In distributed systems, *Head-of-Line Blocking* is lethal. A single node's latency should never define the cluster's *throughput*.
- **The Solution:** We use `tokio::task::JoinSet` to parallelize RPC calls (`RequestVote` and `AppendEntries`). This allows processing responses in the exact order of arrival (`join_next().await`).
- **Graceful Fail-Fast:** The instant we reach Quorum (simple majority), we assume leadership immediately and trigger `set.abort_all()`.
- **Resource Leak Prevention:** Instead of leaking connections, aborting tasks in Tokio triggers the Future's `Drop`, performing a graceful teardown at the socket level (e.g., sending a FIN packet), returning resources to the Operating System immediately.

## 2. Mitigating Network Saturation (Microbursting)
With aggressive fan-out, sending hundreds of simultaneous RPC calls to *Learners* or *Followers* could create a sharp traffic spike (Microburst), overflowing network switch buffers.
- **Active Defense (Implemented):** We inject a `tokio::sync::Semaphore` at the root of the `JoinSet`. By acquiring permits (`acquire_owned`), we limit concurrent throughput to safe batches (e.g., Batches of 50). This stabilizes P99 latency and protects the network infrastructure.

## 3. CPU Isolation and Starvation
Raft requires persistent writes to a secure Write-Ahead Log on disk. Disk *fsync* operations are blocking at the Kernel level. If executed directly, they would cause *Thread Starvation* on the Tokio worker.
- **Active Defense:** All blocking I/O (or future simulations) is isolated using `tokio::task::spawn_blocking`. This dispatches the load to a specialized Thread Pool, ensuring the main election and heartbeat loop runs 100% asynchronously, non-preemptively, and immune to deadlocks.
