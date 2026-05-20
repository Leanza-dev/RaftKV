# Engineering at Scale: RaftKV Architecture

Welcome to the RaftKV Architecture Decision Record (ADR). This document highlights the design choices that allow us to achieve single-digit millisecond leader election latencies and extreme partition resilience, focusing on memory safety and boundless concurrency in Rust.

## 1. Dynamic Concurrency with `JoinSet`
In distributed systems, Head-of-Line Blocking is a silent killer. The latency of an entire node should never be dictated by its slowest peer.
- **The Solution:** We utilize Tokio's `JoinSet` to parallelize RPC calls (`RequestVote` and `AppendEntries`). This enables processing responses in the exact order of their arrival (`join_next().await`).
- **Graceful Fail-Fast:** The instant a Quorum (simple majority) is achieved, we assume leadership instantly and trigger `set.abort_all()`.
- **Resource Leak Prevention:** Instead of leaking connections, aborting Tokio tasks invokes the Future's `Drop` trait, executing a graceful teardown at the socket layer (e.g., dispatching a FIN packet), instantly returning file descriptors to the OS.

## 2. Mitigating Network Microbursting
Given our aggressive fan-out mechanism, launching hundreds of simultaneous RPC calls to Learners or Followers could generate sharp traffic spikes (Microbursts), potentially overflowing network switch buffers and degrading P99 latencies.
- **Active Defense (Implemented):** We injected a `tokio::sync::Semaphore` at the root of our `JoinSet` execution. By acquiring `permits` concurrently, we strictly bound the outbound request velocity to safe batches (e.g., batches of 50). This guarantees stable network conditions at immense scale.

## 3. CPU Isolation and Starvation Prevention
The Raft consensus demands persistent writes to a secure Write-Ahead Log (WAL). Direct disk `fsync` operations block at the Kernel level. If executed on an async thread, they induce critical Thread Starvation on the Tokio worker.
- **Active Defense (Implemented):** All blocking I/O (and heavy CPU-bound tasks) are meticulously isolated using `tokio::task::spawn_blocking`. This offloads the payload to a specialized backing Thread Pool, ensuring the core Raft heartbeat loop operates 100% asynchronously, non-preemptively, and immune to deadlocks.
