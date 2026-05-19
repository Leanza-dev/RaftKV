# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM rust:1.78-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release 2>&1

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/raftkv /usr/local/bin/raftkv

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/raftkv"]
