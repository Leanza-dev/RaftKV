# RaftKV 🦀

> [🇺🇸 English](./README.md) | 🇧🇷 Português

**Implementação assíncrona e de alta performance do Algoritmo de Consenso Raft em Rust, alimentando um banco de dados chave-valor distribuído.**

![Raft](https://img.shields.io/badge/Algoritmo-Raft-blue.svg)
![Rust](https://img.shields.io/badge/Linguagem-Rust-orange.svg)
![Tokio](https://img.shields.io/badge/Async-Tokio-purple.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

---

## Visão Geral da Arquitetura

O RaftKV implementa os mecanismos centrais do [Algoritmo de Consenso Raft](https://raft.github.io/) em um cluster de nós que se comunicam por **sockets TCP reais**. Cada nó é uma máquina de estados completa, capaz de realizar eleições de líder de forma autônoma e manter consistência de log.

```
┌─────────────────────────────────────────────────────┐
│                  CLUSTER RAFT                       │
│                                                     │
│   ┌──────────┐    TCP     ┌──────────┐             │
│   │  Nó 1   │◄──────────►│  Nó 2   │             │
│   │  (Líder) │            │(Seguidor)│             │
│   └────┬─────┘            └──────────┘             │
│        │         TCP                               │
│        └──────────────────►┌──────────┐            │
│                             │  Nó 3   │            │
│                             │(Seguidor)│            │
│                             └──────────┘            │
└─────────────────────────────────────────────────────┘
```

### Máquina de Estados (`src/raft.rs`)

Cada nó transita autonomamente entre três papéis:

| Estado | Comportamento |
|---|---|
| **Seguidor** | Aguarda heartbeats. Ao atingir o timeout, promove-se a Candidato. |
| **Candidato** | Incrementa o term, vota em si mesmo e envia RPCs `RequestVote` a todos os peers via TCP. |
| **Líder** | Envia RPCs `AppendEntries` (heartbeat) periodicamente para manter autoridade e replicar o log. |

Os timeouts de eleição são aleatorizados (150–300ms) para evitar votos divididos.

### Camada de Rede (`src/network.rs`)

Toda a comunicação entre nós acontece via **conexões TCP brutas** com `tokio::net`. As mensagens são serializadas com `serde_json` e delimitadas por `\n` para enquadramento.

- **RPC `RequestVote`**: O candidato abre uma conexão TCP para cada peer, envia a requisição serializada e aguarda uma resposta `vote_granted`.
- **RPC `AppendEntries`**: O líder envia heartbeats periodicamente a todos os seguidores para manter autoridade e acionar a replicação de log.

### Camada de Armazenamento (`src/store.rs`)

Um armazenamento chave-valor em memória, thread-safe, respaldado por `tokio::sync::RwLock`:
- **Múltiplas leituras concorrentes** sem bloqueio.
- **Escritas exclusivas** garantidas por locks de escrita assíncronos — sem condições de corrida.

---

## Como Usar

### Pré-requisitos
- Toolchain Rust (`rustup`) — [instale aqui](https://rustup.rs/)
- Docker + Docker Compose (para demonstração multi-nó)

### Rodar um Único Nó

```bash
git clone https://github.com/Leanza-dev/RaftKV.git
cd raftkv
RUST_LOG=info NODE_ID=1 PEERS=127.0.0.1:8002,127.0.0.1:8003 LISTEN_ADDR=127.0.0.1:8001 cargo run
```

### Rodar um Cluster de 3 Nós (Recomendado)

```bash
docker compose up --build
```

Observe os logs para acompanhar o ciclo de vida completo do Raft:
1. Todos os nós iniciam como **Seguidores**
2. Um nó atinge o timeout de eleição → transita para **Candidato**
3. O Candidato envia `RequestVote` aos peers via TCP
4. Quorum atingido → nó se torna **Líder**
5. O Líder transmite heartbeats `AppendEntries` a cada 50ms

---

## Estrutura do Projeto

```
raftkv/
├── src/
│   ├── main.rs       # Ponto de entrada — lê NODE_ID, PEERS, LISTEN_ADDR do ambiente
│   ├── raft.rs       # Máquina de estados (Seguidor → Candidato → Líder)
│   ├── network.rs    # Cliente/servidor TCP RPC para RequestVote e AppendEntries
│   └── store.rs      # Store chave-valor com RwLock assíncrono
├── Cargo.toml        # Dependências: tokio, serde_json, rand, log
└── docker-compose.yml # Configuração do cluster com 3 nós
```

---

## Roadmap

- [ ] Log persistente (WAL em disco)
- [ ] Replicação de log com entradas reais
- [ ] API HTTP para operações GET/SET
- [ ] Mudanças de membership (adicionar/remover nós em runtime)

---

*Desenvolvido por Pedro Leanza — Sistemas Distribuídos e Programação de Sistemas.*
