# Tecnologias — raft-kv-rs 1000000.0.0

## Linguagem e runtime

| Tecnologia | Papel |
|---|---|
| **Rust** (edition 2021) | Estado de consenso e testes |
| **tokio** | TCP, timers de eleição/heartbeat no processo real, tasks por conexão |

O simulador determinístico **não** usa o relógio do tokio como fonte da verdade: o tick é um contador. Os testes de integração (eleição, replicação, crash, partição) continuam em tokio com timeouts curtos.

## Serialização e rede

| Tecnologia | Papel |
|---|---|
| **serde + bincode** | RPC e corpo de cada registro em disco |
| **TCP length-prefix** (u32 BE) | Framing, sem gRPC |
| **tokio::io** | `read_exact` / `write_all` |

Hello de peer: frame bincode com magic `RAFT` + id, depois a `Message`. Cliente manda só a `Message` e espera uma resposta no mesmo socket.

## Persistência

| Tecnologia | Papel |
|---|---|
| **tokio::fs** | `raft-state.bin` |
| write + `sync_all` + rename | Substituição do arquivo |
| cabeçalho `RAFT` + versão 1 + CRC-32 | Layout congelado; ver `docs/SPEC.md` |
| `save_vote_then_log` | Duas fases (voto, depois log) para o teste de crash entre escritas |
| `Storage::ephemeral` | Só memória, usada pelo simulador |

O CRC-32 é o IEEE (polinômio refletido), implementado em `storage::crc32`. Não há crate extra de checksum. O formato 1 não é o blob cru da 0.1.0 nem o da 0.5.0 sem cabeçalho.

## CLI e utilitários

| Tecnologia | Papel |
|---|---|
| **clap** | `raft-node serve \| put \| get \| status \| metrics \| trace \| session-put \| membership \| promote \| transfer` |
| **tracing** | Eleição e liderança (`RUST_LOG`) |
| **rand** | Timeout de eleição no processo real; o simulador usa `StdRng` semeado |
| **thiserror / anyhow** | Erros de storage e de CLI |
| **tempfile** (dev) | Diretórios dos testes de integração |

`status` mostra papel, termo, commit, log, KV, peers, config e os três contadores do processo. `metrics` imprime a mesma família de campos mais o lag por peer, lidos do nó. Não há TUI, Prometheus nem export de taxa.

`serve --learners id=addr` coloca o id na config como learner e no mapa de dial. `--snapshot-threshold N` liga compactação automática; `0` deixa desligada.

O crate `src/client.rs` (`SessionClient`) não abre socket. Quem fala TCP é `transport.rs`.

## O que não entrou de propósito

- gRPC / protobuf
- RocksDB ou SQLite
- Métricas de throughput, Prometheus, ou benchmark com número publicado (os contadores 1.1 são só eventos deste processo)
- Snapshot num único RPC sem `offset` (a 1.0 fatia; uma imagem pequena ainda cabe numa fatia)
- Pre-vote desligado por padrão (está ligado, também no `TimeoutNow`)
- Multi-raft, disco só de witness, faltas bizantinas

## Build

```bash
cargo test --offline
cargo build --release --offline
```

As crates já precisam estar no cache local (`cargo fetch` uma vez). Sem bibliotecas de sistema além do toolchain Rust.
