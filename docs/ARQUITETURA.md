# Arquitetura — raft-kv-rs 1000000.0.0

Este documento descreve o que o código faz hoje. A versão 1000000.0.0 foi pedida; não é um carimbo de produção. Não há número de throughput, latência ou tempo de eleição medido.

Há **dois** modos de execução. Eles compartilham o mesmo núcleo (`RaftCore`) e não são um cluster de produção.

## Modos

| Modo | Onde | Relógio | Rede |
|---|---|---|---|
| Nó TCP | `RaftNode` / `raft-node serve` | tokio (`sleep` de eleição e heartbeat) | TCP localhost (ou qualquer endereço que o processo alcance), frame length-prefix |
| Simulador | `src/sim.rs` | tick lógico, RNG com semente | fila em memória; drop, atraso dentro do tick, partição e endereço de escuta |

O simulador **não** arma o timer de check-quorum: ele só chama `on_election_timeout` em quem não é líder. O processo TCP arma. Os dois modos persistem de forma diferente: o nó TCP usa `raft-state.bin`; o simulador usa `Storage::ephemeral` (só memória).

`spawn_cluster` sobe 3 nós **no mesmo processo**, cada um com o seu socket TCP em `127.0.0.1`. É o mesmo transporte do binário, não um barramento separado.

## Peças

```
cliente TCP ou NodeHandle
        │  Message (bincode + u32 BE)
        ▼
transport.rs ── accept / dial ──► node.rs (timers, pendências de cliente)
                                      │
                                      ▼
                                 raft/mod.rs   RaftCore
                                 ├── election.rs    regra de voto
                                 ├── pre_vote.rs    pre-vote sem gravar termo
                                 ├── replication.rs commit por quorum da config
                                 ├── quorum.rs      maioria estável e conjunta
                                 ├── log.rs         log 1-indexado + truncate prefix
                                 ├── config.rs      C_old,new / C_new / learners
                                 ├── kv.rs          estado aplicado + sessões
                                 ├── storage.rs     imagem RAFT v1 + CRC-32
                                 ├── observe.rs     anel term/eleição/commit/snapshot
                                 └── client.rs      serial estável no retry
```

`node.rs` não reimplementa o protocolo. Ele traduz `Outbound` em escrita TCP, timer ou resposta de cliente.

## RPCs

Já existentes e mantidos:

- `RequestVote` / `RequestVoteResponse`
- `PreVote` / `PreVoteResponse`
- `AppendEntries` / `AppendEntriesResponse` (lote, `need_snapshot`, `conflict_index`)
- `InstallSnapshot` / `InstallSnapshotResponse` (`offset`, `done`)
- `TimeoutNow`
- `Client` / `ClientReply` com `Put`, `Get` (read index), `RawGet`, `ReadIndex`, `SessionPut`, `ChangeMembership`, `TransferLeadership`
- `StatusRequest`

Acrescentados em 1.1.0, no mesmo enum:

- `ClientRequest::PromoteLearner` — só entra no consenso conjunto se o learner já tem `match_index` cobrindo o log do líder
- `MetricsRequest` — texto com os contadores do processo (não é um scrape Prometheus)
- `TraceRequest` — texto do anel em `observe.rs` (não é scrape; some no processo)


## Caminhos que já estavam no núcleo

- **Snapshot.** `compact_log` aplica até `commitIndex`, grava KV + sessões + config e corta o prefixo. Se `nextIndex` do peer cai no snapshot, o líder manda `InstallSnapshot` em fatias. O follower só trunca o log quando `done` é verdadeiro.
- **Membership.** Uma mudança por vez: `C_old,new` (maioria dos dois lados) e, depois do commit, o líder anexa `C_new`. Learners recebem log e não votam.
- **Read index.** `Get` espera quorum de acks de heartbeat no termo atual. `RawGet` não espera. Não há lease de leitura: o caminho linearizável é o read index.
- **TimeoutNow.** O líder só envia depois de `matchIndex >= lastLogIndex` e então desce. O alvo faz eleição direta, sem pre-vote, para não ser barrado pelos followers que ainda ouvem o líder antigo.
- **Sessão.** `SessionPut` com `client_id` + `serial`. Na 1.1 o líder também recusa um segundo append se o mesmo par já está no log e ainda não commitou. `client_id == 0` é recusado. `SessionClient` guarda o serial do retry.

## O que a 1.1 acrescenta no processo

**Check-quorum.** No líder, `contact` junta o próprio id e quem mandou ack de `AppendEntries` ou fim de snapshot. Não é o conjunto do read index (esse é zerado a cada leitura). Se a janela fecha sem quorum da config, o líder vira follower **sem** subir o termo. A janela é `4 * election_max_ms` (teto do timeout de eleição). Não é uma latência medida; é o mesmo parâmetro de eleição, alongado para um executor atrasado não parecer partição. Acks de quorum reiniciam a janela. Heartbeat sozinho não reinicia.

**Contadores voláteis** (`MetricsSnapshot` e `status`):

| Campo | O que incrementa |
|---|---|
| `commits` | entradas cujo `commitIndex` avançou **neste** processo |
| `elections` | chamadas a `start_election` (pre-vote perdido não conta; check-quorum que só desce também não) |
| `snapshot_bytes` | bytes da imagem em `compact_log`, mais bytes de `data` enviados em `InstallSnapshot`, mais bytes novos aceitos pelo follower |

Reenvio de fatia conta de novo. Compactar e depois enviar a mesma imagem conta as duas vezes, porque são dois eventos neste processo. Os contadores zeram quando o processo sobe; não vão para `raft-state.bin`. Não existe campo de taxa.

**Compactação automática.** `snapshot_threshold == 0` (padrão) não compacta sozinho. Se for N, `maybe_auto_snapshot` corta o log quando há N entradas commitadas acima do snapshot. A CLI `--snapshot-threshold` liga isso no nó TCP.

**Learners na CLI.** `--learners id=addr` entra no mapa de dial e na config como não-votante. Promover é `raft-node promote` (exige catch-up) ou `membership` (proposta explícita, sem essa trava — o comportamento da 1.0).

## Recover

`RaftCore::open` lê `raft-state.bin` e reconstrói o processo assim:

1. Termo, `votedFor` e o log vêm do disco. Uma queda entre as duas escritas de `save_vote_then_log` deixa o termo e o voto da primeira escrita e **não** o log da segunda. O teste `crash_between_vote_and_log_persists_only_the_first_write` arma essa falha.
2. O KV e as sessões saem da imagem de snapshot, e em seguida só das entradas commitadas acima do snapshot. Entrada não commitada não altera o mapa.
3. A configuração é a última `ChangeConfig` em qualquer ponto do log, commitada ou não. O mesmo vale em `apply_committed`: aplicar o prefixo commitado não pode apagar um `C_new` que já está no log e ainda não commitou. Sem isso, o recover (e um follower que recebe `C_new` cedo) voltava para o joint.
4. O papel volta a follower (ou learner, se o id não vota). Contadores `commits`, `elections` e `snapshot_bytes` começam em zero.

O simulador não passa por esse caminho: o storage dele é efêmero.

## Disco

Formato versão 1, inalterado nesta 1.1. Cabeçalho `RAFT`, `u32` LE = 1, registros com CRC-32 do corpo. Detalhe em `SPEC.md`.

## Checksum da imagem

Cada `InstallSnapshot` traz `checksum`: CRC-32/ISO-HDLC (`storage::crc32`) da imagem inteira, repetido em toda fatia. O follower só compara quando `done` é verdadeiro, sobre o buffer montado. Divergência (byte invertido, fatia com checksum diferente da primeira) devolve `success=false` e **não** chama `install_snapshot`. Prefixo do log e KV ficam. O checksum não prova quem enviou.

## Anel de trace

`RaftCore` empurra `term` (termo subiu), `election` (`start_election`), `commit` (avanço de `commitIndex`) e `snapshot` (compactação local ou instalação aceita). Capacidade 128; o mais antigo sai. `raft-node trace` imprime `seq kind term index`. Não entra em `raft-state.bin`.

## O que isto não é

- Não é multi-raft.
- Não é um witness que persiste só o voto.
- Não tolera falta bizantina.
- Não publica benchmark, nem comparação com etcd.
- O demo `scripts/demo_3nodes.sh` sobe três processos em `127.0.0.1`. Isso exercita o TCP de verdade; não simula data center, disco compartilhado nem falha de rede parcial (o teste de minoria desliga processos).
