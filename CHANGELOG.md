# Changelog

O formato segue o espírito de Keep a Changelog: versão, data, o que entrou e o que continua de fora. Não há seção de desempenho.

## Não lançado

### Mudou

- Crate renomeado de `raft-mvp` para `raft-kv-rs`, igual ao repositório. O binário continua `raft-node`. Quem importa a biblioteca troca `raft_mvp::` por `raft_kv_rs::`; o filtro de log padrão da CLI passa a ser `raft_kv_rs=info`.
- `Cargo.toml`: `repository`/`homepage` apontam para https://github.com/vinicius790/raft-kv-rs e o e-mail placeholder saiu de `authors`.
- Formato em disco (`raft-state.bin`, versão 1) e protocolo de rede não mudaram.

## 1000000.0.0 — 2026-10-03

O número **1000000.0.0** foi pedido explicitamente. Não declara maturidade de produção, não aproxima este crate de etcd e não congela uma API nova de disco. O layout de `raft-state.bin` continua a versão 1.

### Entrou

- `InstallSnapshot` carrega o CRC-32/ISO-HDLC da imagem inteira em cada fatia. Se o buffer montado não fecha com esse checksum, o follower recusa, não trunca o log e não troca o KV. Teste: `corrupted_snapshot_is_refused_without_truncating`.
- Anel circular em memória (`src/observe.rs`) com eventos `term`, `election`, `commit` e `snapshot`. Sai por `TraceRequest` e pelo subcomando `raft-node trace`. Não é scrape Prometheus e não é persistido. Teste: `trace_records_term_election_and_commit`.
- Cenário novo no simulador: um nó particionado não acompanha o commit da maioria e, depois do heal, o commit dos três coincide, sem dois líderes no mesmo termo. Teste: `partitioned_node_lags_then_matches_commit_after_heal`.
- `docs/PROTOCOLO.md` (RPCs, invariantes, fluxo de membership). README, arquitetura e CONTRIBUTING alinhados a esta versão.

### Continua de fora

- Multi-raft, witness sem log, faltas bizantinas, lease de leitura, TLS, autenticação.
- Qualquer número de throughput, latência ou tempo de eleição.
- Arquivo de configuração TOML. As flags da CLI continuam sendo o caminho de subir o processo.
- Checksum não autentica o líder; só detecta imagem corrompida ou truncada.

## 1.1.1 — 2026-10-03

Correção de recover e provas que faltavam. O layout em disco não mudou (versão 1).

### Entrou

- A configuração em vigor é a última `ChangeConfig` do log, commitada ou não. Isso vale no `open` e também em `apply_committed`: sem isso, um follower que recebia `C_new` ainda não commitado voltava para o joint quando aplicava o prefixo.
- `tests/invariants.rs`: recover de termo, voto, log e KV; transição C_old → joint → C_new com queda no meio das duas etapas; follower atrasado que recusa o prefixo e só instala snapshot; retry de `SessionPut` que não duplica e `Get` que observa um valor; no máximo um líder por termo no simulador (seeds 1, 2, 7, 17, 41 e 80 neste teste; a suíte 1..=80 continua em `tests/protocol.rs`).
- CI no GitHub Actions: `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked --all-targets`.
- `CONTRIBUTING.md`. README reescrito para explicar o porquê do protocolo e o que o projeto não é.

### Continua de fora

- Multi-raft, witness sem log, faltas bizantinas, lease de leitura.
- Qualquer número de throughput, latência ou tempo de eleição.

## 1.1.0 — 2026-10-03

Por cima da 1.0.0, sem reescrever o núcleo e sem mudar o layout em disco (versão 1).

### Entrou

- Contadores voláteis `commits`, `elections`, `snapshot_bytes` em `MetricsSnapshot`, `NodeStatus`, `status` e no subcomando `metrics`. Não são taxa e não são persistidos.
- Check-quorum no líder do processo TCP: sem ack de quorum na janela `4 * election_max_ms`, desce de líder sem subir o termo. O simulador não arma esse timer.
- `PromoteLearner`: learner só vira voter pelo consenso conjunto se `match_index` cobre o log. `ChangeMembership` permanece sem essa trava.
- `SessionPut` não duplica entrada se o mesmo `(client_id, serial)` já está no log. `client_id` 0 é recusado. `SessionClient` reutiliza o serial até o caller dar `commit`.
- CLI: `--learners`, `--snapshot-threshold`, `session-put`, `membership`, `promote`, `transfer`.
- `snapshot_threshold` (padrão 0) compacta sozinho quando o sufixo commitado atinge o limiar.
- `docs/ARQUITETURA.md`.
- Testes em `tests/expansion.rs`.

### Continua de fora

- Multi-raft, witness sem log, faltas bizantinas.
- Lease de leitura (read index segue sendo o caminho linearizável).
- Qualquer número de throughput, latência ou tempo de eleição.

## 1.0.0 — 2026-10-03

Formato em disco congelado (versão 1). O que entra aqui fica estável: eleição com pre-vote (incluindo `TimeoutNow`), replicação com pipeline e controle de fluxo, consenso conjunto com uma mudança de cada vez até a anterior commitar, snapshot fatiado, `Get` linearizável por read index, learner, sessão de cliente, métricas sem throughput, CLI `status` / `put` / `get`.

### Estável

- Cabeçalho `RAFT` + `u32` LE versão `1` + registros com CRC-32 do corpo. Versão maior que 1, magic errado ou bit invertido: o nó recusa o arquivo e não sobe.
- `InstallSnapshot` com `offset` e `done`. O follower só trunca o log no último pedaço; pedaço com buraco é recusado.
- Vários `AppendEntries` em voo. `matchIndex` só anda no ack. Ack perdido não duplica a entrada no follower.
- Segunda mudança de membership só depois que a primeira (incluindo `C_new`) commitou. Proposta sobreposta é recusada.
- `Get` (biblioteca, `NodeHandle::get` e CLI) usa read index. Líder sem quorum não responde. `RawGet` / `raw_get` permanece como leitura local.
- `TimeoutNow` faz pre-vote. Se o pre-vote perde, o termo do alvo não sobe.
- Líder pausa replicação de entradas para um peer quando os bytes em voo passam de `max_inflight_bytes` e retoma no ack.
- Simulador: seeds **1..=80** (eleição, queda de pacote, partição) e seeds **41..=80** com queda de pacote e uma mudança de membership. Invariantes: no máximo um líder por termo; logs com o mesmo índice e termo coincidem. Nenhum tempo de eleição foi medido nem publicado.

### Ainda de fora

- Multi-raft.
- Disco só de witness (sem log).
- Faltas bizantinas.
- Números de throughput, latência ou tempo de eleição.

## 0.5.0 — 2026-10-03

Não é 1.0: a API de mensagens (bincode) e o formato em disco mudaram nesta versão e os testes de segurança cobrem os caminhos novos, mas o conjunto de RPC ainda não está congelado.

### Entrou

- Consenso conjunto de membership (`C_old,new` depois `C_new`). Um servidor sozinho não perde o líder por timeout.
- Compactação real: `InstallSnapshot` trunca o prefixo; follower atrasado recupera pelo snapshot, não pelo log inteiro.
- Pre-vote: nó isolado não incrementa termo nem derruba o líder que ainda tem quorum.
- Transferência de liderança (`TimeoutNow`) só depois do follower estar em dia; o prefixo commitado é o mesmo.
- Read index: leitura linearizável espera heartbeat de quorum no termo atual. Líder particionado não responde.
- Persistência em dois estágios (voto/termo, depois log) com injeção de crash entre as escritas.
- Simulador determinístico (tick lógico, drop/delay/partição). Seeds executados nos testes: 1..=40.
- Learner não-votante, que recebe log e snapshot e só entra no quorum quando promovido pelo caminho de consenso conjunto.
- AppendEntries em lote (mais de uma entrada por mensagem); follower rejeita buraco.
- Sessão de cliente (`client_id` + `serial`): propose repetido aplica uma vez.
- `MetricsSnapshot`: termo, commit, lag de replicação por peer. Sem número de throughput.
- Recarga de endereços de peers sem reiniciar o processo (`PeerMap::reload` / simulador).
- CLI `status` mostra papel, termo, commit e peers; `put` / `get` seguem no binário `raft-node`.

### Limites (de propósito)

- Snapshot é um único RPC, não fragmentado.
- Uma mudança de membership por vez. Conjunto de voters vazio é recusado. Um cluster de um nó recusa qualquer proposta que tire esse nó do conjunto — senão o líder se perderia sem quorum.
- `Get` legado lê o estado aplicado no líder sem read-index. O caminho linearizável é `ReadIndex`.
- Sem benchmarks. Qualquer latência observada fora dos testes não é publicada aqui.
- O simulador não usa o relógio de parede como fonte da verdade; os testes de integração com TCP continuam usando timeouts reais curtos.

## 0.1.0

- MVP: eleição, replicação, KV, persistência, demo de 3 nós, snapshot apenas de metadados.
