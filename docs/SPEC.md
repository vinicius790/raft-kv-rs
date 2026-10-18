# Especificação — raft-kv-rs 1000000.0.0

Implementação didática de Raft (Ongaro & Ousterhout) em Rust. Este documento descreve o que o código faz e o que **não** promete. Não há números de throughput, latência ou tempo de eleição inventados. Timeouts padrão estão em `TECNOLOGIAS.md`; testes de integração encurtam esses intervalos de propósito.

Formato em disco abaixo é o da versão **1** e segue congelado na 1.1.0 (igual à 1.0.0): um arquivo com versão maior que 1 é recusado e o nó não sobe.

## Estado

- Persistente: `currentTerm`, `votedFor`, log, configuração de cluster, índice de snapshot.
- Volátil: `commitIndex`, `lastApplied`, papel (Follower, Candidate, Leader, Learner).
- Líder: `nextIndex`, `matchIndex`, acks de heartbeat do termo atual (read index).

O log é 1-indexado. Depois de um snapshot, `snapshot_index` substitui o prefixo; `term_at(snapshot_index)` devolve o termo do snapshot.

## Eleição e pre-vote

1. Timeout de eleição (seguidor votante) dispara **pre-vote** com `next_term = currentTerm+1` **sem** gravar termo nem voto.
2. Um par concede pre-vote só se o log do candidato está em dia **e** ele não é líder **e** não recebeu AppendEntries do líder atual desde o último timeout (`heard_from_leader`).
3. Só com quorum de pre-vote o candidato incrementa o termo e manda `RequestVote`.
4. Nó isolado não consegue quorum de pre-vote contra um líder saudável: o termo do líder não sobe.
5. Cluster de um voter: o timeout elege (ou mantém) o líder local. Timeout **enquanto já é líder** não faz nada. Proposta que tire esse único voter do conjunto é recusada.

`TimeoutNow` (transferência de liderança) **não passa por pre-vote** (modelo do etcd): os outros followers ainda ouvem o líder antigo e recusariam o pre-vote, então com 5+ voters a transferência nunca venceria. O alvo sobe o termo e manda `RequestVote` direto; o voto continua exigindo log em dia. `TimeoutNow` de termo antigo é ignorado. Pre-vote pode ser desligado (`enable_pre_vote = false`) só em testes que injetam `RequestVote` direto.

## Replicação, lote e pipeline

`AppendEntries` carrega até 64 entradas (`MAX_APPEND_BATCH`). O líder pode manter **mais de um** `AppendEntries` em voo para o mesmo peer (`pipeline_window`, padrão 8). `matchIndex` só avança quando chega um ack com índice maior; enviar a mensagem não o move. Um ack perdido não faz o follower aplicar de novo: entrada já presente com o mesmo termo não é duplicada.

O follower rejeita `prev_log_index` maior que o fim do log ou abaixo do snapshot (`success=false`, e `need_snapshot` quando o prefixo foi compactado). Não cria buraco no log.

**Controle de fluxo:** o líder soma os bytes (tamanho bincode) dos lotes ainda sem ack. Se essa soma atinge `max_inflight_bytes` (padrão 1 MiB) e já há lote em voo, ele para de mandar entradas para aquele peer. Um ack que cobre o lote libera o orçamento e a replicação continua. Heartbeat vazio (sem entradas) ainda pode sair, para o mandato não cair só porque o fluxo pausou.

Commit no líder: maior `N` com `log[N].term == currentTerm` replicado num quorum da configuração **atual** (estável ou conjunta). Learner não entra na conta.

## Consenso conjunto (membership)

Uma mudança por vez, e a seguinte só começa depois que a anterior **commitou** — inclusive o `C_new` que o líder anexa ao sair do conjunto. Enquanto o conjunto está joint, ou existe `ChangeConfig` ainda acima de `commitIndex`, uma segunda proposta é recusada.

O líder anexa `C_old,new` (quorum = maioria de `old` **e** maioria de `new`). Quando essa entrada commita, o líder anexa `C_new` (quorum só do conjunto novo).

Limites:

- Voters vazio é recusado.
- Líder único não pode se remover.
- O líder que propõe continua no processo até `C_new` commitar; peers novos entram em `nextIndex`/`matchIndex` na hora da proposta.
- Endereço de rede é outro mapa (`reload` de peers), não a configuração de voto.

## Learner

Papel `Learner`: recebe AppendEntries e InstallSnapshot, não vota, não inicia eleição, não conta para commit. Promoção: `ChangeMembership` que o coloca em `voters` passa pelo mesmo consenso conjunto.

## Snapshot

`compact_log` aplica até `commitIndex`, grava KV + sessões + config, e apaga o prefixo do log. Se `nextIndex` do peer cai em ou antes do snapshot, o líder manda `InstallSnapshot` **em fatias**: `offset` é o byte inicial do pedaço, `done` marca o último. O follower junta os bytes e **só trunca o log quando `done` e o CRC-32 do buffer é o `checksum` anunciado** (CRC-32/ISO-HDLC da imagem inteira, o mesmo em cada fatia). Checksum divergente recusa sem truncar. Um pedaço cujo `offset` não continua o buffer (um buraco) é recusado; o log local permanece. `snapshot_chunk_size` (padrão 1 MiB) define o tamanho de cada fatia. Uma imagem que cabe numa fatia sai com `offset = 0` e `done = true`, como um único RPC.

Ao instalar, o follower só mantém as entradas depois de `last_included_index` se a entrada nesse índice tiver o mesmo termo `last_included_term` (Raft, Fig. 13); senão descarta o log inteiro.

Snapshot com `last_included_index <= commitIndex` do follower é velho: o follower responde `success`/`done` (o líder avança `matchIndex`) e **não** troca KV, log nem config, para o KV não voltar no tempo.

O frame TCP continua com teto de 16 MiB (`transport::MAX_FRAME`), checado na leitura **e** na escrita: `Frame::write` devolve `InvalidData` em vez de mandar um frame que o outro lado recusaria. Uma fatia não deve passar desse teto.

## Read index

`ClientRequest::Get` e `ClientRequest::ReadIndex` são o mesmo caminho (tese do Raft, §6.4): o líder abre uma rodada nova (`read_round`), zera os acks, conta a si mesmo e manda `AppendEntries` com `read_ctx = read_round`. Só responde quando (1) o noop do termo atual já está commitado e (2) `has_config_quorum` sobre os peers que responderam **essa** rodada (o follower ecoa `read_ctx`). Ack de rodada anterior não conta. Sem essas duas condições (líder novo ou particionado) a resposta fica pendente. `NodeHandle::get` e o subcomando `get` da CLI usam esse caminho.

`ClientRequest::RawGet` (e `NodeHandle::raw_get`) lê o estado já aplicado no líder **sem** read index. Não é linearizável.

## Transferência de liderança

`TransferLeadership { target }` só envia `TimeoutNow` depois de `matchIndex[target] >= lastLogIndex`, e o líder então desce. O alvo sobe o termo e sai `RequestVote` na hora, sem pre-vote. O novo líder carrega o mesmo prefixo commitado. Alvo que não é voter é recusado.

## Sessão de cliente

`SessionPut { client_id, serial, key, value }`: se `serial` já foi aplicado para esse cliente, o líder não anexa de novo e o valor antigo permanece. `client_id == 0` não é usado pela API de sessão. Puts comuns (`Put`) não deduplicam.

## Persistência

`save` grava o estado durável antes do RPC de sucesso sair do núcleo (o nó só envia a resposta depois que `handle_message` retorna). `save_vote_then_log` faz duas escritas: (1) termo + voto, (2) log. `CrashPolicy` pode falhar na N-ésima escrita; o teste recarrega o disco e vê o voto sem a entrada de log.

### Layout de `raft-state.bin` (versão 1)

Arquivo vazio ou ausente é estado inicial. Qualquer outro conteúdo é este layout; magic errado, versão diferente de 1, registro truncado ou crc inválido fazem `Storage::open` falhar. O processo não sobe com esse arquivo.

| Offset | Tamanho | Campo |
|---|---|---|
| 0 | 4 | magic ASCII `RAFT` |
| 4 | 4 | versão do formato, `u32` little-endian. Esta versão grava `1`. Valor `0` ou maior que `1` é recusado. |
| 8 | … | um ou mais registros WAL |

Cada registro:

| Offset no registro | Tamanho | Campo |
|---|---|---|
| 0 | 4 | tamanho `N` do corpo, `u32` little-endian |
| 4 | `N` | corpo: bincode da imagem (`currentTerm`, `votedFor`, log, commit, snapshot, KV, sessões, config) |
| 4+`N` | 4 | CRC-32/ISO-HDLC (polinômio refletido `0xEDB88320`) **só do corpo**, `u32` little-endian |

A gravação reescreve o arquivo inteiro de forma atômica (arquivo temporário, `sync_all`, rename): na prática um registro por flush, que é a imagem completa. O leitor aceita vários registros em sequência e fica com o último **somente se todos** os crc conferem. Um bit invertido no corpo ou no crc faz o open falhar; não há carga parcial silenciosa.

O crc é calculado no crate (`storage::crc32`). Não há dependência extra para isso.

## Métricas

`MetricsSnapshot`: id, termo, commit, applied, último índice, papel, `replication_lag` por peer (`last_index - match_index`), `snapshot_index`. Não há campo de throughput.

## Simulador

`src/sim.rs`: tick lógico, RNG com semente, drop, atraso (reordena dentro do tick), partição e endereço de escuta. Mensagem cujo `dest_addr` não bate com o listen atual é descartada até `reload_routes`. Invariantes: no máximo um líder por termo; se dois logs têm o mesmo índice e termo, a entrada coincide. Seeds executados no teste de eleição/partição: **1..=80** (`PROPERTY_SEEDS`). Seeds executados no cenário de perda de pacote mais uma mudança de membership: **41..=80** (`MEMBERSHIP_LOSS_SEEDS`). O simulador não usa `sleep` como relógio. Nenhum tempo de eleição é publicado a partir desses ticks.

## CLI

`raft-node status` imprime papel, termo, commit, peers e o resumo da config. `put` / `get` falam com o nó por TCP length-prefix. `get` é o read index (linearizável). Followers devolvem `not leader` e `leader_hint` (id, não endereço).

## Check-quorum (1.1)

No líder TCP, `contact` inclui o próprio id e peers que ack de `AppendEntries` ou de `InstallSnapshot` com `done`. O conjunto do read index é outro e continua sendo zerado a cada `Get`. Ack **não** rearma o timer de check-quorum: a janela é fixa (`check_quorum_timeout`), rearmada só pela própria checagem. Assim um líder isolado logo depois de um ack desce na checagem seguinte, sem ganhar uma janela extra.

Se `on_election_timeout` corre no líder e `contact` não forma quorum da config, o líder vira follower e **não** incrementa `currentTerm`. A janela armada pelo processo é `4 * election_max_ms`. Heartbeat sem ack não reinicia a janela; ack que completa o quorum reinicia. `enable_check_quorum = false` restaura o comportamento antigo (líder ignora o timeout).

O simulador não chama timeout em líder, então não executa este caminho.

## Contadores (1.1)

`MetricsSnapshot` e `NodeStatus` ganham `commits`, `elections`, `snapshot_bytes`. Definição:

- `commits`: soma dos avanços de `commitIndex` neste processo (líder, follower ou salto por snapshot).
- `elections`: quantas vezes `start_election` rodou aqui. Pre-vote sem quorum não conta.
- `snapshot_bytes`: bytes da imagem serializada em `compact_log`, mais `data.len()` de cada `InstallSnapshot` enviado, mais bytes novos aceitos na montagem do follower. Reenvio conta outra vez.

Não há taxa. O valor zera no boot. `MetricsRequest` devolve esses campos em texto; a CLI `metrics` imprime essa string.

## PromoteLearner e sessão no log (1.1)

`PromoteLearner { id }` exige config estável, `id` learner, e `match_index[id] >= lastLogIndex`. Senão a proposta não é anexada. A promoção em si ainda é o consenso conjunto (`C_old,new` depois `C_new`).

`SessionPut` com serial já presente no log (mesmo ainda não aplicado) não anexa outra entrada: a resposta espera o índice original. `client_id == 0` responde erro.

`snapshot_threshold` (padrão 0) pede `compact_log` quando `commitIndex - snapshot_index` atinge o limiar. Não muda o RPC de snapshot.

## Fora desta 1.1.0

- Não há multi-raft (vários grupos no mesmo processo).
- Não há disco só de witness (membro que persiste só o voto, sem log).
- Não há tolerância a faltas bizantinas.
- Não há benchmark nem número de throughput, latência ou tempo de eleição.
- Não há lease de leitura (o caminho linearizável é read index).
- O simulador não modela o timer de check-quorum.
