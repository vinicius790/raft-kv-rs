# Contribuir

O repositório é um Raft didático. Mudança boa é a que deixa uma invariante mais fácil de apontar no código e num teste, não a que acrescenta superfície.

## Ambiente

Rust estável (edição 2021). `rust-toolchain.toml` pede os componentes `rustfmt` e `clippy`.

```bash
cargo fmt --all
cargo test
cargo clippy --locked --all-targets -- -D warnings
```

O workflow `.github/workflows/ci.yml` recusa formatação divergente, warning do clippy e teste falho. Rode os três antes de abrir um pull request.

## O que entra num pull request

- Comportamento visível (CLI, RPC, disco, default) ganha uma entrada em `CHANGELOG.md` na versão em que vai sair, no estilo Keep a Changelog, sem número de desempenho.
- Teste novo afirma estrutura: termo, índice, papel, conteúdo do log, config, ou "não houve segundo líder". Não publique tempo de eleição, latência ou throughput. Se o teste precisar de um limite, que seja uma cota de passos do simulador ou de iterações, não um orçamento de milissegundos vendido como resultado.
- Não copie implementação de outro projeto. Raft se lê no paper e na dissertação; o código daqui se explica sozinho.
- Persistência muda só com versão de disco nova, e versão mais nova tem de ser recusada pelo leitor atual. A versão 1 está congelada.
- `client_id` 0 continua reservado. `Get` continua sendo read index. `RawGet` continua não linearizável.
- A versão **1000000.0.0** no `Cargo.toml` foi um número pedido. Não a use como argumento de que o crate está pronto para produção. Disco continua na versão 1; uma imagem de snapshot com CRC diferente do anunciado tem de ser recusada sem truncar o log.
- `trace` lista eventos do protocolo deste processo. Não acrescente um exporter Prometheus nem taxa derivada desses eventos.

## O que não entra

- Badge ou README com métrica que o CI não calculou.
- Multi-raft, TLS, autenticação ou API compatível com etcd, a menos que o escopo do repositório mude de forma explícita no changelog.
- `unsafe` sem um comentário do invariante que ele sustenta. Hoje não há `unsafe`.

## Layout

`src/raft/` é o protocolo. `src/observe.rs` é o anel de eventos. `src/node.rs` só traduz `Outbound` em TCP e timer. `src/sim.rs` é o mesmo núcleo com relógio lógico. Teste de integração fica em `tests/`. Checksum de snapshot e `trace` estão em `tests/trace_snapshot.rs`. Propriedade de recover continua em `tests/invariants.rs`.
