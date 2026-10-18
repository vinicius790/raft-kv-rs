//! CLI: `raft-node` server and lightweight client subcommands.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use raft_mvp::node::{spawn_node, NodeConfig};
use raft_mvp::raft::TimingConfig;
use raft_mvp::rpc::{ClientRequest, ClientResponse, Message, NodeId};
use raft_mvp::transport;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "raft-node", about = "Raft MVP node / client (catálogo 2.1)")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a Raft peer.
    Serve {
        #[arg(long)]
        id: NodeId,
        /// Bind address, e.g. 127.0.0.1:7001
        #[arg(long)]
        bind: SocketAddr,
        /// Peer list: id=addr,id=addr (excluding self)
        #[arg(long, default_value = "")]
        peers: String,
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        #[arg(long, default_value_t = raft_mvp::DEFAULT_HEARTBEAT_MS)]
        heartbeat_ms: u64,
        #[arg(long, default_value_t = raft_mvp::DEFAULT_ELECTION_MIN_MS)]
        election_min_ms: u64,
        #[arg(long, default_value_t = raft_mvp::DEFAULT_ELECTION_MAX_MS)]
        election_max_ms: u64,
        /// Learners: id=addr,id=addr. Dialed like peers, but they do not vote.
        #[arg(long, default_value = "")]
        learners: String,
        /// Compact after this many committed entries past the snapshot. 0 disables.
        #[arg(long, default_value_t = 0)]
        snapshot_threshold: u64,
    },
    /// Put a key via a node address (follows leader hint once).
    Put {
        #[arg(long)]
        addr: SocketAddr,
        key: String,
        value: String,
    },
    /// Get a key.
    Get {
        #[arg(long)]
        addr: SocketAddr,
        key: String,
    },
    /// Print node status (includes commit/election/snapshot counters).
    Status {
        #[arg(long)]
        addr: SocketAddr,
    },
    /// Print process-local counters. Values come from the node, not from this CLI.
    Metrics {
        #[arg(long)]
        addr: SocketAddr,
    },
    /// Dump the in-process protocol trace (term, election, commit, snapshot).
    Trace {
        #[arg(long)]
        addr: SocketAddr,
    },
    /// Idempotent put. Same client_id+serial is applied at most once.
    SessionPut {
        #[arg(long)]
        addr: SocketAddr,
        #[arg(long)]
        client_id: u64,
        #[arg(long)]
        serial: u64,
        key: String,
        value: String,
    },
    /// Joint membership change. Voters and learners are comma-separated ids.
    Membership {
        #[arg(long)]
        addr: SocketAddr,
        #[arg(long)]
        voters: String,
        #[arg(long, default_value = "")]
        learners: String,
    },
    /// Promote a caught-up learner (refuses if match_index is behind).
    Promote {
        #[arg(long)]
        addr: SocketAddr,
        #[arg(long)]
        id: NodeId,
    },
    /// Transfer leadership (`TimeoutNow`) after the target is caught up.
    Transfer {
        #[arg(long)]
        addr: SocketAddr,
        #[arg(long)]
        target: NodeId,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("raft_mvp=info".parse()?))
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Commands::Serve {
            id,
            bind,
            peers,
            data_dir,
            heartbeat_ms,
            election_min_ms,
            election_max_ms,
            learners,
            snapshot_threshold,
        } => {
            let mut peers = parse_peers(&peers)?;
            let learner_map = parse_peers(&learners)?;
            let learner_ids: Vec<NodeId> = learner_map.keys().copied().collect();
            peers.extend(learner_map);
            let cfg = NodeConfig {
                id,
                bind,
                peers,
                data_dir,
                timing: TimingConfig {
                    heartbeat_ms,
                    election_min_ms,
                    election_max_ms,
                },
                learners: learner_ids,
                snapshot_threshold,
            };
            let handle = spawn_node(cfg).await?;
            tracing::info!(id, addr = %handle.addr, "serving");
            tokio::signal::ctrl_c().await?;
            handle.shutdown().await;
        }
        Commands::Put { addr, key, value } => {
            let resp = client_put_follow(addr, &key, &value).await?;
            print_resp(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Commands::Get { addr, key } => {
            let resp = client_rpc(addr, Message::Client(ClientRequest::Get { key })).await?;
            print_resp(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Commands::Status { addr } => {
            let msg =
                transport::client_request(addr, Message::StatusRequest, Duration::from_secs(3))
                    .await
                    .context("status request")?;
            match msg {
                Message::ClientReply(r) => {
                    println!("{}", r.value.unwrap_or_default());
                }
                Message::StatusResponse(s) => {
                    println!(
                        "id={} role={} term={} leader={:?} commit={} applied={} log={} kv={} commits={} elections={} snapshot_bytes={}",
                        s.id,
                        s.role,
                        s.term,
                        s.leader,
                        s.commit_index,
                        s.last_applied,
                        s.log_len,
                        s.kv_len,
                        s.commits,
                        s.elections,
                        s.snapshot_bytes
                    );
                }
                other => bail!("unexpected: {other:?}"),
            }
        }
        Commands::Metrics { addr } => {
            let msg =
                transport::client_request(addr, Message::MetricsRequest, Duration::from_secs(3))
                    .await
                    .context("metrics request")?;
            match msg {
                Message::ClientReply(r) => {
                    println!("{}", r.value.unwrap_or_default());
                    if !r.ok {
                        std::process::exit(1);
                    }
                }
                other => bail!("unexpected: {other:?}"),
            }
        }
        Commands::Trace { addr } => {
            let msg =
                transport::client_request(addr, Message::TraceRequest, Duration::from_secs(3))
                    .await
                    .context("trace request")?;
            match msg {
                Message::ClientReply(r) => {
                    println!("{}", r.value.unwrap_or_default());
                    if !r.ok {
                        std::process::exit(1);
                    }
                }
                other => bail!("unexpected: {other:?}"),
            }
        }
        Commands::SessionPut {
            addr,
            client_id,
            serial,
            key,
            value,
        } => {
            let resp = client_rpc(
                addr,
                Message::Client(ClientRequest::SessionPut {
                    client_id,
                    serial,
                    key,
                    value,
                }),
            )
            .await?;
            print_resp(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Commands::Membership {
            addr,
            voters,
            learners,
        } => {
            let resp = client_rpc(
                addr,
                Message::Client(ClientRequest::ChangeMembership {
                    voters: parse_ids(&voters)?,
                    learners: parse_ids(&learners)?,
                }),
            )
            .await?;
            print_resp(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Commands::Promote { addr, id } => {
            let resp =
                client_rpc(addr, Message::Client(ClientRequest::PromoteLearner { id })).await?;
            print_resp(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
        Commands::Transfer { addr, target } => {
            let resp = client_rpc(
                addr,
                Message::Client(ClientRequest::TransferLeadership { target }),
            )
            .await?;
            print_resp(&resp);
            if !resp.ok {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

fn parse_ids(s: &str) -> anyhow::Result<Vec<NodeId>> {
    let mut ids = Vec::new();
    if s.trim().is_empty() {
        return Ok(ids);
    }
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        ids.push(part.parse()?);
    }
    Ok(ids)
}

fn parse_peers(s: &str) -> anyhow::Result<HashMap<NodeId, SocketAddr>> {
    let mut map = HashMap::new();
    if s.trim().is_empty() {
        return Ok(map);
    }
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (id, addr) = part
            .split_once('=')
            .with_context(|| format!("bad peer entry: {part}"))?;
        map.insert(id.parse()?, addr.parse()?);
    }
    Ok(map)
}

async fn client_rpc(addr: SocketAddr, msg: Message) -> anyhow::Result<ClientResponse> {
    let reply = transport::client_request(addr, msg, Duration::from_secs(5)).await?;
    match reply {
        Message::ClientReply(r) => Ok(r),
        other => bail!("unexpected reply: {other:?}"),
    }
}

async fn client_put_follow(
    addr: SocketAddr,
    key: &str,
    value: &str,
) -> anyhow::Result<ClientResponse> {
    // `leader_hint` is a node id. This CLI has no peer map, so it does not dial
    // another address. The caller prints the hint.
    client_rpc(
        addr,
        Message::Client(ClientRequest::Put {
            key: key.to_string(),
            value: value.to_string(),
        }),
    )
    .await
}

fn print_resp(resp: &ClientResponse) {
    if resp.ok {
        if let Some(v) = &resp.value {
            println!("{v}");
        } else {
            println!("ok");
        }
    } else {
        eprintln!(
            "error: {} (leader_hint={:?})",
            resp.error.as_deref().unwrap_or("?"),
            resp.leader_hint
        );
    }
}
