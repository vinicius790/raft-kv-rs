//! Shared helpers for the localhost-cluster tests.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use raft_kv_rs::node::{spawn_node, NodeConfig, NodeHandle};
use raft_kv_rs::raft::TimingConfig;

/// Ask the OS for `n` free loopback ports (bind 127.0.0.1:0 while all are
/// held, so they are distinct), then release them for the nodes. No fixed
/// ports, so parallel runs and other binaries do not collide.
pub fn ephemeral_addrs(n: usize) -> Vec<(u64, SocketAddr)> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .enumerate()
        .map(|(i, l)| ((i + 1) as u64, l.local_addr().unwrap()))
        .collect()
}

/// Start node `id` of `addrs` with data under `root/node-{id}`. A restart on
/// the same port retries `AddrInUse` for up to 3 s while the old listener
/// task winds down.
pub async fn spawn_one(
    addrs: &[(u64, SocketAddr)],
    id: u64,
    root: &Path,
    timing: TimingConfig,
) -> anyhow::Result<NodeHandle> {
    let bind = addrs.iter().find(|(i, _)| *i == id).unwrap().1;
    let peers: HashMap<u64, SocketAddr> = addrs.iter().filter(|(i, _)| *i != id).copied().collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let cfg = NodeConfig {
            id,
            bind,
            peers: peers.clone(),
            data_dir: root.join(format!("node-{id}")),
            timing: timing.clone(),
            learners: Vec::new(),
            snapshot_threshold: 0,
        };
        match spawn_node(cfg).await {
            Ok(h) => return Ok(h),
            Err(e) if tokio::time::Instant::now() < deadline && is_addr_in_use(&e) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_addr_in_use(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
    })
}

/// Three-node cluster on the given addresses.
pub async fn spawn_cluster_at(
    addrs: &[(u64, SocketAddr)],
    root: &Path,
    timing: TimingConfig,
) -> anyhow::Result<Vec<NodeHandle>> {
    let mut handles = Vec::new();
    for (id, _) in addrs {
        handles.push(spawn_one(addrs, *id, root, timing.clone()).await?);
    }
    Ok(handles)
}

/// Three-node cluster on fresh ephemeral ports.
pub async fn spawn_cluster(root: &Path, timing: TimingConfig) -> anyhow::Result<Vec<NodeHandle>> {
    spawn_cluster_at(&ephemeral_addrs(3), root, timing).await
}
