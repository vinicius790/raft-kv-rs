//! Disk persistence for term, vote, log, commit, config, snapshot.
//! Persist-before-ack. Optional crash injection between staged writes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::kv::{ClientId, Serial};
use crate::raft::config::ClusterConfig;
use crate::raft::log::RaftLog;
use crate::raft::state::PersistentState;
use crate::rpc::{LogIndex, NodeId, Term};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("injected crash after stage {0}")]
    InjectedCrash(usize),
    #[error("bad raft storage magic")]
    BadMagic,
    #[error("unsupported on-disk format version {0}")]
    UnsupportedVersion(u32),
    #[error("wal checksum mismatch")]
    Checksum,
    #[error("truncated wal record")]
    Truncated,
}

/// ASCII magic at the start of `raft-state.bin`.
pub const ON_DISK_MAGIC: &[u8; 4] = b"RAFT";
/// Format version written by this crate. A larger version is rejected.
pub const ON_DISK_VERSION: u32 = 1;

/// CRC-32/ISO-HDLC (IEEE), the polynomial reflected as 0xEDB88320.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

fn encode_image(disk: &OnDisk) -> Result<Vec<u8>, StorageError> {
    let body = bincode::serialize(disk)?;
    let crc = crc32(&body);
    let mut out = Vec::with_capacity(12 + body.len());
    out.extend_from_slice(ON_DISK_MAGIC);
    out.extend_from_slice(&ON_DISK_VERSION.to_le_bytes());
    // One record. The layout allows several; each carries its own crc32.
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn decode_image(bytes: &[u8]) -> Result<OnDisk, StorageError> {
    if bytes.len() < 8 {
        return Err(StorageError::Truncated);
    }
    if bytes[0..4] != *ON_DISK_MAGIC {
        return Err(StorageError::BadMagic);
    }
    let ver = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if ver > ON_DISK_VERSION || ver == 0 {
        return Err(StorageError::UnsupportedVersion(ver));
    }
    let mut i = 8usize;
    let mut last: Option<OnDisk> = None;
    if i >= bytes.len() {
        return Err(StorageError::Truncated);
    }
    while i < bytes.len() {
        if bytes.len() - i < 4 {
            return Err(StorageError::Truncated);
        }
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if bytes.len() - i < len + 4 {
            return Err(StorageError::Truncated);
        }
        let body = &bytes[i..i + len];
        i += len;
        let want = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        i += 4;
        if crc32(body) != want {
            return Err(StorageError::Checksum);
        }
        last = Some(bincode::deserialize(body)?);
    }
    last.ok_or(StorageError::Truncated)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OnDisk {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: RaftLog,
    commit_index: LogIndex,
    snapshot_index: LogIndex,
    snapshot_term: Term,
    snapshot_kv: HashMap<String, String>,
    snapshot_sessions: HashMap<ClientId, Serial>,
    config: ClusterConfig,
}

/// Crash inject policy: crash on the N-th flush call (1-based). 0 = disabled.
#[derive(Clone, Default, Debug)]
pub struct CrashPolicy {
    /// Crash before completing flush number N (1-based). Shared so tests can set it.
    pub crash_on_flush: Arc<AtomicUsize>,
    flush_count: Arc<AtomicUsize>,
}

impl CrashPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arm_next(&self, n: usize) {
        self.crash_on_flush.store(n, Ordering::SeqCst);
        self.flush_count.store(0, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    path: PathBuf,
    disk: OnDisk,
    crash: CrashPolicy,
    /// When true, flushes stay in memory (simulator). No fsync.
    memory: bool,
}

impl Storage {
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with_crash(dir, CrashPolicy::new()).await
    }

    pub async fn open_with_crash(
        dir: impl AsRef<Path>,
        crash: CrashPolicy,
    ) -> Result<Self, StorageError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).await?;
        let path = dir.join("raft-state.bin");
        let disk = if path.exists() {
            let bytes = fs::read(&path).await?;
            if bytes.is_empty() {
                OnDisk::default()
            } else {
                decode_image(&bytes)?
            }
        } else {
            OnDisk::default()
        };
        Ok(Self {
            path,
            disk,
            crash,
            memory: false,
        })
    }

    /// In-memory storage for the deterministic simulator (no disk, no sleeps).
    pub fn ephemeral() -> Self {
        Self {
            path: PathBuf::new(),
            disk: OnDisk::default(),
            crash: CrashPolicy::new(),
            memory: true,
        }
    }

    pub fn crash_policy(&self) -> CrashPolicy {
        self.crash.clone()
    }

    pub fn persistent(&self) -> PersistentState {
        PersistentState {
            current_term: self.disk.current_term,
            voted_for: self.disk.voted_for,
            log: self.disk.log.clone(),
            config: self.disk.config.clone(),
        }
    }

    pub fn commit_index(&self) -> LogIndex {
        self.disk.commit_index
    }

    pub fn snapshot_meta(
        &self,
    ) -> (
        LogIndex,
        Term,
        HashMap<String, String>,
        HashMap<ClientId, Serial>,
    ) {
        (
            self.disk.snapshot_index,
            self.disk.snapshot_term,
            self.disk.snapshot_kv.clone(),
            self.disk.snapshot_sessions.clone(),
        )
    }

    pub async fn save(
        &mut self,
        state: &PersistentState,
        commit_index: LogIndex,
    ) -> Result<(), StorageError> {
        self.disk.current_term = state.current_term;
        self.disk.voted_for = state.voted_for;
        self.disk.log = state.log.clone();
        self.disk.config = state.config.clone();
        self.disk.commit_index = commit_index;
        self.disk.snapshot_index = state.log.snapshot_index;
        self.disk.snapshot_term = state.log.snapshot_term;
        self.flush().await
    }

    /// Persist vote fields first, then full state — used to simulate mid-write crash.
    pub async fn save_vote_then_log(
        &mut self,
        state: &PersistentState,
        commit_index: LogIndex,
    ) -> Result<(), StorageError> {
        // Stage 1: term + vote only (partial durable state).
        self.disk.current_term = state.current_term;
        self.disk.voted_for = state.voted_for;
        self.flush_stage(1).await?;
        // Stage 2: full log + config + commit.
        self.disk.log = state.log.clone();
        self.disk.config = state.config.clone();
        self.disk.commit_index = commit_index;
        self.disk.snapshot_index = state.log.snapshot_index;
        self.disk.snapshot_term = state.log.snapshot_term;
        self.flush_stage(2).await
    }

    // One argument per durable snapshot field. Packing them would hide the format.
    #[allow(clippy::too_many_arguments)]
    pub async fn install_snapshot(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
        kv: HashMap<String, String>,
        sessions: HashMap<ClientId, Serial>,
        remaining_log: RaftLog,
        commit_index: LogIndex,
        config: ClusterConfig,
        term: Term,
        voted_for: Option<NodeId>,
    ) -> Result<(), StorageError> {
        self.disk.snapshot_index = last_included_index;
        self.disk.snapshot_term = last_included_term;
        self.disk.snapshot_kv = kv;
        self.disk.snapshot_sessions = sessions;
        self.disk.log = remaining_log;
        self.disk.commit_index = commit_index;
        self.disk.config = config;
        self.disk.current_term = term;
        self.disk.voted_for = voted_for;
        self.flush().await
    }

    async fn flush(&self) -> Result<(), StorageError> {
        self.flush_stage(0).await
    }

    async fn flush_stage(&self, stage: usize) -> Result<(), StorageError> {
        let n = self.crash.flush_count.fetch_add(1, Ordering::SeqCst) + 1;
        let arm = self.crash.crash_on_flush.load(Ordering::SeqCst);
        if arm != 0 && n == arm {
            return Err(StorageError::InjectedCrash(stage));
        }
        if self.memory {
            return Ok(());
        }
        let bytes = encode_image(&self.disk)?;
        let tmp = self.path.with_extension("bin.tmp");
        {
            let mut f = fs::File::create(&tmp).await?;
            f.write_all(&bytes).await?;
            f.sync_all().await?;
        }
        fs::rename(&tmp, &self.path).await?;
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::Command;
    use crate::raft::log::LogEntry;

    #[tokio::test]
    async fn persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Storage::open(dir.path()).await.unwrap();
        let mut st = s.persistent();
        st.current_term = 7;
        st.voted_for = Some(2);
        st.log.append(LogEntry {
            term: 7,
            command: Command::Put {
                key: "a".into(),
                value: "b".into(),
            },
        });
        s.save(&st, 1).await.unwrap();
        drop(s);
        let s2 = Storage::open(dir.path()).await.unwrap();
        let st2 = s2.persistent();
        assert_eq!(st2.current_term, 7);
        assert_eq!(st2.voted_for, Some(2));
        assert_eq!(st2.log.last_index(), 1);
        assert_eq!(s2.commit_index(), 1);
    }

    #[tokio::test]
    async fn injected_crash_between_vote_and_log() {
        let dir = tempfile::tempdir().unwrap();
        let crash = CrashPolicy::new();
        crash.arm_next(1); // crash on first flush (vote stage)
        let mut s = Storage::open_with_crash(dir.path(), crash.clone())
            .await
            .unwrap();
        let mut st = s.persistent();
        st.current_term = 3;
        st.voted_for = Some(1);
        st.log.append(LogEntry {
            term: 3,
            command: Command::Noop,
        });
        let err = s.save_vote_then_log(&st, 0).await.unwrap_err();
        assert!(matches!(err, StorageError::InjectedCrash(1)));
        // Nothing durable yet (crashed before first successful flush completes).
        // Arm cleared: open without crash and save vote only via normal path mid-test:
        // After InjectedCrash the in-memory disk has term/vote but flush didn't write.
        // Verify file absent or old.
        let path = dir.path().join("raft-state.bin");
        assert!(
            !path.exists() || fs::metadata(&path).await.unwrap().len() == 0 || {
                let s2 = Storage::open(dir.path()).await.unwrap();
                s2.persistent().current_term == 0
            }
        );

        // Now persist vote alone then crash before log.
        let crash2 = CrashPolicy::new();
        let mut s = Storage::open_with_crash(dir.path(), crash2.clone())
            .await
            .unwrap();
        let mut st = s.persistent();
        st.current_term = 4;
        st.voted_for = Some(9);
        // First flush succeeds (vote), second crashes.
        crash2.arm_next(2);
        st.log.append(LogEntry {
            term: 4,
            command: Command::Put {
                key: "x".into(),
                value: "y".into(),
            },
        });
        let err = s.save_vote_then_log(&st, 0).await.unwrap_err();
        assert!(matches!(err, StorageError::InjectedCrash(2)));
        let s2 = Storage::open(dir.path()).await.unwrap();
        let st2 = s2.persistent();
        assert_eq!(st2.current_term, 4);
        assert_eq!(st2.voted_for, Some(9));
        // Log from stage-2 did not land.
        assert_eq!(st2.log.last_index(), 0);
    }

    #[test]
    fn crc32_ieee_vector() {
        // Check value for "123456789" (ITU-T V.42).
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[tokio::test]
    async fn load_v1_header_and_reject_newer_version() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Storage::open(dir.path()).await.unwrap();
        let mut st = s.persistent();
        st.current_term = 3;
        st.voted_for = Some(1);
        s.save(&st, 0).await.unwrap();
        let path = dir.path().join("raft-state.bin");
        let bytes = fs::read(&path).await.unwrap();
        assert_eq!(&bytes[0..4], b"RAFT");
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            ON_DISK_VERSION
        );
        let s2 = Storage::open(dir.path()).await.unwrap();
        assert_eq!(s2.persistent().current_term, 3);
        assert_eq!(s2.persistent().voted_for, Some(1));

        let mut bogus = bytes.clone();
        bogus[4..8].copy_from_slice(&99u32.to_le_bytes());
        fs::write(&path, &bogus).await.unwrap();
        let err = Storage::open(dir.path()).await.unwrap_err();
        assert!(
            matches!(err, StorageError::UnsupportedVersion(99)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn flipped_bit_refuses_to_boot() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Storage::open(dir.path()).await.unwrap();
        let mut st = s.persistent();
        st.current_term = 8;
        st.log.append(LogEntry {
            term: 8,
            command: Command::Put {
                key: "k".into(),
                value: "v".into(),
            },
        });
        s.save(&st, 1).await.unwrap();
        drop(s);
        let path = dir.path().join("raft-state.bin");
        let mut bytes = fs::read(&path).await.unwrap();
        assert!(bytes.len() > 16, "image too small to flip a payload byte");
        // Flip a bit inside the record body (past magic, version, and length).
        let idx = 12;
        bytes[idx] ^= 0x01;
        fs::write(&path, &bytes).await.unwrap();
        let err = Storage::open(dir.path()).await.unwrap_err();
        assert!(
            matches!(err, StorageError::Checksum),
            "corrupt file must not boot, got {err:?}"
        );
    }
}
