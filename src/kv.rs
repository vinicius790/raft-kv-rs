//! Linearizable KV store applied from committed Raft log entries.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::raft::config::ClusterConfig;

/// Client identity for at-most-once propose deduplication.
pub type ClientId = u64;
pub type Serial = u64;

/// Commands stored in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Noop,
    Put {
        key: String,
        value: String,
    },
    /// Membership change entry (joint or final C_new).
    ChangeConfig(ClusterConfig),
    /// Client propose with session serial (deduped on apply).
    SessionPut {
        client_id: ClientId,
        serial: Serial,
        key: String,
        value: String,
    },
    /// Remove a key. Appended last so older bincode variant tags stay stable.
    Delete {
        key: String,
    },
}

#[derive(Debug, Default, Clone)]
pub struct KvStore {
    map: HashMap<String, String>,
    /// Last applied serial per client (at-most-once).
    sessions: HashMap<ClientId, Serial>,
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, cmd: &Command) {
        match cmd {
            Command::Noop | Command::ChangeConfig(_) => {}
            Command::Put { key, value } => {
                self.map.insert(key.clone(), value.clone());
            }
            Command::SessionPut {
                client_id,
                serial,
                key,
                value,
            } => {
                let last = self.sessions.get(client_id).copied().unwrap_or(0);
                if *serial <= last {
                    return;
                }
                self.sessions.insert(*client_id, *serial);
                self.map.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.map.remove(key);
            }
        }
    }

    /// Returns true if this session put would be a duplicate (already applied).
    pub fn is_duplicate(&self, client_id: ClientId, serial: Serial) -> bool {
        self.sessions
            .get(&client_id)
            .map(|&last| serial <= last)
            .unwrap_or(false)
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn restore(&mut self, snapshot: HashMap<String, String>) {
        self.map = snapshot;
        // Sessions are part of the application snapshot for correctness of
        // at-most-once across compaction; restored separately when present.
    }

    pub fn restore_full(
        &mut self,
        snapshot: HashMap<String, String>,
        sessions: HashMap<ClientId, Serial>,
    ) {
        self.map = snapshot;
        self.sessions = sessions;
    }

    pub fn snapshot(&self) -> HashMap<String, String> {
        self.map.clone()
    }

    pub fn sessions_snapshot(&self) -> HashMap<ClientId, Serial> {
        self.sessions.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get() {
        let mut kv = KvStore::new();
        kv.apply(&Command::Put {
            key: "k".into(),
            value: "v".into(),
        });
        assert_eq!(kv.get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn session_dedup() {
        let mut kv = KvStore::new();
        let c = Command::SessionPut {
            client_id: 7,
            serial: 1,
            key: "a".into(),
            value: "1".into(),
        };
        kv.apply(&c);
        kv.apply(&Command::SessionPut {
            client_id: 7,
            serial: 1,
            key: "a".into(),
            value: "DUP".into(),
        });
        assert_eq!(kv.get("a").map(String::as_str), Some("1"));
        kv.apply(&Command::SessionPut {
            client_id: 7,
            serial: 2,
            key: "a".into(),
            value: "2".into(),
        });
        assert_eq!(kv.get("a").map(String::as_str), Some("2"));
    }
}
