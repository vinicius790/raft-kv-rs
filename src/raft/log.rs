//! Raft log: term-tagged command entries with 1-based indexing and prefix compaction.

use serde::{Deserialize, Serialize};

use crate::kv::Command;
use crate::rpc::{LogIndex, Term};

/// A single log entry. Real entries start at index 1 (or after a snapshot).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub command: Command,
}

/// Compactable log. Indices `1..=snapshot_index` are discarded; `snapshot_index`
/// is represented only by `(snapshot_index, snapshot_term)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLog {
    /// Highest index covered by an installed snapshot (0 = none).
    pub snapshot_index: LogIndex,
    /// Term of the entry at `snapshot_index` (0 if none).
    pub snapshot_term: Term,
    /// Contiguous entries with Raft indices `snapshot_index+1 ..`.
    entries: Vec<LogEntry>,
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            snapshot_index: 0,
            snapshot_term: 0,
            entries: Vec::new(),
        }
    }

    pub fn last_index(&self) -> LogIndex {
        self.snapshot_index + self.entries.len() as LogIndex
    }

    pub fn last_term(&self) -> Term {
        if let Some(e) = self.entries.last() {
            e.term
        } else {
            self.snapshot_term
        }
    }

    pub fn first_index(&self) -> LogIndex {
        self.snapshot_index + 1
    }

    pub fn is_empty(&self) -> bool {
        self.last_index() == 0
    }

    pub fn len(&self) -> LogIndex {
        self.last_index()
    }

    fn offset_of(&self, index: LogIndex) -> Option<usize> {
        if index == 0 {
            return None;
        }
        if index <= self.snapshot_index {
            return None;
        }
        let off = (index - self.snapshot_index - 1) as usize;
        if off < self.entries.len() {
            Some(off)
        } else {
            None
        }
    }

    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.offset_of(index).map(|o| self.entries[o].term)
    }

    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        self.offset_of(index).map(|o| &self.entries[o])
    }

    pub fn append(&mut self, entry: LogEntry) -> LogIndex {
        self.entries.push(entry);
        self.last_index()
    }

    /// Truncate everything after `index` (keep through `index`).
    pub fn truncate_after(&mut self, index: LogIndex) {
        if index < self.snapshot_index {
            self.entries.clear();
            return;
        }
        let keep = (index - self.snapshot_index) as usize;
        if keep < self.entries.len() {
            self.entries.truncate(keep);
        }
    }

    /// Discard prefix through `last_included` (must already be applied/committed).
    pub fn compact_through(&mut self, last_included: LogIndex, last_included_term: Term) {
        if last_included <= self.snapshot_index {
            self.snapshot_index = last_included;
            self.snapshot_term = last_included_term;
            return;
        }
        if last_included > self.last_index() {
            self.entries.clear();
            self.snapshot_index = last_included;
            self.snapshot_term = last_included_term;
            return;
        }
        let drop = (last_included - self.snapshot_index) as usize;
        if drop <= self.entries.len() {
            self.entries.drain(..drop);
        } else {
            self.entries.clear();
        }
        self.snapshot_index = last_included;
        self.snapshot_term = last_included_term;
    }

    /// Replace state with an installed snapshot (discard conflicting prefix/suffix).
    pub fn install_snapshot(&mut self, last_included: LogIndex, last_included_term: Term) {
        if last_included < self.snapshot_index {
            return;
        }
        if last_included == self.snapshot_index {
            self.snapshot_term = last_included_term;
            return;
        }
        if last_included < self.last_index() {
            // Keep suffix after last_included if present.
            let drop = (last_included - self.snapshot_index) as usize;
            if drop <= self.entries.len() {
                self.entries.drain(..drop);
            } else {
                self.entries.clear();
            }
        } else {
            self.entries.clear();
        }
        self.snapshot_index = last_included;
        self.snapshot_term = last_included_term;
    }

    /// Append entries starting at `prev_log_index + 1`, truncating conflicts.
    pub fn append_entries(&mut self, prev_log_index: LogIndex, entries: &[LogEntry]) -> LogIndex {
        for (offset, entry) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + offset as LogIndex;
            if idx <= self.snapshot_index {
                continue;
            }
            if let Some(existing) = self.entry(idx) {
                if existing.term != entry.term {
                    self.truncate_after(idx - 1);
                    self.entries.push(entry.clone());
                }
            } else {
                // Must be contiguous.
                if idx != self.last_index() + 1 {
                    // Gap: stop; caller should install snapshot.
                    break;
                }
                self.entries.push(entry.clone());
            }
        }
        self.last_index()
    }

    pub fn entries_from(&self, from_index: LogIndex) -> Vec<LogEntry> {
        if from_index == 0 {
            return self.entries.clone();
        }
        if from_index <= self.snapshot_index {
            return self.entries.clone();
        }
        match self.offset_of(from_index) {
            Some(o) => self.entries[o..].to_vec(),
            None => Vec::new(),
        }
    }

    /// Limited batch for AppendEntries pipelining.
    pub fn entries_range(&self, from_index: LogIndex, max: usize) -> Vec<LogEntry> {
        let mut v = self.entries_from(from_index);
        if v.len() > max {
            v.truncate(max);
        }
        v
    }

    pub fn slice(&self, start: LogIndex, end_inclusive: LogIndex) -> Vec<LogEntry> {
        if start == 0 || start > end_inclusive {
            return Vec::new();
        }
        let mut out = Vec::new();
        for i in start..=end_inclusive {
            if let Some(e) = self.entry(i) {
                out.push(e.clone());
            }
        }
        out
    }

    /// Candidate's log is at least as up-to-date as ours (Raft §5.4.1).
    pub fn is_other_up_to_date(&self, other_last_index: LogIndex, other_last_term: Term) -> bool {
        let my_term = self.last_term();
        let my_index = self.last_index();
        other_last_term > my_term || (other_last_term == my_term && other_last_index >= my_index)
    }

    pub fn all_entries(&self) -> &[LogEntry] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_terms() {
        let mut log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        log.append(LogEntry {
            term: 1,
            command: Command::Put {
                key: "a".into(),
                value: "1".into(),
            },
        });
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.term_at(1), Some(1));
    }

    #[test]
    fn truncate_on_conflict() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 1,
            command: Command::Noop,
        });
        log.append(LogEntry {
            term: 1,
            command: Command::Noop,
        });
        log.append_entries(
            1,
            &[LogEntry {
                term: 2,
                command: Command::Put {
                    key: "x".into(),
                    value: "y".into(),
                },
            }],
        );
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(2), Some(2));
    }

    #[test]
    fn up_to_date_rule() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 2,
            command: Command::Noop,
        });
        assert!(log.is_other_up_to_date(1, 2));
        assert!(log.is_other_up_to_date(5, 2));
        assert!(!log.is_other_up_to_date(10, 1));
        assert!(log.is_other_up_to_date(0, 3));
    }

    #[test]
    fn compact_prefix() {
        let mut log = RaftLog::new();
        for t in 1..=5 {
            log.append(LogEntry {
                term: 1,
                command: Command::Put {
                    key: format!("k{t}"),
                    value: "v".into(),
                },
            });
        }
        log.compact_through(3, 1);
        assert_eq!(log.snapshot_index, 3);
        assert_eq!(log.first_index(), 4);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.term_at(3), Some(1));
        assert!(log.entry(2).is_none());
        assert!(log.entry(4).is_some());
    }
}
