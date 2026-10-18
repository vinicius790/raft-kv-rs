//! In-process ring of protocol events. Not a metrics exporter.
//!
//! The buffer keeps the newest events only. Export is a text dump (`trace`),
//! not a Prometheus scrape and not a rate.

use std::collections::VecDeque;

/// Default capacity. Older events are dropped, not summarized.
pub const DEFAULT_TRACE_CAP: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    pub seq: u64,
    pub kind: &'static str,
    pub term: u64,
    pub index: u64,
}

#[derive(Debug, Clone)]
pub struct TraceRing {
    cap: usize,
    next_seq: u64,
    events: VecDeque<TraceEvent>,
}

impl Default for TraceRing {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_TRACE_CAP)
    }
}

impl TraceRing {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            next_seq: 1,
            events: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn push(&mut self, kind: &'static str, term: u64, index: u64) {
        if self.events.len() == self.cap {
            self.events.pop_front();
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.events.push_back(TraceEvent {
            seq,
            kind,
            term,
            index,
        });
    }

    /// One event per line: `seq=<n> kind=<term|election|commit|snapshot> term=<t> index=<i>`.
    pub fn render(&self) -> String {
        let mut lines = Vec::with_capacity(self.events.len());
        for e in &self.events {
            lines.push(format!(
                "seq={} kind={} term={} index={}",
                e.seq, e.kind, e.term, e.index
            ));
        }
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_drops_oldest_and_keeps_order() {
        let mut ring = TraceRing::with_capacity(3);
        ring.push("term", 1, 0);
        ring.push("election", 1, 0);
        ring.push("commit", 1, 4);
        ring.push("snapshot", 2, 4);
        assert_eq!(ring.len(), 3);
        let text = ring.render();
        assert!(!text.contains("kind=term"));
        assert!(text.contains("kind=election"));
        assert!(text.contains("kind=commit"));
        assert!(text.contains("kind=snapshot"));
        let seqs: Vec<_> = text
            .lines()
            .map(|l| l.split_whitespace().next().unwrap().to_string())
            .collect();
        assert_eq!(seqs, vec!["seq=2", "seq=3", "seq=4"]);
    }
}
