//! Linearizability checker for a key/value register history (Wing & Gong
//! search with memoisation, per key, as in Knossos/Porcupine).
//!
//! Every operation has an invocation and (maybe) a completion time. A write
//! whose outcome is unknown (timeout) has no completion: it may take effect at
//! any point after its invocation, or never. Reads with unknown outcome carry
//! no information and should simply be left out of the history.

use std::collections::{BTreeMap, HashSet};

/// One client operation on a single key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    pub key: String,
    pub kind: OpKind,
    /// Invocation time (any monotonic unit).
    pub call: u64,
    /// Completion time; `None` = outcome unknown (writes only).
    pub ret: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpKind {
    /// `Some(v)` = put, `None` = delete.
    Write(Option<String>),
    /// Observed value (`None` = key absent).
    Read(Option<String>),
}

/// Largest per-key history the exact search accepts.
pub const MAX_OPS_PER_KEY: usize = 64;

/// Returns `Ok(())` if some total order of the operations respects real time
/// and register semantics, per key (registers are independent, so the whole
/// history is linearizable iff every key's sub-history is).
pub fn check(history: &[Op]) -> Result<(), String> {
    let mut by_key: BTreeMap<&str, Vec<&Op>> = BTreeMap::new();
    for op in history {
        if matches!(op.kind, OpKind::Read(_)) && op.ret.is_none() {
            return Err(format!("read without completion on {}", op.key));
        }
        by_key.entry(&op.key).or_default().push(op);
    }
    for (key, ops) in by_key {
        if ops.len() > MAX_OPS_PER_KEY {
            return Err(format!("{key}: {} ops > {MAX_OPS_PER_KEY}", ops.len()));
        }
        if !check_key(&ops) {
            return Err(format!(
                "history on key {key:?} is not linearizable: {ops:#?}"
            ));
        }
    }
    Ok(())
}

fn check_key(ops: &[&Op]) -> bool {
    let n = ops.len();
    // Ops that must be linearized: everything except unknown-outcome writes.
    let required: u64 = (0..n)
        .filter(|&i| ops[i].ret.is_some())
        .fold(0, |m, i| m | (1 << i));
    let mut seen: HashSet<(u64, Option<String>)> = HashSet::new();
    let mut stack: Vec<(u64, Option<String>)> = vec![(0, None)];
    while let Some((done, state)) = stack.pop() {
        if done & required == required {
            return true;
        }
        if !seen.insert((done, state.clone())) {
            continue;
        }
        // Earliest completion among pending required ops bounds who may go next.
        let horizon = (0..n)
            .filter(|&i| done & (1 << i) == 0)
            .filter_map(|i| ops[i].ret)
            .min()
            .unwrap_or(u64::MAX);
        for (i, op) in ops.iter().enumerate() {
            let bit = 1u64 << i;
            if done & bit != 0 || op.call > horizon {
                continue;
            }
            match &op.kind {
                OpKind::Write(v) => stack.push((done | bit, v.clone())),
                OpKind::Read(v) if *v == state => stack.push((done | bit, state.clone())),
                OpKind::Read(_) => {}
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(v: &str, call: u64, ret: Option<u64>) -> Op {
        Op {
            key: "k".into(),
            kind: OpKind::Write(Some(v.into())),
            call,
            ret,
        }
    }
    fn r(v: Option<&str>, call: u64, ret: u64) -> Op {
        Op {
            key: "k".into(),
            kind: OpKind::Read(v.map(Into::into)),
            call,
            ret: Some(ret),
        }
    }

    #[test]
    fn sequential_and_concurrent_histories_pass() {
        assert!(check(&[w("a", 0, Some(1)), r(Some("a"), 2, 3)]).is_ok());
        // Overlapping write: read may see old or new value.
        assert!(check(&[w("a", 0, Some(1)), w("b", 2, Some(10)), r(Some("a"), 3, 4)]).is_ok());
        assert!(check(&[w("a", 0, Some(1)), w("b", 2, Some(10)), r(Some("b"), 3, 4)]).is_ok());
        assert!(check(&[r(None, 0, 1)]).is_ok());
    }

    #[test]
    fn stale_read_is_rejected() {
        // b is acknowledged before the read starts; reading a is stale.
        let h = [w("a", 0, Some(1)), w("b", 2, Some(3)), r(Some("a"), 4, 5)];
        assert!(check(&h).is_err());
        // Value from nowhere.
        assert!(check(&[r(Some("z"), 0, 1)]).is_err());
        // Non-monotonic reads after a completed write.
        let h = [
            w("a", 0, Some(1)),
            w("b", 2, Some(20)),
            r(Some("b"), 3, 4),
            r(Some("a"), 5, 6),
        ];
        assert!(check(&h).is_err());
    }

    #[test]
    fn unknown_writes_may_or_may_not_apply() {
        assert!(check(&[w("a", 0, None), r(Some("a"), 5, 6)]).is_ok());
        assert!(check(&[w("a", 0, None), r(None, 5, 6)]).is_ok());
        // ...but cannot take effect before they were invoked.
        assert!(check(&[r(Some("a"), 0, 1), w("a", 2, None)]).is_err());
    }
}
