//! Client session for at-most-once KV puts.
//!
//! The server dedups on `(client_id, serial)`. This helper keeps the serial
//! stable across retries until the caller marks the propose committed.

/// Tracks one client's next serial. `client_id` must be non-zero.
#[derive(Debug, Clone)]
pub struct SessionClient {
    client_id: u64,
    /// Next serial to allocate after the in-flight one commits.
    next: u64,
    /// Serial of a propose that has not been acknowledged yet.
    inflight: Option<u64>,
}

impl SessionClient {
    pub fn new(client_id: u64) -> Self {
        Self {
            client_id,
            next: 1,
            inflight: None,
        }
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Serial to send. Repeats the in-flight serial until [`commit`](Self::commit).
    pub fn serial_for_attempt(&mut self) -> u64 {
        if let Some(s) = self.inflight {
            return s;
        }
        let s = self.next;
        self.inflight = Some(s);
        s
    }

    /// The propose for the current serial was applied. The next attempt uses a new serial.
    pub fn commit(&mut self) {
        if let Some(s) = self.inflight.take() {
            self.next = s.saturating_add(1);
        }
    }

    pub fn inflight(&self) -> Option<u64> {
        self.inflight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_reuses_serial_until_commit() {
        let mut c = SessionClient::new(7);
        assert_eq!(c.serial_for_attempt(), 1);
        assert_eq!(c.serial_for_attempt(), 1, "retry must not allocate");
        c.commit();
        assert_eq!(c.serial_for_attempt(), 2);
        assert_eq!(c.client_id(), 7);
    }
}
