// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Routing of PostgreSQL `CancelRequest` messages.
//!
//! A client cancels a running query by opening a *new* connection and sending a
//! `CancelRequest` carrying the backend's process id and a secret key — the
//! `BackendKeyData` the server handed it at startup. Because the proxy splices
//! each session straight through to a compute backend, the backend's own
//! `(process_id, secret_key)` flows to the client unchanged, and the cancel must
//! be delivered back to *that same backend*.
//!
//! Two pieces make this work:
//!
//! * [`CancelRegistry`] — maps each live session's `(process_id, secret_key)` to
//!   the backend address that issued it.
//! * [`KeyScanner`] — sniffs the backend→client byte stream during the splice,
//!   frames the typed protocol messages, and extracts the `BackendKeyData` so the
//!   session can be registered. It only inspects; every byte is still forwarded
//!   to the client untouched.

use std::collections::HashMap;
use std::sync::Mutex;

/// A backend cancellation key: `(process_id, secret_key)`.
pub type CancelKey = (i32, i32);

/// Maps live sessions' cancellation keys to the backend that owns them.
#[derive(Default)]
pub struct CancelRegistry {
    map: Mutex<HashMap<CancelKey, String>>,
}

impl CancelRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session's key against the backend (`host:port`) that issued it.
    pub fn insert(&self, key: CancelKey, backend: impl Into<String>) {
        self.map.lock().unwrap().insert(key, backend.into());
    }

    /// Forget a session's key (called when the splice ends).
    pub fn remove(&self, key: CancelKey) {
        self.map.lock().unwrap().remove(&key);
    }

    /// Resolve a cancellation key to its backend, if still live.
    pub fn lookup(&self, key: CancelKey) -> Option<String> {
        self.map.lock().unwrap().get(&key).cloned()
    }

    /// Number of currently-tracked sessions (used in tests).
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Whether no sessions are currently tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// PostgreSQL backend message types (first byte of a typed message).
const MSG_BACKEND_KEY_DATA: u8 = b'K';
const MSG_READY_FOR_QUERY: u8 = b'Z';
/// `BackendKeyData` length field: 4 (length) + 4 (pid) + 4 (secret).
const BACKEND_KEY_DATA_LEN: usize = 12;
/// Stop buffering after this many bytes without finding the key — it always
/// arrives early (just before `ReadyForQuery`), so this only bounds memory.
const SCAN_CAP: usize = 16 * 1024;

/// Frames the typed backend→client stream to extract the first `BackendKeyData`.
///
/// Feed it the bytes the backend sends (in any chunking); it returns the
/// `(process_id, secret_key)` the moment a complete `BackendKeyData` message is
/// seen, and then goes inert. It never holds bytes back — the caller forwards
/// every byte to the client regardless.
#[derive(Default)]
pub struct KeyScanner {
    buf: Vec<u8>,
    done: bool,
}

impl KeyScanner {
    /// A fresh scanner.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether scanning has finished (key found, given up, or stream framed past
    /// the point the key can appear). Once done, [`push`](Self::push) is a no-op.
    pub fn done(&self) -> bool {
        self.done
    }

    /// Feed the next chunk of backend→client bytes; returns the cancellation key
    /// once a complete `BackendKeyData` message has been framed.
    pub fn push(&mut self, bytes: &[u8]) -> Option<CancelKey> {
        if self.done {
            return None;
        }
        self.buf.extend_from_slice(bytes);

        let mut off = 0;
        while self.buf.len() - off >= 5 {
            let typ = self.buf[off];
            let len = i32::from_be_bytes(self.buf[off + 1..off + 5].try_into().unwrap());
            // A valid message length covers at least its own 4-byte field.
            if len < 4 {
                self.done = true;
                return None;
            }
            let len = len as usize;
            // Need the whole message body (type byte + `len` payload bytes).
            if self.buf.len() - off - 1 < len {
                break;
            }
            if typ == MSG_BACKEND_KEY_DATA && len == BACKEND_KEY_DATA_LEN {
                let p = off + 5;
                let pid = i32::from_be_bytes(self.buf[p..p + 4].try_into().unwrap());
                let secret = i32::from_be_bytes(self.buf[p + 4..p + 8].try_into().unwrap());
                self.done = true;
                return Some((pid, secret));
            }
            // The key always precedes ReadyForQuery; if we reach it, give up.
            if typ == MSG_READY_FOR_QUERY {
                self.done = true;
                return None;
            }
            off += 1 + len;
        }

        // Drop fully-consumed messages and bound memory.
        if off > 0 {
            self.buf.drain(0..off);
        }
        if self.buf.len() > SCAN_CAP {
            self.done = true;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(typ: u8, payload: &[u8]) -> Vec<u8> {
        let mut m = vec![typ];
        m.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(payload);
        m
    }

    fn backend_key_data(pid: i32, secret: i32) -> Vec<u8> {
        let mut p = pid.to_be_bytes().to_vec();
        p.extend_from_slice(&secret.to_be_bytes());
        msg(MSG_BACKEND_KEY_DATA, &p)
    }

    /// A realistic startup reply: AuthenticationOk, a ParameterStatus, the
    /// BackendKeyData, then ReadyForQuery.
    fn startup_reply(pid: i32, secret: i32) -> Vec<u8> {
        let mut s = msg(b'R', &0i32.to_be_bytes()); // AuthenticationOk
        s.extend_from_slice(&msg(b'S', b"server_version\x0016\x00")); // ParameterStatus
        s.extend_from_slice(&backend_key_data(pid, secret));
        s.extend_from_slice(&msg(MSG_READY_FOR_QUERY, b"I")); // ReadyForQuery (idle)
        s
    }

    #[test]
    fn extracts_backend_key_data_from_a_whole_buffer() {
        let mut sc = KeyScanner::new();
        assert_eq!(sc.push(&startup_reply(4242, 2024)), Some((4242, 2024)));
        assert!(sc.done());
        // Inert afterwards.
        assert_eq!(sc.push(&backend_key_data(1, 1)), None);
    }

    #[test]
    fn extracts_key_when_fed_one_byte_at_a_time() {
        let stream = startup_reply(-7, 99); // negative pid exercises sign bits
        let mut sc = KeyScanner::new();
        let mut found = None;
        for b in stream {
            if let Some(k) = sc.push(&[b]) {
                found = Some(k);
                break;
            }
        }
        assert_eq!(found, Some((-7, 99)));
    }

    #[test]
    fn gives_up_at_ready_for_query_without_a_key() {
        let mut sc = KeyScanner::new();
        let mut s = msg(b'R', &0i32.to_be_bytes());
        s.extend_from_slice(&msg(MSG_READY_FOR_QUERY, b"I"));
        assert_eq!(sc.push(&s), None);
        assert!(sc.done(), "should stop scanning once ReadyForQuery is seen");
    }

    #[test]
    fn registry_insert_lookup_remove() {
        let reg = CancelRegistry::new();
        assert!(reg.is_empty());
        reg.insert((10, 20), "127.0.0.1:6543");
        assert_eq!(reg.lookup((10, 20)).as_deref(), Some("127.0.0.1:6543"));
        assert_eq!(reg.lookup((10, 21)), None);
        assert_eq!(reg.len(), 1);
        reg.remove((10, 20));
        assert!(reg.is_empty());
    }
}
