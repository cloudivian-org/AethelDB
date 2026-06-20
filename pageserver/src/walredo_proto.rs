// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The page-server ↔ wal-redo process pipe protocol (Phase 3).
//!
//! Reconstructing a page from a non-full-page WAL record needs Postgres's own
//! per-resource-manager redo routines, so the page server ships the work to a
//! child *wal-redo* process: "here is page `(rel, blk)`, here is its base image
//! and the WAL records that follow — apply them and hand the page back."
//!
//! This module defines that request/response framing. It is deliberately small
//! and self-describing so the Rust [`PostgresRedoManager`](crate::walredo) and
//! the C wal-redo backend (see `compute/walredo/`) agree byte-for-byte. The
//! envelope is big-endian to match the project's other wire protocols; the WAL
//! record *bytes* inside are opaque to this layer (Postgres-native order).

use common::{Lsn, RelTag, PAGE_SIZE};
use thiserror::Error;

/// Request magic, ASCII "WRD1".
pub const REQ_MAGIC: u32 = 0x5752_4431;
/// Response magic, ASCII "WRR1".
pub const RESP_MAGIC: u32 = 0x5752_5231;
/// Protocol version.
pub const VERSION: u8 = 1;

/// `flags` bit: a base image follows the header (else the base is all zeros).
pub const FLAG_HAS_BASE: u8 = 0x01;

/// Fixed size of a response header (magic, version, status, reserved).
pub const RESP_HEADER_LEN: usize = 8;
/// Response status: the 8 KiB page follows.
pub const STATUS_OK: u8 = 0;
/// Response status: an error message (`u32` length + bytes) follows.
pub const STATUS_ERR: u8 = 1;

/// Errors decoding a redo message.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("redo message truncated while reading {0}")]
    Truncated(&'static str),
    #[error("bad redo magic {got:#010x}, expected {want:#010x}")]
    BadMagic { got: u32, want: u32 },
    #[error("unsupported redo protocol version {0}")]
    BadVersion(u8),
    #[error("base image is {0} bytes, expected {PAGE_SIZE}")]
    BadBaseSize(usize),
    #[error("unknown response status {0}")]
    BadStatus(u8),
}

/// A request to reconstruct one page: a base image plus the WAL records to apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedoRequest {
    /// Relation and fork of the page.
    pub rel: RelTag,
    /// Block number within the fork.
    pub blkno: u32,
    /// The base page image; `None` means start from an all-zero page (e.g. a
    /// `will_init` record reinitializes the page).
    pub base_image: Option<Vec<u8>>,
    /// WAL records to apply in order, each as `(lsn, raw record bytes)`.
    pub records: Vec<(Lsn, Vec<u8>)>,
}

/// A wal-redo reply: either the reconstructed page or an error message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedoResponse {
    /// The reconstructed 8 KiB page.
    Page(Vec<u8>),
    /// Redo failed, with a human-readable reason.
    Error(String),
}

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn get_u32(b: &[u8], o: usize, what: &'static str) -> Result<u32, ProtoError> {
    let s = b.get(o..o + 4).ok_or(ProtoError::Truncated(what))?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}
fn get_u64(b: &[u8], o: usize, what: &'static str) -> Result<u64, ProtoError> {
    let s = b.get(o..o + 8).ok_or(ProtoError::Truncated(what))?;
    let mut a = [0u8; 8];
    a.copy_from_slice(s);
    Ok(u64::from_be_bytes(a))
}

impl RedoRequest {
    /// Encode the request to wire bytes (header + optional base + records).
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(40 + PAGE_SIZE + self.records.iter().map(|(_, r)| r.len() + 12).sum::<usize>());
        put_u32(&mut b, REQ_MAGIC);
        b.push(VERSION);
        b.push(if self.base_image.is_some() { FLAG_HAS_BASE } else { 0 });
        b.extend_from_slice(&[0, 0]); // reserved
        put_u32(&mut b, self.rel.spc_node);
        put_u32(&mut b, self.rel.db_node);
        put_u32(&mut b, self.rel.rel_node);
        b.push(self.rel.fork as u8);
        b.extend_from_slice(&[0, 0, 0]); // reserved
        put_u32(&mut b, self.blkno);
        if let Some(img) = &self.base_image {
            b.extend_from_slice(img);
        }
        put_u32(&mut b, self.records.len() as u32);
        for (lsn, rec) in &self.records {
            put_u64(&mut b, lsn.raw());
            put_u32(&mut b, rec.len() as u32);
            b.extend_from_slice(rec);
        }
        b
    }

    /// Decode a request from a complete buffer (used by the wal-redo process).
    pub fn decode(buf: &[u8]) -> Result<RedoRequest, ProtoError> {
        let magic = get_u32(buf, 0, "magic")?;
        if magic != REQ_MAGIC {
            return Err(ProtoError::BadMagic { got: magic, want: REQ_MAGIC });
        }
        let version = *buf.get(4).ok_or(ProtoError::Truncated("version"))?;
        if version != VERSION {
            return Err(ProtoError::BadVersion(version));
        }
        let flags = *buf.get(5).ok_or(ProtoError::Truncated("flags"))?;
        let spc = get_u32(buf, 8, "spc")?;
        let db = get_u32(buf, 12, "db")?;
        let relnode = get_u32(buf, 16, "relnode")?;
        let fork_raw = *buf.get(20).ok_or(ProtoError::Truncated("fork"))?;
        let fork = common::ForkNumber::from_raw(fork_raw).ok_or(ProtoError::Truncated("fork"))?;
        let blkno = get_u32(buf, 24, "blkno")?;

        let mut pos = 28;
        let base_image = if flags & FLAG_HAS_BASE != 0 {
            let img = buf.get(pos..pos + PAGE_SIZE).ok_or(ProtoError::Truncated("base image"))?.to_vec();
            pos += PAGE_SIZE;
            Some(img)
        } else {
            None
        };

        let n = get_u32(buf, pos, "n_records")? as usize;
        pos += 4;
        let mut records = Vec::with_capacity(n);
        for _ in 0..n {
            let lsn = Lsn(get_u64(buf, pos, "record lsn")?);
            pos += 8;
            let len = get_u32(buf, pos, "record len")? as usize;
            pos += 4;
            let rec = buf.get(pos..pos + len).ok_or(ProtoError::Truncated("record bytes"))?.to_vec();
            pos += len;
            records.push((lsn, rec));
        }
        Ok(RedoRequest { rel: RelTag { spc_node: spc, db_node: db, rel_node: relnode, fork }, blkno, base_image, records })
    }
}

impl RedoResponse {
    /// Encode the response to wire bytes (used by the wal-redo process).
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        put_u32(&mut b, RESP_MAGIC);
        b.push(VERSION);
        match self {
            RedoResponse::Page(page) => {
                b.push(STATUS_OK);
                b.extend_from_slice(&[0, 0]); // reserved
                b.extend_from_slice(page);
            }
            RedoResponse::Error(msg) => {
                b.push(STATUS_ERR);
                b.extend_from_slice(&[0, 0]); // reserved
                put_u32(&mut b, msg.len() as u32);
                b.extend_from_slice(msg.as_bytes());
            }
        }
        b
    }

    /// Decode a response from a complete buffer (used in tests).
    pub fn decode(buf: &[u8]) -> Result<RedoResponse, ProtoError> {
        let magic = get_u32(buf, 0, "magic")?;
        if magic != RESP_MAGIC {
            return Err(ProtoError::BadMagic { got: magic, want: RESP_MAGIC });
        }
        if *buf.get(4).ok_or(ProtoError::Truncated("version"))? != VERSION {
            return Err(ProtoError::BadVersion(buf[4]));
        }
        match *buf.get(5).ok_or(ProtoError::Truncated("status"))? {
            STATUS_OK => {
                let page = buf.get(RESP_HEADER_LEN..RESP_HEADER_LEN + PAGE_SIZE).ok_or(ProtoError::Truncated("page"))?.to_vec();
                Ok(RedoResponse::Page(page))
            }
            STATUS_ERR => {
                let len = get_u32(buf, RESP_HEADER_LEN, "error len")? as usize;
                let msg = buf.get(RESP_HEADER_LEN + 4..RESP_HEADER_LEN + 4 + len).ok_or(ProtoError::Truncated("error msg"))?;
                Ok(RedoResponse::Error(String::from_utf8_lossy(msg).into_owned()))
            }
            other => Err(ProtoError::BadStatus(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ForkNumber;

    fn rel() -> RelTag {
        RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main }
    }

    #[test]
    fn request_round_trips_with_base_and_records() {
        let req = RedoRequest {
            rel: rel(),
            blkno: 42,
            base_image: Some(vec![7u8; PAGE_SIZE]),
            records: vec![(Lsn(10), vec![1, 2, 3]), (Lsn(20), vec![9; 100])],
        };
        assert_eq!(RedoRequest::decode(&req.encode()).unwrap(), req);
    }

    #[test]
    fn request_round_trips_without_base() {
        let req = RedoRequest {
            rel: rel(),
            blkno: 0,
            base_image: None,
            records: vec![(Lsn(5), vec![0xAB, 0xCD])],
        };
        let decoded = RedoRequest::decode(&req.encode()).unwrap();
        assert_eq!(decoded, req);
        assert!(decoded.base_image.is_none());
    }

    #[test]
    fn response_page_round_trips() {
        let resp = RedoResponse::Page(vec![0xEE; PAGE_SIZE]);
        assert_eq!(RedoResponse::decode(&resp.encode()).unwrap(), resp);
    }

    #[test]
    fn response_error_round_trips() {
        let resp = RedoResponse::Error("rmgr 12 redo failed".to_string());
        assert_eq!(RedoResponse::decode(&resp.encode()).unwrap(), resp);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = RedoResponse::Page(vec![0; PAGE_SIZE]).encode();
        bytes[0] ^= 0xFF;
        assert!(matches!(RedoResponse::decode(&bytes), Err(ProtoError::BadMagic { .. })));
    }
}
