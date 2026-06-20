// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The compute -> safekeeper WAL ingest protocol.
//!
//! The compute node streams its Write-Ahead Log to the safekeepers as a series
//! of [`AppendRequest`]s. Each carries a contiguous run of WAL bytes starting at
//! a given LSN, tagged with the proposer's `term` (the consensus epoch). The
//! safekeeper persists the bytes durably, replicates to its peers, and replies
//! with an [`AppendResponse`] reporting how far it has flushed and the new
//! quorum-committed LSN — the position the compute node may treat as durable.
//!
//! Like the page-service protocol, this is a fixed big-endian layout so the C
//! WAL proposer and the Rust safekeeper agree byte-for-byte. It is defined and
//! round-trip tested here.

use crate::error::{Error, Result};
use crate::ids::{TenantId, TimelineId};
use crate::lsn::Lsn;

/// Protocol magic, ASCII "SWL1".
pub const MAGIC: u32 = 0x5357_4C31;
/// Protocol version.
pub const VERSION: u8 = 1;

/// Request type tag: append WAL.
pub const TYPE_APPEND: u8 = 1;

/// Response status: success.
pub const STATUS_OK: u8 = 0;
/// Response status: the proposer's term is stale; it should step down.
pub const STATUS_STALE_TERM: u8 = 1;
/// Response status: a non-contiguous or otherwise invalid append.
pub const STATUS_REJECTED: u8 = 2;

/// Fixed size of an [`AppendRequest`] header (the payload follows it).
pub const REQUEST_HEADER_LEN: usize = 60;
/// Fixed size of an [`AppendResponse`].
pub const RESPONSE_LEN: usize = 32;

/// A run of WAL bytes streamed from compute to a safekeeper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    /// Tenant whose WAL this is.
    pub tenant: TenantId,
    /// Timeline (branch) this WAL belongs to.
    pub timeline: TimelineId,
    /// Consensus term of the proposing compute node.
    pub term: u64,
    /// LSN of the first byte of `payload`.
    pub start_lsn: Lsn,
    /// The contiguous WAL bytes.
    pub payload: Vec<u8>,
}

/// A safekeeper's reply to an [`AppendRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResponse {
    /// Status code (see `STATUS_*`).
    pub status: u8,
    /// The safekeeper's current term (>= the request's on success).
    pub term: u64,
    /// How far this safekeeper has durably flushed.
    pub flush_lsn: Lsn,
    /// The quorum-committed LSN: durable on a majority of safekeepers.
    pub commit_lsn: Lsn,
}

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn get_u32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn get_u64(b: &[u8], o: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[o..o + 8]);
    u64::from_be_bytes(a)
}

impl AppendRequest {
    /// Encode the request (header + payload) to wire bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(REQUEST_HEADER_LEN + self.payload.len());
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(TYPE_APPEND);
        b.push(0); // reserved
        b.push(0); // reserved
        b.extend_from_slice(self.tenant.as_bytes()); // 8..24
        b.extend_from_slice(self.timeline.as_bytes()); // 24..40
        put_u64(&mut b, self.term); // 40..48
        put_u64(&mut b, self.start_lsn.raw()); // 48..56
        put_u32(&mut b, self.payload.len() as u32); // 56..60
        b.extend_from_slice(&self.payload);
        b
    }

    /// Read the payload length out of a 60-byte header.
    pub fn payload_len(header: &[u8]) -> Result<usize> {
        if header.len() < REQUEST_HEADER_LEN {
            return Err(Error::parse("append header too short"));
        }
        if get_u32(header, 0) != MAGIC {
            return Err(Error::parse("bad WAL protocol magic"));
        }
        if header[4] != VERSION {
            return Err(Error::parse("unsupported WAL protocol version"));
        }
        if header[5] != TYPE_APPEND {
            return Err(Error::parse("unexpected WAL message type"));
        }
        Ok(get_u32(header, 56) as usize)
    }

    /// Decode a complete request buffer (header + payload).
    pub fn decode(buf: &[u8]) -> Result<AppendRequest> {
        let plen = Self::payload_len(buf)?;
        if buf.len() < REQUEST_HEADER_LEN + plen {
            return Err(Error::parse("append payload truncated"));
        }
        Ok(AppendRequest {
            tenant: TenantId::from_bytes(buf[8..24].try_into().unwrap()),
            timeline: TimelineId::from_bytes(buf[24..40].try_into().unwrap()),
            term: get_u64(buf, 40),
            start_lsn: Lsn(get_u64(buf, 48)),
            payload: buf[REQUEST_HEADER_LEN..REQUEST_HEADER_LEN + plen].to_vec(),
        })
    }

    /// LSN just past the end of this request's payload.
    pub fn end_lsn(&self) -> Lsn {
        Lsn(self.start_lsn.raw() + self.payload.len() as u64)
    }
}

impl AppendResponse {
    /// Encode the fixed-size response.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(RESPONSE_LEN);
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(self.status);
        b.push(0); // reserved
        b.push(0); // reserved
        put_u64(&mut b, self.term); // 8..16
        put_u64(&mut b, self.flush_lsn.raw()); // 16..24
        put_u64(&mut b, self.commit_lsn.raw()); // 24..32
        b
    }

    /// Decode a fixed-size response buffer.
    pub fn decode(buf: &[u8]) -> Result<AppendResponse> {
        if buf.len() < RESPONSE_LEN {
            return Err(Error::parse("append response too short"));
        }
        if get_u32(buf, 0) != MAGIC || buf[4] != VERSION {
            return Err(Error::parse("bad WAL response header"));
        }
        Ok(AppendResponse {
            status: buf[5],
            term: get_u64(buf, 8),
            flush_lsn: Lsn(get_u64(buf, 16)),
            commit_lsn: Lsn(get_u64(buf, 24)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_request_round_trips() {
        let req = AppendRequest {
            tenant: TenantId::from_bytes([3; 16]),
            timeline: TimelineId::from_bytes([4; 16]),
            term: 7,
            start_lsn: Lsn(0x1000),
            payload: b"some-wal-bytes".to_vec(),
        };
        let bytes = req.encode();
        assert_eq!(bytes.len(), REQUEST_HEADER_LEN + req.payload.len());
        assert_eq!(AppendRequest::payload_len(&bytes).unwrap(), req.payload.len());
        assert_eq!(AppendRequest::decode(&bytes).unwrap(), req);
        assert_eq!(req.end_lsn(), Lsn(0x1000 + 14));
    }

    #[test]
    fn append_response_round_trips() {
        let resp = AppendResponse {
            status: STATUS_OK,
            term: 7,
            flush_lsn: Lsn(0x2000),
            commit_lsn: Lsn(0x1f00),
        };
        assert_eq!(AppendResponse::decode(&resp.encode()).unwrap(), resp);
    }

    #[test]
    fn decode_rejects_bad_magic_and_truncation() {
        let mut bytes = AppendRequest {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            term: 1,
            start_lsn: Lsn(0),
            payload: b"x".to_vec(),
        }
        .encode();
        bytes[0] ^= 0xFF;
        assert!(AppendRequest::decode(&bytes).is_err());
        assert!(AppendResponse::decode(&[0u8; 4]).is_err());
    }
}
