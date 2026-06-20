// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The safekeeper WAL protocols: compute→safekeeper ingest, and
//! safekeeper→page-server read-back.
//!
//! **Ingest.** The compute node streams its Write-Ahead Log to the safekeepers
//! as a series of [`AppendRequest`]s. Each carries a contiguous run of WAL bytes
//! starting at a given LSN, tagged with the proposer's `term` (the consensus
//! epoch). The safekeeper persists the bytes durably, replicates to its peers,
//! and replies with an [`AppendResponse`] reporting how far it has flushed and
//! the new quorum-committed LSN — the position the compute node may treat as
//! durable.
//!
//! **Read-back.** The page server pulls *committed* WAL back out with a
//! [`ReadRequest`] from a cursor LSN; the safekeeper answers with a
//! [`ReadResponse`] carrying a chunk of `[start_lsn, commit_lsn)` plus its
//! current commit position. Both message kinds share a one-byte type tag in a
//! common 8-byte prefix so a single connection can carry either.
//!
//! Like the page-service protocol, this is a fixed big-endian layout so the C
//! WAL proposer and the Rust services agree byte-for-byte. It is defined and
//! round-trip tested here.

use crate::error::{Error, Result};
use crate::ids::{TenantId, TimelineId};
use crate::lsn::Lsn;

/// Protocol magic, ASCII "SWL1".
pub const MAGIC: u32 = 0x5357_4C31;
/// Protocol version.
pub const VERSION: u8 = 1;

/// Request type tag: append WAL (compute → safekeeper). The receiving safekeeper
/// replicates the bytes to its peers before acknowledging.
pub const TYPE_APPEND: u8 = 1;
/// Request type tag: read committed WAL (page server → safekeeper).
pub const TYPE_READ: u8 = 2;
/// Request type tag: replicate WAL (leader safekeeper → peer safekeeper). Same
/// body as an append, but the peer only stores + flushes + acks — it does not
/// re-replicate, which is what stops forwarding from looping.
pub const TYPE_REPLICATE: u8 = 3;
/// Request type tag: request a leadership vote (candidate → safekeeper).
pub const TYPE_VOTE: u8 = 4;

/// Response status: success.
pub const STATUS_OK: u8 = 0;
/// Response status: the proposer's term is stale; it should step down.
pub const STATUS_STALE_TERM: u8 = 1;
/// Response status: a non-contiguous or otherwise invalid append.
pub const STATUS_REJECTED: u8 = 2;

/// Length of the common prefix every message starts with: magic(4) version(1)
/// type(1) reserved(2). Enough to learn the message type before reading on.
pub const PREFIX_LEN: usize = 8;
/// Fixed size of an [`AppendRequest`] header (the payload follows it).
pub const REQUEST_HEADER_LEN: usize = 60;
/// Fixed size of an [`AppendResponse`].
pub const RESPONSE_LEN: usize = 32;
/// Fixed size of a [`ReadRequest`] (no payload follows).
pub const READ_REQUEST_LEN: usize = 52;
/// Fixed size of a [`ReadResponse`] header (the WAL payload follows it).
pub const READ_RESPONSE_HEADER_LEN: usize = 28;
/// Fixed size of a [`VoteRequest`].
pub const VOTE_REQUEST_LEN: usize = 56;
/// Fixed size of a [`VoteResponse`].
pub const VOTE_RESPONSE_LEN: usize = 24;

/// Validate the common prefix and return the message type byte.
pub fn message_type(prefix: &[u8]) -> Result<u8> {
    if prefix.len() < PREFIX_LEN {
        return Err(Error::parse("WAL message prefix too short"));
    }
    if get_u32(prefix, 0) != MAGIC {
        return Err(Error::parse("bad WAL protocol magic"));
    }
    if prefix[4] != VERSION {
        return Err(Error::parse("unsupported WAL protocol version"));
    }
    Ok(prefix[5])
}

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

/// A page server's request for committed WAL starting at a cursor LSN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRequest {
    /// Tenant whose WAL to read.
    pub tenant: TenantId,
    /// Timeline (branch) to read.
    pub timeline: TimelineId,
    /// LSN to begin reading from (the page server's ingest cursor).
    pub start_lsn: Lsn,
    /// Maximum number of WAL bytes to return in one chunk.
    pub max_bytes: u32,
}

/// A safekeeper's reply carrying a chunk of committed WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    /// Status code (see `STATUS_*`).
    pub status: u8,
    /// The safekeeper's current quorum-committed LSN.
    pub commit_lsn: Lsn,
    /// LSN of the first byte of `payload` (may be > the requested `start_lsn`
    /// if it had fallen below the retained range).
    pub start_lsn: Lsn,
    /// Committed WAL bytes in `[start_lsn, start_lsn + payload.len())`; empty
    /// when the reader is already caught up to `commit_lsn`.
    pub payload: Vec<u8>,
}

/// A candidate's request for a leadership vote in `term`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    /// Tenant whose safekeeper group is voting.
    pub tenant: TenantId,
    /// Timeline (branch) this group serves.
    pub timeline: TimelineId,
    /// The term the candidate is standing for.
    pub term: u64,
    /// The candidate's node id.
    pub candidate: u64,
}

/// A safekeeper's reply to a [`VoteRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResponse {
    /// Whether the vote was granted.
    pub granted: bool,
    /// The voter's current term (≥ the request's; higher means the candidate is
    /// behind and should step down).
    pub term: u64,
    /// The voter's durable flush position (so a new leader knows where to resume).
    pub flush_lsn: Lsn,
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
    /// Encode the request (header + payload) to wire bytes as a `TYPE_APPEND`.
    pub fn encode(&self) -> Vec<u8> {
        self.encode_as(TYPE_APPEND)
    }

    /// Encode the request as a `TYPE_REPLICATE` (leader → peer forwarding).
    pub fn encode_replicate(&self) -> Vec<u8> {
        self.encode_as(TYPE_REPLICATE)
    }

    fn encode_as(&self, msg_type: u8) -> Vec<u8> {
        let mut b = Vec::with_capacity(REQUEST_HEADER_LEN + self.payload.len());
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(msg_type);
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

    /// Read the payload length out of a 60-byte append/replicate header.
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
        if header[5] != TYPE_APPEND && header[5] != TYPE_REPLICATE {
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

impl ReadRequest {
    /// Encode the fixed-size read request.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(READ_REQUEST_LEN);
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(TYPE_READ);
        b.push(0); // reserved
        b.push(0); // reserved
        b.extend_from_slice(self.tenant.as_bytes()); // 8..24
        b.extend_from_slice(self.timeline.as_bytes()); // 24..40
        put_u64(&mut b, self.start_lsn.raw()); // 40..48
        put_u32(&mut b, self.max_bytes); // 48..52
        b
    }

    /// Decode a fixed-size read request buffer.
    pub fn decode(buf: &[u8]) -> Result<ReadRequest> {
        if buf.len() < READ_REQUEST_LEN {
            return Err(Error::parse("read request too short"));
        }
        if message_type(buf)? != TYPE_READ {
            return Err(Error::parse("unexpected WAL message type"));
        }
        Ok(ReadRequest {
            tenant: TenantId::from_bytes(buf[8..24].try_into().unwrap()),
            timeline: TimelineId::from_bytes(buf[24..40].try_into().unwrap()),
            start_lsn: Lsn(get_u64(buf, 40)),
            max_bytes: get_u32(buf, 48),
        })
    }
}

impl ReadResponse {
    /// Encode the response (header + WAL payload).
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(READ_RESPONSE_HEADER_LEN + self.payload.len());
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(self.status);
        b.push(0); // reserved
        b.push(0); // reserved
        put_u64(&mut b, self.commit_lsn.raw()); // 8..16
        put_u64(&mut b, self.start_lsn.raw()); // 16..24
        put_u32(&mut b, self.payload.len() as u32); // 24..28
        b.extend_from_slice(&self.payload);
        b
    }

    /// Read the payload length out of a 28-byte response header.
    pub fn payload_len(header: &[u8]) -> Result<usize> {
        if header.len() < READ_RESPONSE_HEADER_LEN {
            return Err(Error::parse("read response header too short"));
        }
        if get_u32(header, 0) != MAGIC || header[4] != VERSION {
            return Err(Error::parse("bad WAL read response header"));
        }
        Ok(get_u32(header, 24) as usize)
    }

    /// Decode a complete response buffer (header + payload).
    pub fn decode(buf: &[u8]) -> Result<ReadResponse> {
        let plen = Self::payload_len(buf)?;
        if buf.len() < READ_RESPONSE_HEADER_LEN + plen {
            return Err(Error::parse("read response payload truncated"));
        }
        Ok(ReadResponse {
            status: buf[5],
            commit_lsn: Lsn(get_u64(buf, 8)),
            start_lsn: Lsn(get_u64(buf, 16)),
            payload: buf[READ_RESPONSE_HEADER_LEN..READ_RESPONSE_HEADER_LEN + plen].to_vec(),
        })
    }
}

impl VoteRequest {
    /// Encode the fixed-size vote request.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(VOTE_REQUEST_LEN);
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(TYPE_VOTE);
        b.push(0);
        b.push(0);
        b.extend_from_slice(self.tenant.as_bytes()); // 8..24
        b.extend_from_slice(self.timeline.as_bytes()); // 24..40
        put_u64(&mut b, self.term); // 40..48
        put_u64(&mut b, self.candidate); // 48..56
        b
    }

    /// Decode a fixed-size vote request.
    pub fn decode(buf: &[u8]) -> Result<VoteRequest> {
        if buf.len() < VOTE_REQUEST_LEN {
            return Err(Error::parse("vote request too short"));
        }
        if message_type(buf)? != TYPE_VOTE {
            return Err(Error::parse("unexpected WAL message type"));
        }
        Ok(VoteRequest {
            tenant: TenantId::from_bytes(buf[8..24].try_into().unwrap()),
            timeline: TimelineId::from_bytes(buf[24..40].try_into().unwrap()),
            term: get_u64(buf, 40),
            candidate: get_u64(buf, 48),
        })
    }
}

impl VoteResponse {
    /// Encode the fixed-size vote response.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(VOTE_RESPONSE_LEN);
        put_u32(&mut b, MAGIC);
        b.push(VERSION);
        b.push(self.granted as u8);
        b.push(0);
        b.push(0);
        put_u64(&mut b, self.term); // 8..16
        put_u64(&mut b, self.flush_lsn.raw()); // 16..24
        b
    }

    /// Decode a fixed-size vote response.
    pub fn decode(buf: &[u8]) -> Result<VoteResponse> {
        if buf.len() < VOTE_RESPONSE_LEN {
            return Err(Error::parse("vote response too short"));
        }
        if get_u32(buf, 0) != MAGIC || buf[4] != VERSION {
            return Err(Error::parse("bad vote response header"));
        }
        Ok(VoteResponse {
            granted: buf[5] != 0,
            term: get_u64(buf, 8),
            flush_lsn: Lsn(get_u64(buf, 16)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_round_trips() {
        let req = VoteRequest {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            term: 7,
            candidate: 3,
        };
        let bytes = req.encode();
        assert_eq!(bytes.len(), VOTE_REQUEST_LEN);
        assert_eq!(message_type(&bytes).unwrap(), TYPE_VOTE);
        assert_eq!(VoteRequest::decode(&bytes).unwrap(), req);

        let resp = VoteResponse { granted: true, term: 7, flush_lsn: Lsn(0x4000) };
        assert_eq!(VoteResponse::decode(&resp.encode()).unwrap(), resp);
    }

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

    #[test]
    fn read_request_round_trips() {
        let req = ReadRequest {
            tenant: TenantId::from_bytes([5; 16]),
            timeline: TimelineId::from_bytes([6; 16]),
            start_lsn: Lsn(0x4000),
            max_bytes: 1 << 20,
        };
        let bytes = req.encode();
        assert_eq!(bytes.len(), READ_REQUEST_LEN);
        assert_eq!(message_type(&bytes).unwrap(), TYPE_READ);
        assert_eq!(ReadRequest::decode(&bytes).unwrap(), req);
    }

    #[test]
    fn read_response_round_trips() {
        let resp = ReadResponse {
            status: STATUS_OK,
            commit_lsn: Lsn(0x9000),
            start_lsn: Lsn(0x4000),
            payload: b"committed-wal-chunk".to_vec(),
        };
        let bytes = resp.encode();
        assert_eq!(bytes.len(), READ_RESPONSE_HEADER_LEN + resp.payload.len());
        assert_eq!(ReadResponse::payload_len(&bytes).unwrap(), resp.payload.len());
        assert_eq!(ReadResponse::decode(&bytes).unwrap(), resp);
    }

    #[test]
    fn message_type_distinguishes_append_and_read() {
        let append = AppendRequest {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            term: 1,
            start_lsn: Lsn(0),
            payload: b"x".to_vec(),
        }
        .encode();
        let read = ReadRequest {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            start_lsn: Lsn(0),
            max_bytes: 16,
        }
        .encode();
        assert_eq!(message_type(&append).unwrap(), TYPE_APPEND);
        assert_eq!(message_type(&read).unwrap(), TYPE_READ);
    }
}
