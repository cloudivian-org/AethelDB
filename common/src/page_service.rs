// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The compute <-> page server wire protocol.
//!
//! This is the contract the patched PostgreSQL storage manager (the C
//! `aethel_smgr` extension) speaks to the Rust page server. It is intentionally a
//! fixed-layout, big-endian binary protocol so the C encoder is trivial and
//! provably matches the Rust codec defined and tested here.
//!
//! Two request types exist so far:
//! * [`Request::GetPage`] — fetch one 8 KiB page at a given LSN.
//! * [`Request::GetRelSize`] — fetch a relation fork's block count at an LSN,
//!   which the storage manager needs to answer `smgrnblocks`.
//!
//! Every message — request or response — is `[8-byte header][payload]`:
//!
//! ```text
//! Request  header:  magic:u32  version:u8  type:u8  fork:u8  flags:u8
//! Response header:  magic:u32  version:u8  status:u8  rsvd:u8  rsvd:u8
//!          then:    payload_len:u32  payload[payload_len]
//! ```
//! (Requests carry their fixed-size payload immediately after the header; the
//! payload length is implied by the type. Responses are length-prefixed.)

use crate::error::{Error, Result};
use crate::ids::{TenantId, TimelineId};
use crate::lsn::Lsn;
use crate::page::{ForkNumber, RelTag, PAGE_SIZE};

/// Protocol magic, ASCII "SPG1".
pub const MAGIC: u32 = 0x5350_4731;
/// Protocol version.
pub const VERSION: u8 = 1;

/// Request type tag: fetch a page.
pub const TYPE_GET_PAGE: u8 = 1;
/// Request type tag: fetch a relation fork size.
pub const TYPE_GET_REL_SIZE: u8 = 2;

/// Response status: success.
pub const STATUS_OK: u8 = 0;
/// Response status: the page/relation does not exist at the requested LSN.
pub const STATUS_NOT_FOUND: u8 = 1;
/// Response status: server-side error; payload is a UTF-8 message.
pub const STATUS_ERROR: u8 = 2;

/// Fixed size, in bytes, of a `GetPage` request payload (after the 8-byte header).
const GET_PAGE_PAYLOAD: usize = 16 + 16 + 4 + 4 + 4 + 4 + 8; // tenant,timeline,spc,db,rel,block,lsn
/// Fixed size of a `GetRelSize` request payload.
const GET_REL_SIZE_PAYLOAD: usize = 16 + 16 + 4 + 4 + 4 + 8; // tenant,timeline,spc,db,rel,lsn

const HEADER_LEN: usize = 8;

/// A request from the compute storage manager to the page server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Fetch the 8 KiB image of one page as of `lsn` (an [`Lsn::INVALID`] lsn
    /// means "the latest version").
    GetPage { tenant: TenantId, timeline: TimelineId, rel: RelTag, block: u32, lsn: Lsn },
    /// Fetch the number of blocks in a relation fork as of `lsn`.
    GetRelSize { tenant: TenantId, timeline: TimelineId, rel: RelTag, lsn: Lsn },
}

/// A response from the page server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// A full 8 KiB page image.
    Page(Vec<u8>),
    /// A relation fork's block count.
    RelSize(u32),
    /// The requested object does not exist at that LSN.
    NotFound,
    /// A server-side error with a human-readable message.
    Error(String),
}

/// Small helpers for big-endian encoding.
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn get_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}
fn get_u64(buf: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[off..off + 8]);
    u64::from_be_bytes(a)
}

impl Request {
    /// Encode this request to its on-the-wire bytes.
    pub fn encode(&self) -> Vec<u8> {
        let (type_tag, fork, tenant, timeline, rel, payload_extra) = match self {
            Request::GetPage { tenant, timeline, rel, block, lsn } => {
                (TYPE_GET_PAGE, rel.fork, tenant, timeline, rel, Some((*block, *lsn)))
            }
            Request::GetRelSize { tenant, timeline, rel, lsn } => {
                (TYPE_GET_REL_SIZE, rel.fork, tenant, timeline, rel, Some((u32::MAX, *lsn)))
            }
        };

        let mut buf = Vec::with_capacity(HEADER_LEN + GET_PAGE_PAYLOAD);
        // Header.
        put_u32(&mut buf, MAGIC);
        buf.push(VERSION);
        buf.push(type_tag);
        buf.push(fork as u8);
        buf.push(0); // flags

        // Common payload prefix.
        buf.extend_from_slice(tenant.as_bytes());
        buf.extend_from_slice(timeline.as_bytes());
        put_u32(&mut buf, rel.spc_node);
        put_u32(&mut buf, rel.db_node);
        put_u32(&mut buf, rel.rel_node);

        match self {
            Request::GetPage { block, lsn, .. } => {
                let _ = payload_extra;
                put_u32(&mut buf, *block);
                put_u64(&mut buf, lsn.raw());
            }
            Request::GetRelSize { lsn, .. } => {
                put_u64(&mut buf, lsn.raw());
            }
        }
        buf
    }

    /// Given an 8-byte request header, return the total on-wire length of the
    /// message (header + the type's fixed payload), so a server can read the
    /// exact number of bytes before decoding.
    pub fn total_len(header: &[u8]) -> Result<usize> {
        if header.len() < HEADER_LEN {
            return Err(Error::parse("request header too short"));
        }
        if get_u32(header, 0) != MAGIC {
            return Err(Error::parse("bad protocol magic"));
        }
        if header[4] != VERSION {
            return Err(Error::parse("unsupported protocol version"));
        }
        match header[5] {
            TYPE_GET_PAGE => Ok(HEADER_LEN + GET_PAGE_PAYLOAD),
            TYPE_GET_REL_SIZE => Ok(HEADER_LEN + GET_REL_SIZE_PAYLOAD),
            _ => Err(Error::parse("unknown request type")),
        }
    }

    /// Decode a request from a complete message buffer.
    pub fn decode(buf: &[u8]) -> Result<Request> {
        if buf.len() < HEADER_LEN {
            return Err(Error::parse("request shorter than header"));
        }
        if get_u32(buf, 0) != MAGIC {
            return Err(Error::parse("bad protocol magic"));
        }
        if buf[4] != VERSION {
            return Err(Error::parse("unsupported protocol version"));
        }
        let type_tag = buf[5];
        let fork =
            ForkNumber::from_raw(buf[6]).ok_or_else(|| Error::parse("invalid fork number"))?;

        let body = &buf[HEADER_LEN..];
        let expected = match type_tag {
            TYPE_GET_PAGE => GET_PAGE_PAYLOAD,
            TYPE_GET_REL_SIZE => GET_REL_SIZE_PAYLOAD,
            _ => return Err(Error::parse("unknown request type")),
        };
        if body.len() < expected {
            return Err(Error::parse("request payload truncated"));
        }

        let tenant = TenantId::from_bytes(body[0..16].try_into().unwrap());
        let timeline = TimelineId::from_bytes(body[16..32].try_into().unwrap());
        let rel = RelTag {
            spc_node: get_u32(body, 32),
            db_node: get_u32(body, 36),
            rel_node: get_u32(body, 40),
            fork,
        };

        match type_tag {
            TYPE_GET_PAGE => {
                let block = get_u32(body, 44);
                let lsn = Lsn(get_u64(body, 48));
                Ok(Request::GetPage { tenant, timeline, rel, block, lsn })
            }
            TYPE_GET_REL_SIZE => {
                let lsn = Lsn(get_u64(body, 44));
                Ok(Request::GetRelSize { tenant, timeline, rel, lsn })
            }
            _ => unreachable!(),
        }
    }
}

impl Response {
    /// Encode this response, including its length-prefixed header.
    pub fn encode(&self) -> Vec<u8> {
        let (status, payload): (u8, Vec<u8>) = match self {
            Response::Page(page) => (STATUS_OK, page.clone()),
            Response::RelSize(n) => (STATUS_OK, n.to_be_bytes().to_vec()),
            Response::NotFound => (STATUS_NOT_FOUND, Vec::new()),
            Response::Error(msg) => (STATUS_ERROR, msg.as_bytes().to_vec()),
        };

        let mut buf = Vec::with_capacity(HEADER_LEN + payload.len());
        put_u32(&mut buf, MAGIC);
        buf.push(VERSION);
        buf.push(status);
        buf.push(0); // reserved
        buf.push(0); // reserved
                     // NOTE: header is 8 bytes total; the 4-byte length lives at offset 4..8?
                     // We keep magic(4)+version(1)+status(1)+rsvd(2) = 8, then a 4-byte length.
        put_u32(&mut buf, payload.len() as u32);
        buf.extend_from_slice(&payload);
        buf
    }

    /// Decode a response from a complete message buffer (header + payload).
    pub fn decode(buf: &[u8]) -> Result<Response> {
        // Layout: magic(4) version(1) status(1) rsvd(2) len(4) payload.
        const RESP_HEADER: usize = HEADER_LEN + 4;
        if buf.len() < RESP_HEADER {
            return Err(Error::parse("response shorter than header"));
        }
        if get_u32(buf, 0) != MAGIC {
            return Err(Error::parse("bad protocol magic"));
        }
        if buf[4] != VERSION {
            return Err(Error::parse("unsupported protocol version"));
        }
        let status = buf[5];
        let len = get_u32(buf, 8) as usize;
        if buf.len() < RESP_HEADER + len {
            return Err(Error::parse("response payload truncated"));
        }
        let payload = &buf[RESP_HEADER..RESP_HEADER + len];

        match status {
            STATUS_OK => {
                if len == PAGE_SIZE {
                    Ok(Response::Page(payload.to_vec()))
                } else if len == 4 {
                    Ok(Response::RelSize(get_u32(payload, 0)))
                } else {
                    Err(Error::parse("OK response with unexpected payload length"))
                }
            }
            STATUS_NOT_FOUND => Ok(Response::NotFound),
            STATUS_ERROR => {
                let msg = String::from_utf8_lossy(payload).into_owned();
                Ok(Response::Error(msg))
            }
            _ => Err(Error::parse("unknown response status")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel() -> RelTag {
        RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main }
    }

    #[test]
    fn get_page_request_round_trips() {
        let req = Request::GetPage {
            tenant: TenantId::from_bytes([1; 16]),
            timeline: TimelineId::from_bytes([2; 16]),
            rel: rel(),
            block: 42,
            lsn: Lsn(0x16_B374_D848),
        };
        let bytes = req.encode();
        // header(8) + tenant(16) + timeline(16) + spc/db/rel(12) + block(4) + lsn(8)
        assert_eq!(bytes.len(), 8 + GET_PAGE_PAYLOAD);
        assert_eq!(Request::decode(&bytes).unwrap(), req);
    }

    #[test]
    fn get_rel_size_request_round_trips() {
        let req = Request::GetRelSize {
            tenant: TenantId::from_bytes([7; 16]),
            timeline: TimelineId::from_bytes([8; 16]),
            rel: rel(),
            lsn: Lsn::INVALID,
        };
        let bytes = req.encode();
        assert_eq!(Request::decode(&bytes).unwrap(), req);
    }

    #[test]
    fn page_response_round_trips() {
        let page = vec![0xABu8; PAGE_SIZE];
        let bytes = Response::Page(page.clone()).encode();
        match Response::decode(&bytes).unwrap() {
            Response::Page(p) => assert_eq!(p, page),
            other => panic!("expected Page, got {other:?}"),
        }
    }

    #[test]
    fn relsize_notfound_and_error_round_trip() {
        assert_eq!(
            Response::decode(&Response::RelSize(99).encode()).unwrap(),
            Response::RelSize(99)
        );
        assert_eq!(Response::decode(&Response::NotFound.encode()).unwrap(), Response::NotFound);
        match Response::decode(&Response::Error("boom".into()).encode()).unwrap() {
            Response::Error(m) => assert_eq!(m, "boom"),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_magic_and_truncation() {
        let mut bytes = Request::GetRelSize {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            rel: rel(),
            lsn: Lsn::INVALID,
        }
        .encode();
        bytes[0] ^= 0xFF; // corrupt magic
        assert!(Request::decode(&bytes).is_err());
        assert!(Request::decode(&[0u8; 3]).is_err()); // too short
    }
}
