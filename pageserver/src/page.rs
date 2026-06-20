// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Page versions and the ingest record.
//!
//! The page server stores the history of every page as a sequence of versions,
//! each tagged with the LSN at which it took effect. A version is either a full
//! 8 KiB **image** (a base) or a **delta** — a set of byte edits patched onto
//! the previous version. This mirrors how a WAL decoder turns PostgreSQL WAL
//! into per-page records: a full-page-image record becomes an [`PageVersion::Image`],
//! an ordinary heap/index change becomes a [`PageVersion::Delta`].
//!
//! A [`Modification`] is one such record addressed to a specific page — the
//! unit the repository ingests. Its wire codec is what a WAL decoder (or, in
//! tests, a client) sends to the ingest endpoint.

use common::{ForkNumber, Lsn, RelTag, PAGE_SIZE};
use thiserror::Error;

/// A single contiguous byte edit within a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteEdit {
    /// Offset within the 8 KiB page.
    pub offset: u16,
    /// Replacement bytes.
    pub data: Vec<u8>,
}

/// One version of a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageVersion {
    /// A complete 8 KiB page image (a reconstruction base point).
    Image(Vec<u8>),
    /// A set of byte edits applied on top of the previous version.
    Delta(Vec<ByteEdit>),
}

/// Errors from decoding ingest records or applying deltas.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PageError {
    #[error("buffer truncated while decoding {0}")]
    Truncated(&'static str),
    #[error("image is {0} bytes, expected {PAGE_SIZE}")]
    BadImageSize(usize),
    #[error("edit at offset {offset} length {len} runs past the end of the page")]
    EditOutOfBounds { offset: usize, len: usize },
    #[error("unknown version tag {0}")]
    BadVersionTag(u8),
}

impl PageVersion {
    /// Apply this version on top of `page` in place.
    pub fn apply_to(&self, page: &mut [u8]) -> Result<(), PageError> {
        match self {
            PageVersion::Image(img) => {
                if img.len() != PAGE_SIZE {
                    return Err(PageError::BadImageSize(img.len()));
                }
                page.copy_from_slice(img);
                Ok(())
            }
            PageVersion::Delta(edits) => {
                for e in edits {
                    let start = e.offset as usize;
                    let end = start + e.data.len();
                    if end > PAGE_SIZE {
                        return Err(PageError::EditOutOfBounds { offset: start, len: e.data.len() });
                    }
                    page[start..end].copy_from_slice(&e.data);
                }
                Ok(())
            }
        }
    }

    /// Whether this version is a full image (a valid reconstruction base).
    pub fn is_image(&self) -> bool {
        matches!(self, PageVersion::Image(_))
    }
}

/// One page modification to ingest: which page, at which LSN, and the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Modification {
    /// The relation/fork this page belongs to.
    pub rel: RelTag,
    /// Block number within the fork.
    pub block: u32,
    /// LSN at which this modification takes effect.
    pub lsn: Lsn,
    /// The new version (image or delta).
    pub version: PageVersion,
}

const TAG_IMAGE: u8 = 0;
const TAG_DELTA: u8 = 1;

/// Append a `PageVersion` to `buf` (shared by ingest records and layer files).
pub(crate) fn write_version(buf: &mut Vec<u8>, v: &PageVersion) {
    match v {
        PageVersion::Image(img) => {
            buf.push(TAG_IMAGE);
            buf.extend_from_slice(img);
        }
        PageVersion::Delta(edits) => {
            buf.push(TAG_DELTA);
            buf.extend_from_slice(&(edits.len() as u16).to_be_bytes());
            for e in edits {
                buf.extend_from_slice(&e.offset.to_be_bytes());
                buf.extend_from_slice(&(e.data.len() as u16).to_be_bytes());
                buf.extend_from_slice(&e.data);
            }
        }
    }
}

/// Read a `PageVersion` from `buf` at `pos`, returning the new position.
pub(crate) fn read_version(buf: &[u8], mut pos: usize) -> Result<(PageVersion, usize), PageError> {
    let tag = *buf.get(pos).ok_or(PageError::Truncated("version tag"))?;
    pos += 1;
    match tag {
        TAG_IMAGE => {
            if buf.len() < pos + PAGE_SIZE {
                return Err(PageError::Truncated("image"));
            }
            let img = buf[pos..pos + PAGE_SIZE].to_vec();
            Ok((PageVersion::Image(img), pos + PAGE_SIZE))
        }
        TAG_DELTA => {
            let count = read_u16(buf, &mut pos)? as usize;
            let mut edits = Vec::with_capacity(count);
            for _ in 0..count {
                let offset = read_u16(buf, &mut pos)?;
                let len = read_u16(buf, &mut pos)? as usize;
                if buf.len() < pos + len {
                    return Err(PageError::Truncated("edit data"));
                }
                edits.push(ByteEdit { offset, data: buf[pos..pos + len].to_vec() });
                pos += len;
            }
            Ok((PageVersion::Delta(edits), pos))
        }
        other => Err(PageError::BadVersionTag(other)),
    }
}

fn read_u16(buf: &[u8], pos: &mut usize) -> Result<u16, PageError> {
    if buf.len() < *pos + 2 {
        return Err(PageError::Truncated("u16"));
    }
    let v = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32, PageError> {
    if buf.len() < *pos + 4 {
        return Err(PageError::Truncated("u32"));
    }
    let v = u32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_u64(buf: &[u8], pos: &mut usize) -> Result<u64, PageError> {
    if buf.len() < *pos + 8 {
        return Err(PageError::Truncated("u64"));
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[*pos..*pos + 8]);
    *pos += 8;
    Ok(u64::from_be_bytes(a))
}

impl Modification {
    /// Encode the modification to its ingest-wire body (no length prefix).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32 + PAGE_SIZE);
        buf.extend_from_slice(&self.rel.spc_node.to_be_bytes());
        buf.extend_from_slice(&self.rel.db_node.to_be_bytes());
        buf.extend_from_slice(&self.rel.rel_node.to_be_bytes());
        buf.push(self.rel.fork as u8);
        buf.extend_from_slice(&self.block.to_be_bytes());
        buf.extend_from_slice(&self.lsn.raw().to_be_bytes());
        write_version(&mut buf, &self.version);
        buf
    }

    /// Decode a modification from its ingest-wire body.
    pub fn decode(buf: &[u8]) -> Result<Modification, PageError> {
        let mut pos = 0;
        let spc_node = read_u32(buf, &mut pos)?;
        let db_node = read_u32(buf, &mut pos)?;
        let rel_node = read_u32(buf, &mut pos)?;
        let fork_raw = *buf.get(pos).ok_or(PageError::Truncated("fork"))?;
        pos += 1;
        let fork = ForkNumber::from_raw(fork_raw).ok_or(PageError::Truncated("fork"))?;
        let block = read_u32(buf, &mut pos)?;
        let lsn = Lsn(read_u64(buf, &mut pos)?);
        let (version, _) = read_version(buf, pos)?;
        Ok(Modification { rel: RelTag { spc_node, db_node, rel_node, fork }, block, lsn, version })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel() -> RelTag {
        RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main }
    }

    #[test]
    fn delta_applies_byte_edits() {
        let mut page = vec![0u8; PAGE_SIZE];
        let d = PageVersion::Delta(vec![
            ByteEdit { offset: 0, data: vec![1, 2, 3] },
            ByteEdit { offset: 100, data: vec![9, 9] },
        ]);
        d.apply_to(&mut page).unwrap();
        assert_eq!(&page[0..3], &[1, 2, 3]);
        assert_eq!(&page[100..102], &[9, 9]);
    }

    #[test]
    fn out_of_bounds_edit_is_rejected() {
        let mut page = vec![0u8; PAGE_SIZE];
        let d = PageVersion::Delta(vec![ByteEdit { offset: (PAGE_SIZE - 1) as u16, data: vec![1, 2] }]);
        assert!(matches!(d.apply_to(&mut page), Err(PageError::EditOutOfBounds { .. })));
    }

    #[test]
    fn modification_image_round_trips() {
        let m = Modification {
            rel: rel(),
            block: 7,
            lsn: Lsn(0x1234),
            version: PageVersion::Image(vec![0xCD; PAGE_SIZE]),
        };
        assert_eq!(Modification::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn modification_delta_round_trips() {
        let m = Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(99),
            version: PageVersion::Delta(vec![ByteEdit { offset: 42, data: vec![7, 7, 7] }]),
        };
        assert_eq!(Modification::decode(&m.encode()).unwrap(), m);
    }
}
