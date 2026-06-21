// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! PostgreSQL WAL decoder (Phase 1 of the WAL decode/redo subsystem).
//!
//! See `docs/design/wal-redo.md` for the full picture. This module turns a raw
//! PostgreSQL **WAL byte stream** into per-page change records, which the page
//! server indexes and later replays to reconstruct any page at any LSN.
//!
//! It implements two things, both pure (no I/O, no Postgres):
//!
//! * [`WalStreamDecoder`] — frames the stream: WAL is a sequence of 8 KiB pages,
//!   each beginning with an `XLogPageHeader`; records are `MAXALIGN`ed and may
//!   span page boundaries (continuation). Feed it bytes, poll out whole
//!   `XLogRecord` images with their start LSN.
//! * [`decode_wal_record`] — parses one record's **generic** layer: the list of
//!   registered block references `(RelFileLocator, fork, block)`, each with an
//!   optional full-page image (FPI) and per-block data, plus the rmgr id and
//!   main data. This layer is rmgr-agnostic — it identifies *which* pages a
//!   record touches without understanding heap/btree/etc. internals, which is
//!   exactly what the page server needs to route changes to pages.
//!
//! ## Endianness
//! PostgreSQL writes WAL in host byte order and the on-disk format is not
//! portable across architectures; production deployments are little-endian
//! (x86-64, aarch64), so this decoder reads little-endian. The constants and
//! struct layouts mirror PostgreSQL 16's `access/xlogrecord.h` and
//! `access/xlog_internal.h`.

use common::{ForkNumber, Lsn, RelTag, PAGE_SIZE};
use thiserror::Error;

/// WAL block size. Equal to the heap page size (`PAGE_SIZE`) in a default build.
pub const XLOG_BLCKSZ: usize = PAGE_SIZE;
/// `sizeof(XLogRecord)` — the fixed record header.
pub const SIZE_OF_XLOG_RECORD: usize = 24;
/// `sizeof(XLogPageHeaderData)` — the per-page short header (MAXALIGNed).
pub const SIZE_OF_XLOG_SHORT_PHD: usize = 24;
/// `sizeof(XLogLongPageHeaderData)` — the header on the first page of a segment.
pub const SIZE_OF_XLOG_LONG_PHD: usize = 40;
/// Default WAL segment size (16 MiB); only affects where long headers appear.
pub const DEFAULT_WAL_SEG_SIZE: u64 = 16 * 1024 * 1024;

/// `XLOG_PAGE_MAGIC` for PostgreSQL 16. Used to sanity-check page headers.
pub const XLOG_PAGE_MAGIC_PG16: u16 = 0xD113;

// XLogPageHeaderData.xlp_info flags.
/// Set on a page whose first record is a continuation of the previous page's.
/// Referenced by the stream framing tests; kept for documentation of the format.
#[cfg_attr(not(test), allow(dead_code))]
const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
const XLP_LONG_HEADER: u16 = 0x0002;

// Special block-id sentinels in a record body.
const XLR_MAX_BLOCK_ID: u8 = 32;
const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

// XLogRecordBlockHeader.fork_flags bits.
const BKPBLOCK_FORK_MASK: u8 = 0x0F;
const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_WILL_INIT: u8 = 0x40;
const BKPBLOCK_SAME_REL: u8 = 0x80;

// XLogRecordBlockImageHeader.bimg_info bits.
const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_APPLY: u8 = 0x02;
const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;
const BKPIMAGE_COMPRESS_LZ4: u8 = 0x08;
const BKPIMAGE_COMPRESS_ZSTD: u8 = 0x10;
const BKPIMAGE_COMPRESSED: u8 =
    BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD;

/// Errors produced while framing or decoding WAL.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WalDecodeError {
    /// The record body ended before a field could be read.
    #[error("WAL record truncated while reading {0}")]
    Truncated(&'static str),
    /// `xl_tot_len` disagrees with the buffer actually handed in.
    #[error("xl_tot_len {declared} != record length {actual}")]
    BadTotalLen { declared: u32, actual: usize },
    /// `xl_tot_len` is smaller than the fixed header — corrupt record.
    #[error("xl_tot_len {0} is shorter than the record header")]
    ShortTotalLen(u32),
    /// A block id outside the legal range appeared in the body.
    #[error("illegal block id {0} in WAL record")]
    BadBlockId(u8),
    /// A `SAME_REL` block appeared with no preceding relation to inherit.
    #[error("BKPBLOCK_SAME_REL with no previous relation")]
    SameRelWithoutPrev,
    /// The fork number in a block header is not one we model.
    #[error("unknown fork number {0}")]
    BadFork(u8),
    /// A page header carried the wrong magic for the expected PG version.
    #[error("bad XLOG page magic {got:#06x}, expected {want:#06x}")]
    BadPageMagic { got: u16, want: u16 },
    /// A stored full-page image had an unexpected size.
    #[error("full-page image is {got} bytes, expected {want}")]
    BadImageSize { got: usize, want: usize },
    /// The image is compressed with a method this build can't yet decompress.
    #[error("compressed full-page image ({0:?}) is not yet supported")]
    UnsupportedCompression(Compression),
    /// The hole described by an image runs past the page.
    #[error("image hole offset {offset}+{length} exceeds page size")]
    BadHole { offset: usize, length: usize },
}

/// Compression applied to a stored full-page image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Stored verbatim (after any hole removal).
    None,
    /// PostgreSQL's built-in LZ (`pglz`).
    Pglz,
    /// LZ4.
    Lz4,
    /// Zstandard.
    Zstd,
}

/// A full-page image carried by a WAL record, as stored on the wire.
///
/// The stored `bytes` have had the page's all-zero "hole" removed and may be
/// compressed; [`restore`](DecodedImage::restore) reconstructs the full 8 KiB
/// page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedImage {
    /// Stored image bytes (post hole-removal / compression).
    pub bytes: Vec<u8>,
    /// Offset of the removed hole within the full page.
    pub hole_offset: u16,
    /// Length of the removed hole (0 if the page had no hole).
    pub hole_length: u16,
    /// Compression method applied to `bytes`.
    pub compression: Compression,
    /// `BKPIMAGE_APPLY`: redo should install this image even when the page
    /// already exists (i.e. it is authoritative, not just a torn-page guard).
    pub apply: bool,
}

impl DecodedImage {
    /// Reconstruct the full 8 KiB page from the stored bytes.
    ///
    /// Re-inserts the zero hole; decompresses if needed. Compressed images
    /// return [`WalDecodeError::UnsupportedCompression`] until the matching
    /// decompressor is wired in (Phase 2) — we never return a wrong page.
    pub fn restore(&self) -> Result<Vec<u8>, WalDecodeError> {
        if self.compression != Compression::None {
            return Err(WalDecodeError::UnsupportedCompression(self.compression));
        }
        let mut page = vec![0u8; XLOG_BLCKSZ];
        let hl = self.hole_length as usize;
        if hl == 0 {
            if self.bytes.len() != XLOG_BLCKSZ {
                return Err(WalDecodeError::BadImageSize {
                    got: self.bytes.len(),
                    want: XLOG_BLCKSZ,
                });
            }
            page.copy_from_slice(&self.bytes);
        } else {
            let ho = self.hole_offset as usize;
            if ho + hl > XLOG_BLCKSZ {
                return Err(WalDecodeError::BadHole { offset: ho, length: hl });
            }
            let want = XLOG_BLCKSZ - hl;
            if self.bytes.len() != want {
                return Err(WalDecodeError::BadImageSize { got: self.bytes.len(), want });
            }
            // bytes = [before hole][after hole]; the hole region stays zero.
            page[..ho].copy_from_slice(&self.bytes[..ho]);
            page[ho + hl..].copy_from_slice(&self.bytes[ho..]);
        }
        Ok(page)
    }
}

/// One registered block reference within a WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBlock {
    /// The relation and fork this block belongs to.
    pub rel: RelTag,
    /// Block number within the fork.
    pub blkno: u32,
    /// `BKPBLOCK_WILL_INIT`: redo re-initializes the page, so no base image is
    /// needed and prior history for this page can be discarded.
    pub will_init: bool,
    /// The full-page image attached to this block, if any.
    pub image: Option<DecodedImage>,
    /// The rmgr-specific per-block data (empty if the block had none).
    pub data: Vec<u8>,
}

/// A fully decoded WAL record: its identity plus every page it touches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedWalRecord {
    /// LSN of the first byte of this record.
    pub lsn: Lsn,
    /// Resource-manager id (`xl_rmid`) — heap, btree, xlog, …
    pub rmid: u8,
    /// Record-specific info bits (`xl_info`).
    pub info: u8,
    /// Transaction id that wrote the record (`xl_xid`).
    pub xid: u32,
    /// The rmgr "main data" payload.
    pub main_data: Vec<u8>,
    /// Every block this record references, in block-id order.
    pub blocks: Vec<DecodedBlock>,
}

fn compression_from_info(bimg_info: u8) -> Compression {
    if bimg_info & BKPIMAGE_COMPRESS_PGLZ != 0 {
        Compression::Pglz
    } else if bimg_info & BKPIMAGE_COMPRESS_LZ4 != 0 {
        Compression::Lz4
    } else if bimg_info & BKPIMAGE_COMPRESS_ZSTD != 0 {
        Compression::Zstd
    } else {
        Compression::None
    }
}

/// A little-endian cursor over a byte slice with bounds-checked reads.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    fn u8(&mut self, what: &'static str) -> Result<u8, WalDecodeError> {
        let b = *self.buf.get(self.pos).ok_or(WalDecodeError::Truncated(what))?;
        self.pos += 1;
        Ok(b)
    }
    fn u16(&mut self, what: &'static str) -> Result<u16, WalDecodeError> {
        let s = self.take(2, what)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u32(&mut self, what: &'static str) -> Result<u32, WalDecodeError> {
        let s = self.take(4, what)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self, what: &'static str) -> Result<u64, WalDecodeError> {
        let s = self.take(8, what)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(s);
        Ok(u64::from_le_bytes(a))
    }
    fn take(&mut self, n: usize, what: &'static str) -> Result<&'a [u8], WalDecodeError> {
        if self.remaining() < n {
            return Err(WalDecodeError::Truncated(what));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn skip(&mut self, n: usize, what: &'static str) -> Result<(), WalDecodeError> {
        self.take(n, what).map(|_| ())
    }
}

/// Per-block work captured during the header pass, resolved during the data pass.
struct BlockWork {
    rel: RelTag,
    blkno: u32,
    will_init: bool,
    has_data: bool,
    data_len: usize,
    image_meta: Option<ImageMeta>,
}

struct ImageMeta {
    bimg_len: usize,
    hole_offset: u16,
    hole_length: u16,
    compression: Compression,
    apply: bool,
}

/// Decode a single complete WAL record image (the `xl_tot_len` bytes that
/// [`WalStreamDecoder::poll_decode`] yields) into its block references.
///
/// `lsn` is the record's start LSN, recorded on the result for indexing.
pub fn decode_wal_record(lsn: Lsn, record: &[u8]) -> Result<DecodedWalRecord, WalDecodeError> {
    let mut c = Cursor::new(record);

    // --- Fixed XLogRecord header (24 bytes). ---
    let xl_tot_len = c.u32("xl_tot_len")?;
    let xl_xid = c.u32("xl_xid")?;
    let _xl_prev = c.u64("xl_prev")?;
    let xl_info = c.u8("xl_info")?;
    let xl_rmid = c.u8("xl_rmid")?;
    c.skip(2, "xl_padding")?;
    let _xl_crc = c.u32("xl_crc")?;

    if (xl_tot_len as usize) < SIZE_OF_XLOG_RECORD {
        return Err(WalDecodeError::ShortTotalLen(xl_tot_len));
    }
    if xl_tot_len as usize != record.len() {
        return Err(WalDecodeError::BadTotalLen { declared: xl_tot_len, actual: record.len() });
    }

    // --- Header section: registered blocks + the main-data length marker. ---
    let mut blocks: Vec<BlockWork> = Vec::new();
    // Set when the loop reaches the (always-last) main-data length marker.
    let main_data_len: usize;
    let mut last_locator: Option<(u32, u32, u32)> = None;

    loop {
        let block_id = c.u8("block_id")?;
        match block_id {
            // The main-data length marker is always registered last; it ends
            // the header section.
            XLR_BLOCK_ID_DATA_SHORT => {
                main_data_len = c.u8("main_data_len")? as usize;
                break;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                main_data_len = c.u32("main_data_len")? as usize;
                break;
            }
            XLR_BLOCK_ID_ORIGIN => {
                c.skip(2, "replication origin")?; // RepOriginId
            }
            XLR_BLOCK_ID_TOPLEVEL_XID => {
                c.skip(8, "toplevel xid")?; // TransactionId (8 bytes here)
            }
            id if id <= XLR_MAX_BLOCK_ID => {
                let fork_flags = c.u8("fork_flags")?;
                let data_len = c.u16("block data_len")? as usize;

                let fork_raw = fork_flags & BKPBLOCK_FORK_MASK;
                let has_image = fork_flags & BKPBLOCK_HAS_IMAGE != 0;
                let has_data = fork_flags & BKPBLOCK_HAS_DATA != 0;
                let will_init = fork_flags & BKPBLOCK_WILL_INIT != 0;
                let same_rel = fork_flags & BKPBLOCK_SAME_REL != 0;

                let image_meta = if has_image {
                    let bimg_len = c.u16("bimg_len")? as usize;
                    let hole_offset = c.u16("hole_offset")?;
                    let bimg_info = c.u8("bimg_info")?;
                    let has_hole = bimg_info & BKPIMAGE_HAS_HOLE != 0;
                    let compression = compression_from_info(bimg_info);
                    let apply = bimg_info & BKPIMAGE_APPLY != 0;

                    let hole_length = if has_hole && (bimg_info & BKPIMAGE_COMPRESSED != 0) {
                        // XLogRecordBlockCompressHeader carries the hole length
                        // explicitly when the image is both holed and compressed.
                        c.u16("hole_length")?
                    } else if has_hole {
                        (XLOG_BLCKSZ - bimg_len) as u16
                    } else {
                        0
                    };
                    Some(ImageMeta { bimg_len, hole_offset, hole_length, compression, apply })
                } else {
                    None
                };

                let (spc, db, relnode) = if same_rel {
                    last_locator.ok_or(WalDecodeError::SameRelWithoutPrev)?
                } else {
                    let spc = c.u32("spcOid")?;
                    let db = c.u32("dbOid")?;
                    let relnode = c.u32("relNumber")?;
                    last_locator = Some((spc, db, relnode));
                    (spc, db, relnode)
                };
                let blkno = c.u32("blkno")?;

                let fork =
                    ForkNumber::from_raw(fork_raw).ok_or(WalDecodeError::BadFork(fork_raw))?;
                blocks.push(BlockWork {
                    rel: RelTag { spc_node: spc, db_node: db, rel_node: relnode, fork },
                    blkno,
                    will_init,
                    has_data,
                    data_len,
                    image_meta,
                });
            }
            other => return Err(WalDecodeError::BadBlockId(other)),
        }
    }

    // --- Data section: for each block, image then per-block data; then main. ---
    let mut decoded_blocks = Vec::with_capacity(blocks.len());
    for b in blocks {
        let image = match b.image_meta {
            Some(meta) => {
                let bytes = c.take(meta.bimg_len, "block image")?.to_vec();
                Some(DecodedImage {
                    bytes,
                    hole_offset: meta.hole_offset,
                    hole_length: meta.hole_length,
                    compression: meta.compression,
                    apply: meta.apply,
                })
            }
            None => None,
        };
        let data = if b.has_data { c.take(b.data_len, "block data")?.to_vec() } else { Vec::new() };
        decoded_blocks.push(DecodedBlock {
            rel: b.rel,
            blkno: b.blkno,
            will_init: b.will_init,
            image,
            data,
        });
    }
    let main_data = c.take(main_data_len, "main data")?.to_vec();

    Ok(DecodedWalRecord {
        lsn,
        rmid: xl_rmid,
        info: xl_info,
        xid: xl_xid,
        main_data,
        blocks: decoded_blocks,
    })
}

/// Frames a raw WAL byte stream into whole `XLogRecord` images.
///
/// WAL is a sequence of [`XLOG_BLCKSZ`]-byte pages, each prefixed by an
/// `XLogPageHeader`; records are `MAXALIGN`ed and may continue across page
/// boundaries. Feed bytes with [`feed_bytes`](Self::feed_bytes) and pull
/// complete records with [`poll_decode`](Self::poll_decode); the decoder keeps
/// the absolute LSN so it knows where page (and segment) headers fall.
pub struct WalStreamDecoder {
    /// Absolute LSN of `buf[0]` — the next unconsumed byte in the stream.
    lsn: u64,
    /// WAL segment size; long page headers appear at each segment boundary.
    wal_seg_size: u64,
    /// Whether to validate the page-header magic against PG 16.
    check_magic: bool,
    /// Unconsumed stream bytes; `buf[0]` sits at absolute LSN `lsn`.
    buf: Vec<u8>,
}

impl WalStreamDecoder {
    /// Start decoding at `start_lsn`, which must be a record boundary (the WAL
    /// position the page server last ingested up to).
    pub fn new(start_lsn: Lsn) -> Self {
        WalStreamDecoder {
            lsn: start_lsn.raw(),
            wal_seg_size: DEFAULT_WAL_SEG_SIZE,
            check_magic: true,
            buf: Vec::new(),
        }
    }

    /// Override the WAL segment size (default 16 MiB).
    pub fn with_segment_size(mut self, seg_size: u64) -> Self {
        self.wal_seg_size = seg_size;
        self
    }

    /// Disable page-magic validation (useful for synthetic fixtures).
    pub fn without_magic_check(mut self) -> Self {
        self.check_magic = false;
        self
    }

    /// The LSN the decoder is currently positioned at.
    pub fn lsn(&self) -> Lsn {
        Lsn(self.lsn)
    }

    /// Append more raw stream bytes.
    pub fn feed_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Size of the page header at absolute position `lsn`.
    fn page_header_size(&self, lsn: u64) -> usize {
        if lsn % self.wal_seg_size == 0 {
            SIZE_OF_XLOG_LONG_PHD
        } else {
            SIZE_OF_XLOG_SHORT_PHD
        }
    }

    /// Validate the page header sitting at `buf[off]` (absolute LSN `lsn`).
    fn check_page_header(&self, off: usize, lsn: u64) -> Result<(), WalDecodeError> {
        if !self.check_magic {
            return Ok(());
        }
        let magic = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]);
        if magic != XLOG_PAGE_MAGIC_PG16 {
            return Err(WalDecodeError::BadPageMagic { got: magic, want: XLOG_PAGE_MAGIC_PG16 });
        }
        let info = u16::from_le_bytes([self.buf[off + 2], self.buf[off + 3]]);
        // A long header must appear exactly at segment boundaries.
        let expect_long = lsn % self.wal_seg_size == 0;
        if expect_long != (info & XLP_LONG_HEADER != 0) {
            // Tolerate but don't fail hard; size is derived from position.
        }
        Ok(())
    }

    /// Collect `n` logical payload bytes starting at the current position,
    /// transparently skipping page headers at page boundaries.
    ///
    /// Returns the payload, the number of raw `buf` bytes it spans, and the LSN
    /// of the first payload byte; `Ok(None)` if not enough has been fed yet.
    fn collect_payload(&self, n: usize) -> Result<Option<(Vec<u8>, usize, u64)>, WalDecodeError> {
        let mut out = Vec::with_capacity(n);
        let mut off = 0usize;
        let mut lsn = self.lsn;
        let mut first_payload_lsn: Option<u64> = None;

        while out.len() < n {
            if lsn % XLOG_BLCKSZ as u64 == 0 {
                let h = self.page_header_size(lsn);
                if off + h > self.buf.len() {
                    return Ok(None);
                }
                self.check_page_header(off, lsn)?;
                off += h;
                lsn += h as u64;
                continue;
            }
            if first_payload_lsn.is_none() {
                first_payload_lsn = Some(lsn);
            }
            let page_remaining = XLOG_BLCKSZ - (lsn % XLOG_BLCKSZ as u64) as usize;
            let want = (n - out.len()).min(page_remaining);
            if off + want > self.buf.len() {
                return Ok(None);
            }
            out.extend_from_slice(&self.buf[off..off + want]);
            off += want;
            lsn += want as u64;
        }
        // n == 0 edge: anchor the payload LSN at the (post-header) position.
        let start = first_payload_lsn.unwrap_or(lsn);
        Ok(Some((out, off, start)))
    }

    /// Drop `raw` consumed bytes from the front of `buf` and advance `lsn`.
    fn consume(&mut self, raw: usize) {
        self.buf.drain(0..raw);
        self.lsn += raw as u64;
    }

    /// Try to decode the next complete record. Returns `Ok(None)` when more
    /// bytes are needed.
    pub fn poll_decode(&mut self) -> Result<Option<(Lsn, Vec<u8>)>, WalDecodeError> {
        loop {
            // Records are MAXALIGNed (8 bytes). If the previous record left us
            // mid-alignment, skip the padding now (it never crosses a page
            // boundary). Doing it here — rather than eagerly after emitting —
            // means the final record needs no trailing pad to be returned.
            let misalign = (self.lsn % 8) as usize;
            if misalign != 0 {
                let pad = 8 - misalign;
                if pad > self.buf.len() {
                    return Ok(None);
                }
                self.consume(pad);
            }

            // Read xl_tot_len (first 4 bytes of the record header). This also
            // skips any page header sitting at the current boundary.
            let (len_bytes, _, rec_lsn) = match self.collect_payload(4)? {
                Some(v) => v,
                None => return Ok(None),
            };
            let xl_tot_len =
                u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);

            // A zero (or sub-header) length marks the end of valid WAL on this
            // page: the rest is zero padding. Skip to the next page boundary.
            if (xl_tot_len as usize) < SIZE_OF_XLOG_RECORD {
                let next_page = (rec_lsn / XLOG_BLCKSZ as u64 + 1) * XLOG_BLCKSZ as u64;
                let skip = (next_page - self.lsn) as usize;
                if skip > self.buf.len() {
                    return Ok(None);
                }
                self.consume(skip);
                continue;
            }

            // Pull the whole record image, then emit it. Alignment padding is
            // dealt with at the top of the next call.
            let (record, raw, rec_lsn) = match self.collect_payload(xl_tot_len as usize)? {
                Some(v) => v,
                None => return Ok(None),
            };
            self.consume(raw);
            return Ok(Some((Lsn(rec_lsn), record)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Builders for format-accurate synthetic WAL ----

    /// Build one XLogRecord image (header + body), filling a valid `xl_tot_len`.
    /// `body` is everything after the 24-byte header.
    fn make_record(rmid: u8, info: u8, xid: u32, body: &[u8]) -> Vec<u8> {
        let tot = SIZE_OF_XLOG_RECORD + body.len();
        let mut r = Vec::with_capacity(tot);
        r.extend_from_slice(&(tot as u32).to_le_bytes()); // xl_tot_len
        r.extend_from_slice(&xid.to_le_bytes()); // xl_xid
        r.extend_from_slice(&0u64.to_le_bytes()); // xl_prev
        r.push(info); // xl_info
        r.push(rmid); // xl_rmid
        r.extend_from_slice(&[0u8, 0u8]); // padding
        r.extend_from_slice(&0u32.to_le_bytes()); // xl_crc (unchecked here)
        r.extend_from_slice(body);
        r
    }

    /// A block header with no image and no data (just a rel + blkno reference).
    fn block_ref(
        block_id: u8,
        fork: u8,
        rel: (u32, u32, u32),
        blkno: u32,
        same_rel: bool,
    ) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(block_id);
        let mut fork_flags = fork & BKPBLOCK_FORK_MASK;
        if same_rel {
            fork_flags |= BKPBLOCK_SAME_REL;
        }
        b.push(fork_flags);
        b.extend_from_slice(&0u16.to_le_bytes()); // data_len = 0
        if !same_rel {
            b.extend_from_slice(&rel.0.to_le_bytes());
            b.extend_from_slice(&rel.1.to_le_bytes());
            b.extend_from_slice(&rel.2.to_le_bytes());
        }
        b.extend_from_slice(&blkno.to_le_bytes());
        b
    }

    /// The main-data length marker (short form) that terminates the headers.
    fn main_short(len: u8) -> Vec<u8> {
        vec![XLR_BLOCK_ID_DATA_SHORT, len]
    }

    /// The main-data length marker (long form) for payloads > 255 bytes.
    fn main_long(len: u32) -> Vec<u8> {
        let mut v = vec![XLR_BLOCK_ID_DATA_LONG];
        v.extend_from_slice(&len.to_le_bytes());
        v
    }

    #[test]
    fn decodes_single_block_reference() {
        let mut body = block_ref(0, ForkNumber::Main as u8, (1663, 5, 16384), 42, false);
        body.extend_from_slice(&main_short(3));
        body.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // main data
        let rec = make_record(10, 0x20, 777, &body);

        let d = decode_wal_record(Lsn(0x100), &rec).unwrap();
        assert_eq!(d.lsn, Lsn(0x100));
        assert_eq!(d.rmid, 10);
        assert_eq!(d.xid, 777);
        assert_eq!(d.main_data, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(d.blocks.len(), 1);
        let b = &d.blocks[0];
        assert_eq!(
            b.rel,
            RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main }
        );
        assert_eq!(b.blkno, 42);
        assert!(b.image.is_none());
        assert!(b.data.is_empty());
    }

    #[test]
    fn same_rel_inherits_previous_locator() {
        let mut body = block_ref(0, ForkNumber::Main as u8, (1663, 5, 16384), 1, false);
        // Second block reuses the relation, different fork + block.
        body.extend_from_slice(&block_ref(1, ForkNumber::Fsm as u8, (0, 0, 0), 2, true));
        body.extend_from_slice(&main_short(0));
        let rec = make_record(11, 0, 1, &body);

        let d = decode_wal_record(Lsn(7), &rec).unwrap();
        assert_eq!(d.blocks.len(), 2);
        assert_eq!(d.blocks[1].rel.rel_node, 16384); // inherited
        assert_eq!(d.blocks[1].rel.fork, ForkNumber::Fsm); // but its own fork
        assert_eq!(d.blocks[1].blkno, 2);
    }

    #[test]
    fn same_rel_without_previous_is_rejected() {
        let mut body = block_ref(0, ForkNumber::Main as u8, (0, 0, 0), 9, true);
        body.extend_from_slice(&main_short(0));
        let rec = make_record(1, 0, 0, &body);
        assert_eq!(decode_wal_record(Lsn(0), &rec), Err(WalDecodeError::SameRelWithoutPrev));
    }

    #[test]
    fn decodes_full_page_image_with_hole_and_restores_it() {
        // Construct a page: nonzero before and after a zero hole.
        let hole_offset = 100usize;
        let hole_length = 200usize;
        let stored_len = XLOG_BLCKSZ - hole_length;
        let mut stored = vec![0u8; stored_len];
        for (i, b) in stored.iter_mut().enumerate() {
            *b = (i % 251) as u8 + 1; // all nonzero so the hole is distinguishable
        }

        // Block header with image.
        let mut body = Vec::new();
        body.push(0u8); // block_id
        let fork_flags = (ForkNumber::Main as u8) | BKPBLOCK_HAS_IMAGE;
        body.push(fork_flags);
        body.extend_from_slice(&0u16.to_le_bytes()); // data_len = 0
                                                     // XLogRecordBlockImageHeader
        body.extend_from_slice(&(stored_len as u16).to_le_bytes()); // bimg_len
        body.extend_from_slice(&(hole_offset as u16).to_le_bytes()); // hole_offset
        body.push(BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY); // bimg_info, uncompressed
                                                       // RelFileLocator + blkno
        body.extend_from_slice(&1663u32.to_le_bytes());
        body.extend_from_slice(&5u32.to_le_bytes());
        body.extend_from_slice(&16384u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes()); // blkno
        body.extend_from_slice(&main_short(0));
        // data section: the stored image bytes, then (empty) main data
        body.extend_from_slice(&stored);

        let rec = make_record(0, 0, 0, &body);
        let d = decode_wal_record(Lsn(0x2000), &rec).unwrap();
        let img = d.blocks[0].image.as_ref().expect("image present");
        assert_eq!(img.hole_offset, hole_offset as u16);
        assert_eq!(img.hole_length, hole_length as u16);
        assert_eq!(img.compression, Compression::None);
        assert!(img.apply);

        let page = img.restore().unwrap();
        assert_eq!(page.len(), XLOG_BLCKSZ);
        // Hole is zero; surrounding bytes match the stored image.
        assert!(page[hole_offset..hole_offset + hole_length].iter().all(|&b| b == 0));
        assert_eq!(&page[..hole_offset], &stored[..hole_offset]);
        assert_eq!(&page[hole_offset + hole_length..], &stored[hole_offset..]);
    }

    #[test]
    fn compressed_image_restore_is_unsupported_not_wrong() {
        let img = DecodedImage {
            bytes: vec![1, 2, 3],
            hole_offset: 0,
            hole_length: 0,
            compression: Compression::Zstd,
            apply: false,
        };
        assert_eq!(img.restore(), Err(WalDecodeError::UnsupportedCompression(Compression::Zstd)));
    }

    #[test]
    fn rejects_bad_total_len() {
        let body = main_short(0);
        let mut rec = make_record(0, 0, 0, &body);
        // Corrupt xl_tot_len to claim more than the buffer holds.
        let bad = (rec.len() as u32 + 10).to_le_bytes();
        rec[0..4].copy_from_slice(&bad);
        assert!(matches!(decode_wal_record(Lsn(0), &rec), Err(WalDecodeError::BadTotalLen { .. })));
    }

    #[test]
    fn rejects_illegal_block_id() {
        // 100 is > XLR_MAX_BLOCK_ID and not a known sentinel.
        let body = vec![100u8];
        let rec = make_record(0, 0, 0, &body);
        assert_eq!(decode_wal_record(Lsn(0), &rec), Err(WalDecodeError::BadBlockId(100)));
    }

    // ---- Stream framing ----

    /// Build a short page header at `pageaddr`.
    fn short_page_header(pageaddr: u64, is_contrecord: bool, rem_len: u32) -> Vec<u8> {
        let mut h = Vec::with_capacity(SIZE_OF_XLOG_SHORT_PHD);
        h.extend_from_slice(&XLOG_PAGE_MAGIC_PG16.to_le_bytes()); // xlp_magic
        let info: u16 = if is_contrecord { XLP_FIRST_IS_CONTRECORD } else { 0 };
        h.extend_from_slice(&info.to_le_bytes()); // xlp_info
        h.extend_from_slice(&1u32.to_le_bytes()); // xlp_tli
        h.extend_from_slice(&pageaddr.to_le_bytes()); // xlp_pageaddr
        h.extend_from_slice(&rem_len.to_le_bytes()); // xlp_rem_len
        h.resize(SIZE_OF_XLOG_SHORT_PHD, 0); // pad to MAXALIGN
        h
    }

    #[test]
    fn stream_yields_two_records_within_a_page() {
        // Page starts at LSN 0 with a long header; put two records after it.
        let mut long_hdr = short_page_header(0, false, 0);
        long_hdr.resize(SIZE_OF_XLOG_LONG_PHD, 0); // promote to long header size
                                                   // Mark the long-header flag.
        let info = XLP_LONG_HEADER;
        long_hdr[2..4].copy_from_slice(&info.to_le_bytes());

        let mut body1 = block_ref(0, ForkNumber::Main as u8, (1, 2, 3), 10, false);
        body1.extend_from_slice(&main_short(0));
        let rec1 = make_record(5, 0, 1, &body1);

        let mut body2 = block_ref(0, ForkNumber::Main as u8, (1, 2, 3), 20, false);
        body2.extend_from_slice(&main_short(0));
        let rec2 = make_record(6, 0, 2, &body2);

        let mut stream = long_hdr.clone();
        stream.extend_from_slice(&rec1);
        // MAXALIGN pad after rec1.
        let pad1 = (8 - (rec1.len() % 8)) % 8;
        stream.extend(std::iter::repeat(0u8).take(pad1));
        let rec2_lsn = stream.len() as u64;
        stream.extend_from_slice(&rec2);

        let mut dec = WalStreamDecoder::new(Lsn(0));
        dec.feed_bytes(&stream);

        let (lsn1, got1) = dec.poll_decode().unwrap().expect("record 1");
        assert_eq!(lsn1, Lsn(SIZE_OF_XLOG_LONG_PHD as u64));
        assert_eq!(got1, rec1);

        let (lsn2, got2) = dec.poll_decode().unwrap().expect("record 2");
        assert_eq!(lsn2, Lsn(rec2_lsn));
        assert_eq!(got2, rec2);

        // Decoded content checks out too.
        let d2 = decode_wal_record(lsn2, &got2).unwrap();
        assert_eq!(d2.blocks[0].blkno, 20);

        assert!(dec.poll_decode().unwrap().is_none());
    }

    #[test]
    fn stream_stitches_a_record_across_a_page_boundary() {
        // Lay out a record that straddles the boundary between page 0 and 1.
        // Page 0: long header + first half of the record.
        // Page 1: short (contrecord) header + second half.
        let mut body = block_ref(0, ForkNumber::Main as u8, (9, 9, 9), 99, false);
        // Pad the record out with enough main data (> one page) so it is forced
        // to span the page boundary. Needs the long marker (len > 255).
        let main_len: u32 = 9000;
        body.extend_from_slice(&main_long(main_len));
        body.extend(std::iter::repeat(0xEEu8).take(main_len as usize));
        let rec = make_record(7, 0, 3, &body);

        // Long header on page 0.
        let mut page0 = short_page_header(0, false, 0);
        page0.resize(SIZE_OF_XLOG_LONG_PHD, 0);
        page0[2..4].copy_from_slice(&XLP_LONG_HEADER.to_le_bytes());

        // Choose a split so the record crosses into page 1.
        let avail_page0 = XLOG_BLCKSZ - SIZE_OF_XLOG_LONG_PHD;
        let first = avail_page0.min(rec.len());
        assert!(first < rec.len(), "record should span the boundary");
        page0.extend_from_slice(&rec[..first]);
        // Fill the rest of page 0 (none here, since first == avail_page0).
        assert_eq!(page0.len(), XLOG_BLCKSZ);

        // Page 1: contrecord short header carrying the remaining bytes.
        let rem = rec.len() - first;
        let mut page1 = short_page_header(XLOG_BLCKSZ as u64, true, rem as u32);
        page1.extend_from_slice(&rec[first..]);

        let mut stream = page0;
        stream.extend_from_slice(&page1);

        let mut dec = WalStreamDecoder::new(Lsn(0));
        dec.feed_bytes(&stream);

        let (lsn, got) = dec.poll_decode().unwrap().expect("spanning record");
        assert_eq!(lsn, Lsn(SIZE_OF_XLOG_LONG_PHD as u64));
        assert_eq!(got, rec, "reassembled record must match the original");
        let d = decode_wal_record(lsn, &got).unwrap();
        assert_eq!(d.blocks[0].blkno, 99);
        assert_eq!(d.main_data.len(), main_len as usize);
    }

    #[test]
    fn stream_returns_none_until_full_record_is_fed() {
        let mut body = block_ref(0, ForkNumber::Main as u8, (1, 1, 1), 5, false);
        body.extend_from_slice(&main_short(0));
        let rec = make_record(5, 0, 1, &body);

        let mut hdr = short_page_header(0, false, 0);
        hdr.resize(SIZE_OF_XLOG_LONG_PHD, 0);
        hdr[2..4].copy_from_slice(&XLP_LONG_HEADER.to_le_bytes());

        let mut dec = WalStreamDecoder::new(Lsn(0));
        dec.feed_bytes(&hdr);
        dec.feed_bytes(&rec[..rec.len() - 2]); // partial record
        assert!(dec.poll_decode().unwrap().is_none());

        dec.feed_bytes(&rec[rec.len() - 2..]); // the rest
        let (_, got) = dec.poll_decode().unwrap().expect("record after full feed");
        assert_eq!(got, rec);
    }
}
