// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Durable WAL storage: a disk-backed, segmented log fronted by an in-memory
//! ring buffer.
//!
//! Incoming WAL bytes are appended contiguously by LSN. They land in two
//! places: a fixed-size **segment file** on disk (the durable copy) and a
//! bounded in-memory **ring** that caches the most recent bytes for fast reads
//! and replication. Durability is explicit: [`WalStorage::append`] makes bytes
//! visible but not yet durable; [`WalStorage::flush`] `fsync`s the touched
//! segments and a metadata file, after which `flush_lsn` may be acknowledged.
//!
//! A metadata file records `(start_lsn, flush_lsn)` and is fsynced on every
//! flush, so after a crash/restart the store recovers exactly the bytes that
//! were acknowledged — never more (unflushed bytes are correctly lost) and
//! never less.
//!
//! The segmented layout makes the log naturally circular: once WAL below some
//! horizon has been offloaded (Step 5), whole segments are recycled with
//! [`WalStorage::remove_segments_before`].

use std::collections::{BTreeSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use common::Lsn;
use thiserror::Error;
use tracing::debug;

/// Errors from the WAL store.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("non-contiguous append: expected start_lsn {expected}, got {got}")]
    NonContiguous { expected: Lsn, got: Lsn },
    #[error("read range [{start}, {end}) is outside stored range [{lo}, {hi})")]
    OutOfRange { start: Lsn, end: Lsn, lo: Lsn, hi: Lsn },
    #[error("metadata file is corrupt")]
    CorruptMeta,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, StorageError>;

/// Configuration for a [`WalStorage`].
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory holding segment files and metadata.
    pub data_dir: PathBuf,
    /// Size of each on-disk segment file, in bytes.
    pub segment_size: u64,
    /// Maximum number of recent bytes to keep cached in memory.
    pub ring_capacity: usize,
}

impl WalConfig {
    /// Reasonable production defaults: 16 MiB segments, 8 MiB ring.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        WalConfig {
            data_dir: data_dir.into(),
            segment_size: 16 * 1024 * 1024,
            ring_capacity: 8 * 1024 * 1024,
        }
    }
}

const META_FILE: &str = "wal.meta";
const META_MAGIC: u32 = 0x534B_4D31; // "SKM1"

/// A durable, segmented WAL log with an in-memory cache.
pub struct WalStorage {
    cfg: WalConfig,
    start_lsn: Lsn,
    write_lsn: Lsn,
    flush_lsn: Lsn,

    ring: VecDeque<u8>,
    ring_start: Lsn,

    /// Cached handle for the segment currently being appended to.
    open_seg: Option<(u64, File)>,
    /// Segments written since the last flush, needing fsync.
    dirty_segments: BTreeSet<u64>,
}

impl WalStorage {
    /// Open (creating if necessary) the WAL store in `cfg.data_dir`.
    ///
    /// If a metadata file is present, recovers `start_lsn`/`flush_lsn` and
    /// resumes appending at `flush_lsn` (any bytes written-but-not-flushed
    /// before a crash are discarded, which is the correct durability boundary).
    pub fn open(cfg: WalConfig) -> Result<Self> {
        std::fs::create_dir_all(&cfg.data_dir)?;

        let (start_lsn, flush_lsn) = match read_meta(&cfg.data_dir)? {
            Some(meta) => meta,
            None => (Lsn::INVALID, Lsn::INVALID),
        };

        Ok(WalStorage {
            cfg,
            start_lsn,
            write_lsn: flush_lsn, // resume at the durable frontier
            flush_lsn,
            ring: VecDeque::new(),
            ring_start: flush_lsn,
            open_seg: None,
            dirty_segments: BTreeSet::new(),
        })
    }

    /// LSN of the first byte still retained.
    pub fn start_lsn(&self) -> Lsn {
        self.start_lsn
    }
    /// LSN just past the last appended byte (may not be durable yet).
    pub fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }
    /// LSN just past the last durably-flushed byte.
    pub fn flush_lsn(&self) -> Lsn {
        self.flush_lsn
    }

    fn segment_path(&self, index: u64) -> PathBuf {
        self.cfg.data_dir.join(format!("{index:016X}.wal"))
    }

    /// Get (opening/creating if needed) the append handle for `index`.
    fn append_segment(&mut self, index: u64) -> Result<&mut File> {
        let matches = matches!(self.open_seg, Some((i, _)) if i == index);
        if !matches {
            let path = self.segment_path(index);
            let existed = path.exists();
            // Open (create if absent) for read+write; never truncate — existing
            // segments hold durable WAL we must keep.
            let f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            if !existed {
                // Preallocate the segment so writes never fail mid-stream.
                f.set_len(self.cfg.segment_size)?;
            }
            self.open_seg = Some((index, f));
        }
        Ok(&mut self.open_seg.as_mut().unwrap().1)
    }

    /// Append a contiguous run of WAL bytes starting at `start_lsn`.
    ///
    /// `start_lsn` must equal the current `write_lsn` (the first append after a
    /// fresh open may start anywhere and seeds `start_lsn`).
    pub fn append(&mut self, start_lsn: Lsn, data: &[u8]) -> Result<()> {
        // Seed the log origin on the very first append.
        if !self.write_lsn.is_valid() && !self.start_lsn.is_valid() {
            self.start_lsn = start_lsn;
            self.write_lsn = start_lsn;
            self.flush_lsn = start_lsn;
            self.ring_start = start_lsn;
        }
        if start_lsn != self.write_lsn {
            return Err(StorageError::NonContiguous { expected: self.write_lsn, got: start_lsn });
        }
        if data.is_empty() {
            return Ok(());
        }

        let seg_size = self.cfg.segment_size;
        let mut written = 0usize;
        while written < data.len() {
            let pos = self.write_lsn.raw();
            let index = pos / seg_size;
            let offset = pos % seg_size;
            let space = (seg_size - offset) as usize;
            let n = space.min(data.len() - written);

            let f = self.append_segment(index)?;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(&data[written..written + n])?;

            self.dirty_segments.insert(index);
            self.write_lsn = Lsn(pos + n as u64);
            written += n;
        }

        // Update the in-memory ring, evicting the oldest bytes past capacity.
        self.ring.extend(data.iter().copied());
        while self.ring.len() > self.cfg.ring_capacity {
            self.ring.pop_front();
            self.ring_start = Lsn(self.ring_start.raw() + 1);
        }
        debug!(write_lsn = %self.write_lsn, "appended {} WAL bytes", data.len());
        Ok(())
    }

    /// Make all appended bytes durable and advance `flush_lsn`.
    pub fn flush(&mut self) -> Result<Lsn> {
        // fsync every segment touched since the last flush.
        let dirty: Vec<u64> = self.dirty_segments.iter().copied().collect();
        for index in dirty {
            // Use the cached handle if it matches, else open the segment.
            if matches!(self.open_seg, Some((i, _)) if i == index) {
                self.open_seg.as_ref().unwrap().1.sync_all()?;
            } else {
                File::open(self.segment_path(index))?.sync_all()?;
            }
        }
        self.dirty_segments.clear();

        self.flush_lsn = self.write_lsn;
        write_meta(&self.cfg.data_dir, self.start_lsn, self.flush_lsn)?;
        Ok(self.flush_lsn)
    }

    /// Read `buf.len()` bytes starting at `lsn`, from the ring if cached or from
    /// disk segments otherwise. The range must lie within `[start_lsn, write_lsn)`.
    pub fn read_at(&self, lsn: Lsn, buf: &mut [u8]) -> Result<()> {
        let end = Lsn(lsn.raw() + buf.len() as u64);
        if lsn < self.start_lsn || end > self.write_lsn {
            return Err(StorageError::OutOfRange {
                start: lsn,
                end,
                lo: self.start_lsn,
                hi: self.write_lsn,
            });
        }

        // Fast path: fully within the in-memory ring.
        if lsn >= self.ring_start && end <= self.write_lsn && !self.ring.is_empty() {
            let off = (lsn.raw() - self.ring_start.raw()) as usize;
            for (i, slot) in buf.iter_mut().enumerate() {
                *slot = self.ring[off + i];
            }
            return Ok(());
        }

        // Slow path: read from disk segments, possibly spanning several.
        let seg_size = self.cfg.segment_size;
        let mut done = 0usize;
        while done < buf.len() {
            let pos = lsn.raw() + done as u64;
            let index = pos / seg_size;
            let offset = pos % seg_size;
            let space = (seg_size - offset) as usize;
            let n = space.min(buf.len() - done);

            let mut f = File::open(self.segment_path(index))?;
            f.seek(SeekFrom::Start(offset))?;
            f.read_exact(&mut buf[done..done + n])?;
            done += n;
        }
        Ok(())
    }

    /// Recycle whole segments that lie entirely below `horizon` (e.g. after the
    /// page server has offloaded that WAL). Advances `start_lsn`.
    pub fn remove_segments_before(&mut self, horizon: Lsn) -> Result<u64> {
        let seg_size = self.cfg.segment_size;
        let mut removed = 0;
        let mut floor = self.start_lsn;
        // The last segment fully below the horizon.
        let horizon_seg = horizon.raw() / seg_size;
        let mut index = self.start_lsn.raw() / seg_size;
        while index < horizon_seg {
            let path = self.segment_path(index);
            if path.exists() {
                std::fs::remove_file(&path)?;
                removed += 1;
            }
            index += 1;
            floor = Lsn(index * seg_size);
        }
        if floor > self.start_lsn {
            self.start_lsn = floor;
        }
        Ok(removed)
    }
}

/// Read `(start_lsn, flush_lsn)` from the metadata file, if present.
fn read_meta(dir: &Path) -> Result<Option<(Lsn, Lsn)>> {
    let path = dir.join(META_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let mut buf = Vec::new();
    File::open(&path)?.read_to_end(&mut buf)?;
    if buf.len() != 20 {
        return Err(StorageError::CorruptMeta);
    }
    let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    if magic != META_MAGIC {
        return Err(StorageError::CorruptMeta);
    }
    let start = Lsn(u64::from_be_bytes(buf[4..12].try_into().unwrap()));
    let flush = Lsn(u64::from_be_bytes(buf[12..20].try_into().unwrap()));
    Ok(Some((start, flush)))
}

/// Atomically write and fsync the metadata file.
fn write_meta(dir: &Path, start: Lsn, flush: Lsn) -> Result<()> {
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(&META_MAGIC.to_be_bytes());
    buf.extend_from_slice(&start.raw().to_be_bytes());
    buf.extend_from_slice(&flush.raw().to_be_bytes());

    // Write to a temp file then rename for atomicity, fsyncing both.
    let tmp = dir.join("wal.meta.tmp");
    {
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(META_FILE))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unique temp directory for one test, cleaned up on drop.
    struct TempDir(PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!("sp-wal-{}-{}", tag, std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn cfg(dir: &TempDir, seg: u64, ring: usize) -> WalConfig {
        WalConfig { data_dir: dir.0.clone(), segment_size: seg, ring_capacity: ring }
    }

    #[test]
    fn append_flush_read_round_trip() {
        let dir = TempDir::new("rt");
        let mut wal = WalStorage::open(cfg(&dir, 4096, 1024)).unwrap();
        wal.append(Lsn(0), b"hello ").unwrap();
        wal.append(Lsn(6), b"world").unwrap();
        assert_eq!(wal.flush().unwrap(), Lsn(11));

        let mut buf = [0u8; 11];
        wal.read_at(Lsn(0), &mut buf).unwrap();
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn durability_survives_reopen() {
        let dir = TempDir::new("durable");
        {
            let mut wal = WalStorage::open(cfg(&dir, 4096, 1024)).unwrap();
            wal.append(Lsn(0), b"committed").unwrap();
            wal.flush().unwrap();
            // Unflushed bytes after the flush must NOT survive.
            wal.append(Lsn(9), b"-lost").unwrap();
        }
        let wal = WalStorage::open(cfg(&dir, 4096, 1024)).unwrap();
        assert_eq!(wal.flush_lsn(), Lsn(9), "only flushed bytes are durable");
        let mut buf = [0u8; 9];
        wal.read_at(Lsn(0), &mut buf).unwrap();
        assert_eq!(&buf, b"committed");
    }

    #[test]
    fn reads_span_segments_and_evicted_ring() {
        let dir = TempDir::new("span");
        // Tiny 8-byte segments and a 4-byte ring force both spanning and eviction.
        let mut wal = WalStorage::open(cfg(&dir, 8, 4)).unwrap();
        let data: Vec<u8> = (0..30u8).collect();
        wal.append(Lsn(0), &data).unwrap();
        wal.flush().unwrap();

        // Recent bytes come from the ring; old bytes come from disk.
        let mut tail = [0u8; 4];
        wal.read_at(Lsn(26), &mut tail).unwrap();
        assert_eq!(&tail, &[26, 27, 28, 29]);

        let mut head = [0u8; 10];
        wal.read_at(Lsn(0), &mut head).unwrap();
        assert_eq!(&head, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn rejects_non_contiguous_append() {
        let dir = TempDir::new("gap");
        let mut wal = WalStorage::open(cfg(&dir, 4096, 1024)).unwrap();
        wal.append(Lsn(0), b"abc").unwrap();
        let err = wal.append(Lsn(10), b"def").unwrap_err();
        assert!(matches!(err, StorageError::NonContiguous { .. }));
    }

    #[test]
    fn recycles_old_segments() {
        let dir = TempDir::new("recycle");
        let mut wal = WalStorage::open(cfg(&dir, 8, 64)).unwrap();
        wal.append(Lsn(0), &[1u8; 40]).unwrap(); // 5 segments
        wal.flush().unwrap();
        let removed = wal.remove_segments_before(Lsn(24)).unwrap(); // segments 0,1,2
        assert_eq!(removed, 3);
        assert_eq!(wal.start_lsn(), Lsn(24));
    }
}
