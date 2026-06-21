// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The log-structured repository: ingest, indexing, and page reconstruction.
//!
//! Page versions are indexed by `(PageKey, Lsn)` — the `(RelNode, BlockNumber,
//! LSN)` key the design calls for, with `RelNode` widened to a full relation
//! tag. Recent versions live in an in-memory **memtable** (a `BTreeMap`); when
//! it fills it is frozen into an immutable [`Layer`] and a fresh memtable
//! starts. A read therefore consults the memtable plus every frozen layer.
//!
//! ## Reconstruction
//! To answer "page `K` at LSN `Y`", the repository gathers every version of `K`
//! with LSN ≤ `Y` from the memtable and all layers, finds the most recent full
//! **image** at or before `Y`, and replays the **deltas** after it in LSN order
//! — producing the exact 8 KiB block as of `Y`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use common::{Lsn, PageKey, RelTag};
use tracing::debug;

use crate::layer::{Layer, LayerId};
use crate::page::{Modification, PageError, PageVersion, WalRecord};
use crate::waldecode::{decode_wal_record, DecodedWalRecord, WalDecodeError, WalStreamDecoder};
use crate::walredo::{RedoError, RustApplyRedoManager, WalRedoManager};

/// What a compaction pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionStats {
    /// Frozen layers before the pass.
    pub layers_before: usize,
    /// Frozen layers after (the compacted layers replace them).
    pub layers_after: usize,
    /// Page versions dropped by GC.
    pub versions_removed: usize,
    /// Ids of layers that were replaced — their object-store files are now
    /// orphaned and may be deleted by the caller.
    pub removed_layer_ids: Vec<LayerId>,
}

/// Outcome of a page lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageLookup {
    /// The reconstructed 8 KiB page.
    Page(Vec<u8>),
    /// No version of the page exists at or before the requested LSN.
    NotFound,
}

struct Inner {
    /// Recent, mutable versions indexed by `(page, lsn)`.
    memtable: BTreeMap<(PageKey, Lsn), PageVersion>,
    /// Relation fork sizes over time: `(rel, lsn) -> nblocks`.
    rel_sizes: BTreeMap<(RelTag, Lsn), u32>,
    /// Frozen, immutable layers (newest last).
    layers: Vec<Arc<Layer>>,
    /// Layer ids already pushed to object storage.
    uploaded: std::collections::HashSet<LayerId>,
    next_layer_id: LayerId,
    /// Freeze the memtable once it holds this many versions.
    freeze_threshold: usize,
}

/// The page server's storage repository (thread-safe).
pub struct Repository {
    inner: Mutex<Inner>,
    /// Backend that materializes a page from its version history. Stateless and
    /// `Send + Sync`, so it lives outside the lock.
    redo: Arc<dyn WalRedoManager>,
}

impl Repository {
    /// Create an empty repository that freezes the memtable every
    /// `freeze_threshold` versions, using the native Rust apply backend.
    pub fn new(freeze_threshold: usize) -> Arc<Self> {
        Self::with_redo(freeze_threshold, Arc::new(RustApplyRedoManager))
    }

    /// Create a repository with an explicit WAL-redo backend (e.g. a Postgres
    /// wal-redo process in a later phase).
    pub fn with_redo(freeze_threshold: usize, redo: Arc<dyn WalRedoManager>) -> Arc<Self> {
        Arc::new(Repository {
            inner: Mutex::new(Inner {
                memtable: BTreeMap::new(),
                rel_sizes: BTreeMap::new(),
                layers: Vec::new(),
                uploaded: std::collections::HashSet::new(),
                next_layer_id: 0,
                freeze_threshold,
            }),
            redo,
        })
    }

    /// Ingest a raw PostgreSQL WAL byte stream beginning at `start_lsn`.
    ///
    /// Frames the stream into records, decodes each into its per-page changes,
    /// and ingests them: a full-page image becomes a [`PageVersion::Image`];
    /// every other touched block stores the raw record as a
    /// [`PageVersion::WalRecord`] for a wal-redo backend to apply later.
    /// Returns the number of WAL records ingested.
    pub fn ingest_wal(&self, start_lsn: Lsn, wal: &[u8]) -> Result<usize, WalDecodeError> {
        let mut decoder = WalStreamDecoder::new(start_lsn);
        decoder.feed_bytes(wal);
        let mut n = 0;
        while let Some((lsn, record)) = decoder.poll_decode()? {
            self.ingest_record(lsn, &record)?;
            n += 1;
        }
        Ok(n)
    }

    /// Decode one already-framed WAL record (its bytes start at `lsn`) and
    /// ingest its per-page changes.
    ///
    /// This is the per-record seam used by the streaming WAL receiver, which
    /// owns the [`WalStreamDecoder`] across network reads; [`ingest_wal`] is the
    /// one-shot convenience that frames a whole buffer itself.
    pub fn ingest_record(&self, lsn: Lsn, record: &[u8]) -> Result<(), WalDecodeError> {
        let decoded = decode_wal_record(lsn, record)?;
        self.ingest(modifications_from_record(&decoded, record));
        crate::metrics::WAL_RECORDS.inc();
        Ok(())
    }

    /// Ingest a batch of page modifications (as a WAL decoder would emit).
    pub fn ingest<I: IntoIterator<Item = Modification>>(&self, mods: I) {
        let mut inner = self.inner.lock().unwrap();
        for m in mods {
            let key = PageKey { rel: m.rel, block: m.block };

            // Track relation size: at this LSN the fork is at least block+1 long.
            let prior = inner
                .rel_sizes
                .range((m.rel, Lsn::INVALID)..=(m.rel, m.lsn))
                .next_back()
                .map(|(_, &n)| n)
                .unwrap_or(0);
            let size = prior.max(m.block + 1);
            let slot = inner.rel_sizes.entry((m.rel, m.lsn)).or_insert(0);
            *slot = (*slot).max(size);

            inner.memtable.insert((key, m.lsn), m.version);

            if inner.memtable.len() >= inner.freeze_threshold {
                Self::freeze_locked(&mut inner);
            }
        }
    }

    /// Force the current memtable into a frozen layer (no-op if empty).
    pub fn freeze(&self) {
        let mut inner = self.inner.lock().unwrap();
        Self::freeze_locked(&mut inner);
    }

    fn freeze_locked(inner: &mut Inner) {
        if inner.memtable.is_empty() {
            return;
        }
        let id = inner.next_layer_id;
        inner.next_layer_id += 1;
        let entries = std::mem::take(&mut inner.memtable);
        let n = entries.len();
        inner.layers.push(Arc::new(Layer::new(id, entries)));
        debug!(layer_id = id, versions = n, "froze memtable into layer");
    }

    /// Reconstruct page `key` as it was at `lsn`, using only this store's data.
    pub fn get_page(&self, key: PageKey, lsn: Lsn) -> Result<PageLookup, PageError> {
        self.get_page_with_base(key, lsn, None)
    }

    /// Reconstruct page `key` at `lsn`, optionally starting from an externally
    /// supplied base image at `base.0` (e.g. the same page on a parent timeline
    /// at the branch point). This store's versions are applied on top.
    ///
    /// With no base and no local base version, the page is `NotFound`. With a
    /// base, the store's deltas are replayed over it; a newer full image in this
    /// store supersedes the base, as usual.
    pub fn get_page_with_base(
        &self,
        key: PageKey,
        lsn: Lsn,
        base: Option<(Lsn, Vec<u8>)>,
    ) -> Result<PageLookup, PageError> {
        let inner = self.inner.lock().unwrap();

        // The supplied base, wrapped as an Image version. Held for the duration
        // so `versions` can borrow it alongside the in-store versions.
        let base_holder = base.map(|(blsn, page)| (blsn, PageVersion::Image(page)));

        // Gather all versions of this page with LSN <= target, across the
        // (optional) base, the memtable, and every layer, ordered by LSN.
        let mut versions: Vec<(Lsn, &PageVersion)> = Vec::new();
        if let Some((blsn, ref bver)) = base_holder {
            versions.push((blsn, bver));
        }
        for ((_, l), v) in inner.memtable.range((key, Lsn::INVALID)..=(key, lsn)) {
            versions.push((*l, v));
        }
        for layer in &inner.layers {
            versions.extend(layer.range(key, lsn));
        }
        versions.sort_by_key(|(l, _)| *l);

        // Hand the ordered history to the redo backend, which knows how to
        // apply each version kind (images, byte-edits, or real WAL records).
        match self.redo.reconstruct(key, lsn, &versions) {
            Ok(Some(page)) => Ok(PageLookup::Page(page)),
            Ok(None) => Ok(PageLookup::NotFound),
            Err(RedoError::Apply(e)) => Err(e),
            // NeedsPostgres / Process / RedoFailed all surface as a redo error.
            Err(e) => Err(PageError::Redo(e.to_string())),
        }
    }

    /// Number of blocks in a relation fork as of `lsn`, if known.
    pub fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Option<u32> {
        let inner = self.inner.lock().unwrap();
        inner.rel_sizes.range((rel, Lsn::INVALID)..=(rel, lsn)).next_back().map(|(_, &n)| n)
    }

    /// Snapshot of frozen layers not yet uploaded to object storage.
    pub fn pending_offload(&self) -> Vec<Arc<Layer>> {
        let inner = self.inner.lock().unwrap();
        inner.layers.iter().filter(|l| !inner.uploaded.contains(&l.id())).cloned().collect()
    }

    /// Mark a layer as durably offloaded.
    pub fn mark_uploaded(&self, id: LayerId) {
        self.inner.lock().unwrap().uploaded.insert(id);
    }

    /// Total number of frozen layers (for diagnostics/tests).
    pub fn layer_count(&self) -> usize {
        self.inner.lock().unwrap().layers.len()
    }

    /// Compact frozen layers into one, GC-pruning any page version that can
    /// never be read at an LSN ≥ `gc_horizon`.
    ///
    /// For each page, the latest base (full image / `will_init`) at or before
    /// `gc_horizon` is kept along with everything after it; older versions are
    /// unreachable within the retention window and are dropped. This bounds
    /// history and collapses the layer stack (read amplification). The caller
    /// must not serve reads below `gc_horizon` after this returns.
    pub fn compact(&self, gc_horizon: Lsn) -> CompactionStats {
        let mut inner = self.inner.lock().unwrap();
        if inner.layers.is_empty() {
            return CompactionStats::default();
        }
        let layers_before = inner.layers.len();
        let removed_layer_ids: Vec<LayerId> = inner.layers.iter().map(|l| l.id()).collect();

        // Merge every frozen layer's entries. LSNs are unique per write, so keys
        // don't collide across layers.
        let mut merged: BTreeMap<(PageKey, Lsn), PageVersion> = BTreeMap::new();
        for layer in &inner.layers {
            for (k, v) in layer.entries() {
                merged.insert(*k, v.clone());
            }
        }
        let before = merged.len();

        // GC: per page, find the latest base at or before the horizon, then drop
        // every version of that page strictly older than it.
        let mut base_lsn: HashMap<PageKey, Lsn> = HashMap::new();
        for ((key, lsn), v) in merged.iter() {
            if *lsn <= gc_horizon && v.is_base() {
                base_lsn.insert(*key, *lsn); // ascending scan -> latest wins
            }
        }
        let drop_keys: Vec<(PageKey, Lsn)> = merged
            .iter()
            .filter(|((key, lsn), _)| base_lsn.get(key).is_some_and(|b| lsn < b))
            .map(|((key, lsn), _)| (*key, *lsn))
            .collect();
        for k in &drop_keys {
            merged.remove(k);
        }
        let versions_removed = before - merged.len();

        // Replace the layer stack with a single compacted layer.
        inner.layers = if merged.is_empty() {
            Vec::new()
        } else {
            let id = inner.next_layer_id;
            inner.next_layer_id += 1;
            vec![Arc::new(Layer::new(id, merged))]
        };
        for oid in &removed_layer_ids {
            inner.uploaded.remove(oid);
        }
        let layers_after = inner.layers.len();
        crate::metrics::GC_VERSIONS_REMOVED.inc_by(versions_removed as u64);
        debug!(layers_before, layers_after, versions_removed, %gc_horizon, "compacted layers");
        CompactionStats { layers_before, layers_after, versions_removed, removed_layer_ids }
    }
}

/// Map a decoded WAL record to one ingest [`Modification`] per touched block.
///
/// A block carrying a restorable full-page image becomes a
/// [`PageVersion::Image`] (a reconstruction base); every other block — and any
/// image we can't yet restore (e.g. compressed) — stores the raw record bytes as
/// a [`PageVersion::WalRecord`] for a wal-redo backend to apply later.
fn modifications_from_record(decoded: &DecodedWalRecord, raw: &[u8]) -> Vec<Modification> {
    let mut mods = Vec::with_capacity(decoded.blocks.len());
    for b in &decoded.blocks {
        let version = match &b.image {
            Some(img) => match img.restore() {
                Ok(page) => PageVersion::Image(page),
                Err(_) => {
                    PageVersion::WalRecord(WalRecord { will_init: b.will_init, rec: raw.to_vec() })
                }
            },
            None => PageVersion::WalRecord(WalRecord { will_init: b.will_init, rec: raw.to_vec() }),
        };
        mods.push(Modification { rel: b.rel, block: b.blkno, lsn: decoded.lsn, version });
    }
    mods
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{ByteEdit, PageVersion};
    use common::{ForkNumber, PAGE_SIZE};

    fn rel() -> RelTag {
        RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
    }
    fn key(block: u32) -> PageKey {
        PageKey { rel: rel(), block }
    }

    fn base_image(byte: u8) -> Modification {
        Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(10),
            version: PageVersion::Image(vec![byte; PAGE_SIZE]),
        }
    }

    #[test]
    fn reconstructs_image_plus_deltas_at_each_lsn() {
        let repo = Repository::new(1_000);
        repo.ingest([
            base_image(0),
            Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(20),
                version: PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![0xAA] }]),
            },
            Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(30),
                version: PageVersion::Delta(vec![ByteEdit { offset: 1, data: vec![0xBB] }]),
            },
        ]);

        // At LSN 10: just the base image.
        match repo.get_page(key(0), Lsn(10)).unwrap() {
            PageLookup::Page(p) => assert_eq!(p[0], 0),
            other => panic!("{other:?}"),
        }
        // At LSN 25: base + first delta only.
        match repo.get_page(key(0), Lsn(25)).unwrap() {
            PageLookup::Page(p) => {
                assert_eq!(p[0], 0xAA);
                assert_eq!(p[1], 0);
            }
            other => panic!("{other:?}"),
        }
        // At LSN 30: base + both deltas.
        match repo.get_page(key(0), Lsn(30)).unwrap() {
            PageLookup::Page(p) => {
                assert_eq!(p[0], 0xAA);
                assert_eq!(p[1], 0xBB);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn reconstruction_reads_across_frozen_layers() {
        let repo = Repository::new(1_000);
        repo.ingest([base_image(7)]);
        repo.freeze(); // image now lives in a layer
        repo.ingest([Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(20),
            version: PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![0x55] }]),
        }]);
        // Reconstruction must combine the layer's image with the memtable delta.
        match repo.get_page(key(0), Lsn(20)).unwrap() {
            PageLookup::Page(p) => {
                assert_eq!(p[0], 0x55);
                assert_eq!(p[1], 7);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(repo.layer_count(), 1);
    }

    #[test]
    fn unknown_page_is_not_found() {
        let repo = Repository::new(1_000);
        repo.ingest([base_image(0)]);
        assert_eq!(repo.get_page(key(999), Lsn(100)).unwrap(), PageLookup::NotFound);
        // A page whose first version is after the requested LSN is also absent.
        assert_eq!(repo.get_page(key(0), Lsn(5)).unwrap(), PageLookup::NotFound);
    }

    #[test]
    fn rel_size_tracks_highest_block_over_lsn() {
        let repo = Repository::new(1_000);
        repo.ingest([
            Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(10),
                version: PageVersion::Image(vec![0; PAGE_SIZE]),
            },
            Modification {
                rel: rel(),
                block: 4,
                lsn: Lsn(20),
                version: PageVersion::Image(vec![0; PAGE_SIZE]),
            },
        ]);
        assert_eq!(repo.get_rel_size(rel(), Lsn(10)), Some(1));
        assert_eq!(repo.get_rel_size(rel(), Lsn(20)), Some(5));
        assert_eq!(repo.get_rel_size(rel(), Lsn(100)), Some(5));
    }

    #[test]
    fn memtable_freezes_at_threshold() {
        let repo = Repository::new(2);
        repo.ingest([
            Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(10),
                version: PageVersion::Image(vec![0; PAGE_SIZE]),
            },
            Modification {
                rel: rel(),
                block: 1,
                lsn: Lsn(11),
                version: PageVersion::Image(vec![0; PAGE_SIZE]),
            },
        ]);
        // Two versions reached the threshold of 2 -> one frozen layer.
        assert_eq!(repo.layer_count(), 1);
    }

    #[test]
    fn compaction_collapses_history_and_bounds_reads() {
        let repo = Repository::new(1); // freeze each write into its own layer
        let img = |byte: u8, lsn: u64| Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(lsn),
            version: PageVersion::Image(vec![byte; PAGE_SIZE]),
        };
        let d = |byte: u8, lsn: u64| Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(lsn),
            version: PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![byte] }]),
        };
        repo.ingest([img(7, 10)]); // base
        repo.ingest([d(0xAA, 20)]); // delta over base@10
        repo.ingest([img(1, 30)]); // a newer full image supersedes
        repo.ingest([d(0xBB, 40)]); // delta over image@30
        assert!(repo.layer_count() >= 4);

        // Compact at horizon 35: the latest base ≤ 35 is image@30, so image@10
        // and delta@20 become unreachable and are dropped.
        let stats = repo.compact(Lsn(35));
        assert_eq!(stats.layers_after, 1, "stack collapses to one layer");
        assert_eq!(stats.versions_removed, 2, "image@10 and delta@20 pruned");
        assert_eq!(stats.removed_layer_ids.len(), stats.layers_before);

        // Reads within the retention window are unchanged.
        match repo.get_page(key(0), Lsn(40)).unwrap() {
            PageLookup::Page(p) => {
                assert_eq!(p[0], 0xBB); // delta@40
                assert_eq!(p[1], 1); // from image@30
            }
            other => panic!("{other:?}"),
        }
        match repo.get_page(key(0), Lsn(30)).unwrap() {
            PageLookup::Page(p) => assert!(p.iter().all(|&b| b == 1)),
            other => panic!("{other:?}"),
        }
        // Below the horizon, the collapsed history is gone.
        assert_eq!(repo.get_page(key(0), Lsn(20)).unwrap(), PageLookup::NotFound);
    }

    #[test]
    fn compaction_keeps_deltas_without_a_local_base() {
        // A delta with no base ≤ horizon must be retained (it may sit on top of
        // an inherited base on a branch).
        let repo = Repository::new(1);
        repo.ingest([Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(30),
            version: PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![0x55] }]),
        }]);
        let stats = repo.compact(Lsn(100));
        assert_eq!(stats.versions_removed, 0, "a base-less delta is never pruned");
    }

    // ---- Real-WAL ingest (Phase 2) ----
    //
    // These build format-accurate PG16 WAL bytes and push them through the
    // public `ingest_wal` path, exercising the framing + decode + redo pipeline
    // end to end rather than hand-feeding `Modification`s.

    use crate::waldecode::{SIZE_OF_XLOG_LONG_PHD, XLOG_PAGE_MAGIC_PG16};

    /// A long page header at LSN 0 (magic + XLP_LONG_HEADER), rest zeroed.
    fn long_header() -> Vec<u8> {
        let mut h = vec![0u8; SIZE_OF_XLOG_LONG_PHD];
        h[0..2].copy_from_slice(&XLOG_PAGE_MAGIC_PG16.to_le_bytes());
        h[2..4].copy_from_slice(&0x0002u16.to_le_bytes()); // XLP_LONG_HEADER
        h
    }

    /// Wrap a record body in a 24-byte XLogRecord header with a valid length.
    fn xlog_record(rmid: u8, body: &[u8]) -> Vec<u8> {
        let tot = 24 + body.len();
        let mut r = Vec::with_capacity(tot);
        r.extend_from_slice(&(tot as u32).to_le_bytes()); // xl_tot_len
        r.extend_from_slice(&0u32.to_le_bytes()); // xl_xid
        r.extend_from_slice(&0u64.to_le_bytes()); // xl_prev
        r.push(0); // xl_info
        r.push(rmid); // xl_rmid
        r.extend_from_slice(&[0, 0]); // padding
        r.extend_from_slice(&0u32.to_le_bytes()); // xl_crc
        r.extend_from_slice(body);
        r
    }

    /// A record body with a single full-page image (with a hole) for block 0 of
    /// `rel()`. The stored 8 bytes surround an all-zero hole in the page.
    fn fpi_body() -> Vec<u8> {
        let stored: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        let hole_offset = 4u16;
        let mut b = Vec::new();
        b.push(0u8); // block_id
        b.push(0x10 | (ForkNumber::Main as u8)); // BKPBLOCK_HAS_IMAGE | fork
        b.extend_from_slice(&0u16.to_le_bytes()); // data_len
        b.extend_from_slice(&(stored.len() as u16).to_le_bytes()); // bimg_len
        b.extend_from_slice(&hole_offset.to_le_bytes()); // hole_offset
        b.push(0x01 | 0x02); // BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY (uncompressed)
        b.extend_from_slice(&rel().spc_node.to_le_bytes());
        b.extend_from_slice(&rel().db_node.to_le_bytes());
        b.extend_from_slice(&rel().rel_node.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes()); // blkno
        b.push(255u8); // XLR_BLOCK_ID_DATA_SHORT
        b.push(0u8); // main_data_len = 0
        b.extend_from_slice(&stored); // image bytes
        b
    }

    /// A record body that references block 0 of `rel()` with no image (a real
    /// change that would need Postgres redo to apply).
    fn delta_body() -> Vec<u8> {
        let mut b = Vec::new();
        b.push(0u8); // block_id
        b.push(ForkNumber::Main as u8); // fork, no flags
        b.extend_from_slice(&0u16.to_le_bytes()); // data_len
        b.extend_from_slice(&rel().spc_node.to_le_bytes());
        b.extend_from_slice(&rel().db_node.to_le_bytes());
        b.extend_from_slice(&rel().rel_node.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes()); // blkno
        b.push(255u8);
        b.push(0u8);
        b
    }

    /// Lay records into one long-header WAL page, MAXALIGN-padded between them.
    fn wal_page(records: &[Vec<u8>]) -> Vec<u8> {
        let mut page = long_header();
        for (i, rec) in records.iter().enumerate() {
            page.extend_from_slice(rec);
            if i + 1 < records.len() {
                let pad = (8 - (page.len() % 8)) % 8;
                page.extend(std::iter::repeat(0u8).take(pad));
            }
        }
        page
    }

    #[test]
    fn ingest_wal_materializes_a_full_page_image() {
        let repo = Repository::new(1_000);
        let wal = wal_page(&[xlog_record(10, &fpi_body())]);

        let n = repo.ingest_wal(Lsn(0), &wal).expect("decode WAL");
        assert_eq!(n, 1);

        match repo.get_page(key(0), Lsn(1_000)).unwrap() {
            PageLookup::Page(p) => {
                assert_eq!(p.len(), PAGE_SIZE);
                assert_eq!(&p[0..4], &[1, 2, 3, 4]); // before the hole
                assert_eq!(&p[PAGE_SIZE - 4..], &[5, 6, 7, 8]); // after the hole
                assert!(p[4..PAGE_SIZE - 4].iter().all(|&b| b == 0)); // the hole
            }
            other => panic!("{other:?}"),
        }
        // The page's relation size is now known from the WAL.
        assert_eq!(repo.get_rel_size(rel(), Lsn(1_000)), Some(1));
    }

    #[test]
    fn ingest_wal_keeps_raw_record_for_non_image_change() {
        let repo = Repository::new(1_000);
        // An FPI base followed by a real (non-image) change to the same page.
        let wal = wal_page(&[xlog_record(10, &fpi_body()), xlog_record(10, &delta_body())]);

        let n = repo.ingest_wal(Lsn(0), &wal).expect("decode WAL");
        assert_eq!(n, 2);

        // The native backend can't apply a raw WAL record: reconstruction must
        // report that a Postgres wal-redo backend is required (Phase 3), rather
        // than silently dropping the change or corrupting the page.
        let err = repo.get_page(key(0), Lsn(1_000)).unwrap_err();
        assert!(matches!(err, PageError::Redo(_)), "expected Redo error, got {err:?}");
    }
}
