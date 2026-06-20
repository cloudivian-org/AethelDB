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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use common::{Lsn, PageKey, RelTag, PAGE_SIZE};
use tracing::debug;

use crate::layer::{Layer, LayerId};
use crate::page::{Modification, PageError, PageVersion};

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
}

impl Repository {
    /// Create an empty repository that freezes the memtable every
    /// `freeze_threshold` versions.
    pub fn new(freeze_threshold: usize) -> Arc<Self> {
        Arc::new(Repository {
            inner: Mutex::new(Inner {
                memtable: BTreeMap::new(),
                rel_sizes: BTreeMap::new(),
                layers: Vec::new(),
                uploaded: std::collections::HashSet::new(),
                next_layer_id: 0,
                freeze_threshold,
            }),
        })
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

    /// Reconstruct page `key` as it was at `lsn`.
    pub fn get_page(&self, key: PageKey, lsn: Lsn) -> Result<PageLookup, PageError> {
        let inner = self.inner.lock().unwrap();

        // Gather all versions of this page with LSN <= target, across the
        // memtable and every layer, then order them by LSN.
        let mut versions: Vec<(Lsn, &PageVersion)> = Vec::new();
        for ((_, l), v) in inner.memtable.range((key, Lsn::INVALID)..=(key, lsn)) {
            versions.push((*l, v));
        }
        for layer in &inner.layers {
            versions.extend(layer.range(key, lsn));
        }
        versions.sort_by_key(|(l, _)| *l);

        // Find the most recent image, then replay deltas after it.
        let base = match versions.iter().rposition(|(_, v)| v.is_image()) {
            Some(i) => i,
            None => return Ok(PageLookup::NotFound),
        };
        let mut page = vec![0u8; PAGE_SIZE];
        versions[base].1.apply_to(&mut page)?;
        for (_, v) in &versions[base + 1..] {
            v.apply_to(&mut page)?;
        }
        Ok(PageLookup::Page(page))
    }

    /// Number of blocks in a relation fork as of `lsn`, if known.
    pub fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Option<u32> {
        let inner = self.inner.lock().unwrap();
        inner
            .rel_sizes
            .range((rel, Lsn::INVALID)..=(rel, lsn))
            .next_back()
            .map(|(_, &n)| n)
    }

    /// Snapshot of frozen layers not yet uploaded to object storage.
    pub fn pending_offload(&self) -> Vec<Arc<Layer>> {
        let inner = self.inner.lock().unwrap();
        inner
            .layers
            .iter()
            .filter(|l| !inner.uploaded.contains(&l.id()))
            .cloned()
            .collect()
    }

    /// Mark a layer as durably offloaded.
    pub fn mark_uploaded(&self, id: LayerId) {
        self.inner.lock().unwrap().uploaded.insert(id);
    }

    /// Total number of frozen layers (for diagnostics/tests).
    pub fn layer_count(&self) -> usize {
        self.inner.lock().unwrap().layers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{ByteEdit, PageVersion};
    use common::ForkNumber;

    fn rel() -> RelTag {
        RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
    }
    fn key(block: u32) -> PageKey {
        PageKey { rel: rel(), block }
    }

    fn base_image(byte: u8) -> Modification {
        Modification { rel: rel(), block: 0, lsn: Lsn(10), version: PageVersion::Image(vec![byte; PAGE_SIZE]) }
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
            Modification { rel: rel(), block: 0, lsn: Lsn(10), version: PageVersion::Image(vec![0; PAGE_SIZE]) },
            Modification { rel: rel(), block: 4, lsn: Lsn(20), version: PageVersion::Image(vec![0; PAGE_SIZE]) },
        ]);
        assert_eq!(repo.get_rel_size(rel(), Lsn(10)), Some(1));
        assert_eq!(repo.get_rel_size(rel(), Lsn(20)), Some(5));
        assert_eq!(repo.get_rel_size(rel(), Lsn(100)), Some(5));
    }

    #[test]
    fn memtable_freezes_at_threshold() {
        let repo = Repository::new(2);
        repo.ingest([
            Modification { rel: rel(), block: 0, lsn: Lsn(10), version: PageVersion::Image(vec![0; PAGE_SIZE]) },
            Modification { rel: rel(), block: 1, lsn: Lsn(11), version: PageVersion::Image(vec![0; PAGE_SIZE]) },
        ]);
        // Two versions reached the threshold of 2 -> one frozen layer.
        assert_eq!(repo.layer_count(), 1);
    }
}
