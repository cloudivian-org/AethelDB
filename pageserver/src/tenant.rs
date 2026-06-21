// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A **tenant** — one isolated database instance and the set of [`Timeline`]s
//! (branches) that make up its history.
//!
//! The tenant is the branch manager: it creates the root timeline, branches new
//! child timelines off any existing one at a chosen LSN (the "instant branch"
//! and point-in-time operations), and resolves a [`TimelineId`] to its
//! [`Timeline`] for the page-service and ingest paths.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common::{Lsn, TimelineId};
use thiserror::Error;

use crate::repository::{CompactionStats, Repository};
use crate::timeline::Timeline;
use crate::walredo::{RustApplyRedoManager, WalRedoManager};

/// Errors from tenant/branch management.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TenantError {
    #[error("timeline {0} already exists")]
    AlreadyExists(TimelineId),
    #[error("parent timeline {0} does not exist")]
    NoSuchParent(TimelineId),
}

/// One isolated database instance: a collection of branchable timelines.
pub struct Tenant {
    timelines: Mutex<HashMap<TimelineId, Arc<Timeline>>>,
    freeze_threshold: usize,
    redo: Arc<dyn WalRedoManager>,
}

impl Tenant {
    /// Create an empty tenant using the native Rust redo backend.
    pub fn new(freeze_threshold: usize) -> Arc<Self> {
        Self::with_redo(freeze_threshold, Arc::new(RustApplyRedoManager))
    }

    /// Create an empty tenant with an explicit WAL-redo backend (e.g. a
    /// Postgres wal-redo process), shared by every timeline's store.
    pub fn with_redo(freeze_threshold: usize, redo: Arc<dyn WalRedoManager>) -> Arc<Self> {
        Arc::new(Tenant { timelines: Mutex::new(HashMap::new()), freeze_threshold, redo })
    }

    fn new_repo(&self) -> Arc<Repository> {
        Repository::with_redo(self.freeze_threshold, self.redo.clone())
    }

    /// Create a fresh root timeline (no ancestor).
    pub fn create_timeline(&self, id: TimelineId) -> Result<Arc<Timeline>, TenantError> {
        let mut timelines = self.timelines.lock().unwrap();
        if timelines.contains_key(&id) {
            return Err(TenantError::AlreadyExists(id));
        }
        let tl = Timeline::root(id, self.new_repo());
        timelines.insert(id, tl.clone());
        crate::metrics::TIMELINES.set(timelines.len() as i64);
        Ok(tl)
    }

    /// Branch a new timeline `new_id` off `parent_id` at `ancestor_lsn`.
    ///
    /// Instant: the branch starts empty and shares all of the parent's history
    /// up to `ancestor_lsn`, diverging only as it is written to.
    pub fn branch_timeline(
        &self,
        new_id: TimelineId,
        parent_id: TimelineId,
        ancestor_lsn: Lsn,
    ) -> Result<Arc<Timeline>, TenantError> {
        let mut timelines = self.timelines.lock().unwrap();
        if timelines.contains_key(&new_id) {
            return Err(TenantError::AlreadyExists(new_id));
        }
        let parent =
            timelines.get(&parent_id).cloned().ok_or(TenantError::NoSuchParent(parent_id))?;
        let tl = Timeline::branched(new_id, self.new_repo(), parent, ancestor_lsn);
        timelines.insert(new_id, tl.clone());
        crate::metrics::TIMELINES.set(timelines.len() as i64);
        Ok(tl)
    }

    /// Resolve a timeline by id.
    pub fn get_timeline(&self, id: TimelineId) -> Option<Arc<Timeline>> {
        self.timelines.lock().unwrap().get(&id).cloned()
    }

    /// All timeline ids currently known to this tenant.
    pub fn timeline_ids(&self) -> Vec<TimelineId> {
        self.timelines.lock().unwrap().keys().copied().collect()
    }

    /// Compact + GC every timeline at `requested_horizon`.
    ///
    /// A timeline's effective horizon is lowered so it never collects history a
    /// child branch still depends on: a child reads its parent at the branch
    /// point, so each parent retains down to the minimum `ancestor_lsn` of its
    /// direct children. (Deeper descendants are covered transitively — each
    /// level pins its own parent.) Returns per-timeline compaction stats.
    pub fn gc(&self, requested_horizon: Lsn) -> Vec<(TimelineId, CompactionStats)> {
        let timelines = self.timelines.lock().unwrap();

        // The lowest branch point pinned on each parent timeline.
        let mut child_pin: HashMap<TimelineId, Lsn> = HashMap::new();
        for tl in timelines.values() {
            if let (Some(parent), Some(alsn)) = (tl.ancestor_timeline(), tl.ancestor_lsn()) {
                let pin = child_pin.entry(parent).or_insert(Lsn(u64::MAX));
                *pin = (*pin).min(alsn);
            }
        }

        timelines
            .iter()
            .map(|(id, tl)| {
                let horizon = match child_pin.get(id) {
                    Some(pin) => requested_horizon.min(*pin),
                    None => requested_horizon,
                };
                (*id, tl.compact(horizon))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{ByteEdit, Modification, PageVersion};
    use crate::repository::PageLookup;
    use common::{ForkNumber, PageKey, RelTag, PAGE_SIZE};

    fn rel() -> RelTag {
        RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
    }
    fn key(block: u32) -> PageKey {
        PageKey { rel: rel(), block }
    }
    fn tl(n: u8) -> TimelineId {
        TimelineId::from_bytes([n; 16])
    }
    fn image(byte: u8, lsn: u64, block: u32) -> Modification {
        Modification {
            rel: rel(),
            block,
            lsn: Lsn(lsn),
            version: PageVersion::Image(vec![byte; PAGE_SIZE]),
        }
    }
    fn delta(offset: u16, byte: u8, lsn: u64, block: u32) -> Modification {
        Modification {
            rel: rel(),
            block,
            lsn: Lsn(lsn),
            version: PageVersion::Delta(vec![ByteEdit { offset, data: vec![byte] }]),
        }
    }
    fn page(tl: &Timeline, block: u32, lsn: u64) -> Vec<u8> {
        match tl.get_page(key(block), Lsn(lsn)).unwrap() {
            PageLookup::Page(p) => p,
            other => panic!("expected page, got {other:?}"),
        }
    }

    #[test]
    fn branch_inherits_unmodified_pages_from_parent() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(7, 10, 0)]);

        // Branch at LSN 20; the page was last written at 10, so the branch sees it.
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(page(&branch, 0, 100)[0], 7, "branch inherits the parent's page");
    }

    #[test]
    fn branch_writes_are_copy_on_write_and_isolated() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0)]);

        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        // Modify the page only on the branch (a delta over the inherited base).
        branch.ingest([delta(0, 0xAB, 30, 0)]);

        // Branch reflects the change; parent is untouched.
        assert_eq!(page(&branch, 0, 100)[0], 0xAB, "branch sees its own write");
        assert_eq!(page(&main, 0, 100)[0], 0, "parent is unaffected (isolation)");
    }

    #[test]
    fn parent_changes_after_branch_point_are_not_visible() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(1, 10, 0)]);
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();

        // Parent writes again *after* the branch point.
        main.ingest([delta(0, 0x99, 50, 0)]);

        assert_eq!(page(&main, 0, 100)[0], 0x99, "parent advances");
        assert_eq!(page(&branch, 0, 100)[0], 1, "branch is frozen at the branch point");
    }

    #[test]
    fn pitr_time_travel_within_a_timeline() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0), delta(0, 0x11, 20, 0), delta(0, 0x22, 30, 0)]);

        assert_eq!(page(&main, 0, 10)[0], 0, "at LSN 10: base image");
        assert_eq!(page(&main, 0, 25)[0], 0x11, "at LSN 25: first delta only");
        assert_eq!(page(&main, 0, 30)[0], 0x22, "at LSN 30: both deltas");
    }

    #[test]
    fn branch_at_a_past_lsn_sees_history_as_of_that_point() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0), delta(0, 0x11, 20, 0), delta(0, 0x22, 30, 0)]);

        // Branch as of LSN 20 (before the second delta): should see 0x11, not 0x22.
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(page(&branch, 0, 100)[0], 0x11, "branch captures the parent at LSN 20");
    }

    #[test]
    fn multi_level_branches_inherit_through_the_chain() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(5, 10, 0)]);
        let child = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        child.ingest([delta(1, 0x44, 30, 0)]); // modify byte 1 on the child
        let grandchild = tenant.branch_timeline(tl(3), tl(2), Lsn(40)).unwrap();

        // Grandchild inherits byte 0 from main and byte 1 from the child.
        let p = page(&grandchild, 0, 100);
        assert_eq!(p[0], 5, "byte 0 inherited from the grandparent");
        assert_eq!(p[1], 0x44, "byte 1 inherited from the parent branch");
    }

    #[test]
    fn rel_size_inherited_from_ancestor() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0), image(0, 12, 4)]); // 5 blocks by LSN 12
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(branch.get_rel_size(rel(), Lsn(100)), Some(5), "branch inherits the size");

        // Extending the relation on the branch grows it further.
        branch.ingest([image(0, 30, 9)]); // block 9 -> 10 blocks
        assert_eq!(branch.get_rel_size(rel(), Lsn(100)), Some(10));
        assert_eq!(main.get_rel_size(rel(), Lsn(100)), Some(5), "parent size unchanged");
    }

    #[test]
    fn duplicate_and_missing_parent_are_errors() {
        let tenant = Tenant::new(1_000);
        tenant.create_timeline(tl(1)).unwrap();
        assert_eq!(tenant.create_timeline(tl(1)).err(), Some(TenantError::AlreadyExists(tl(1))));
        assert_eq!(
            tenant.branch_timeline(tl(2), tl(9), Lsn(10)).err(),
            Some(TenantError::NoSuchParent(tl(9))),
        );
    }

    #[test]
    fn gc_collapses_main_line_history_without_branches() {
        let tenant = Tenant::new(1); // freeze each write
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([
            image(7, 10, 0),
            delta(0, 0xAA, 20, 0),
            image(1, 30, 0),
            delta(0, 0xBB, 40, 0),
        ]);

        let stats = tenant.gc(Lsn(35));
        let removed: usize = stats.iter().map(|(_, s)| s.versions_removed).sum();
        assert_eq!(removed, 2, "image@10 + delta@20 collapsed");
        assert_eq!(page(&main, 0, 40)[0], 0xBB, "reads within retention unchanged");
    }

    #[test]
    fn gc_respects_branch_pins() {
        let tenant = Tenant::new(1);
        let main = tenant.create_timeline(tl(1)).unwrap();
        // base@10 (all 7s), delta@20, a newer full image@30, delta@40.
        main.ingest([
            image(7, 10, 0),
            delta(1, 0x11, 20, 0),
            image(9, 30, 0),
            delta(0, 0xBB, 40, 0),
        ]);
        // Branch at LSN 15 — between base@10 and delta@20. The branch reads main@15.
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(15)).unwrap();

        // GC at a high horizon. Unpinned, main would drop everything below
        // image@30 and the branch's read at 15 would break. The branch pin lowers
        // main's effective horizon to 15, so image@10 is retained.
        tenant.gc(Lsn(100));

        assert_eq!(page(&branch, 0, 100)[0], 7, "branch still reconstructs main@15");
        // Main itself still serves its own latest state.
        let p = page(&main, 0, 100);
        assert_eq!(p[0], 0xBB);
        assert!(p[2..].iter().all(|&b| b == 9), "image@30 base preserved on main");
    }

    #[test]
    fn page_absent_in_branch_and_ancestor_is_not_found() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0)]);
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(branch.get_page(key(999), Lsn(100)).unwrap(), PageLookup::NotFound);
    }
}
