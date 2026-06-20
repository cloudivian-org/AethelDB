// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A **timeline** — one branch of a tenant's page history.
//!
//! Every tenant has a root timeline; a *branch* creates a child timeline that
//! shares all of its parent's history up to a **branch point** (`ancestor_lsn`)
//! and diverges after it. This is the headline "instant branching" feature: a
//! branch allocates only a new [`TimelineId`] and an empty store — no data is
//! copied.
//!
//! Each timeline owns a [`Repository`] holding just the changes written *on this
//! branch*. Reconstruction is **copy-on-write**: to read page `K` at LSN `Y`,
//!
//! 1. if this timeline has its own self-sufficient history for `K` (a full image
//!    or `will_init` record at or before `Y`), reconstruct from it; otherwise
//! 2. inherit the page from the parent **at the branch point** and replay this
//!    timeline's deltas on top.
//!
//! Reads recurse up the ancestor chain, so a grandchild inherits from its
//! grandparent transparently. Writing to a branch never affects its parent.

use std::sync::Arc;

use common::{Lsn, PageKey, RelTag, TimelineId};

use crate::page::{Modification, PageError};
use crate::repository::{CompactionStats, PageLookup, Repository};
use crate::waldecode::WalDecodeError;

/// The parent of a branched timeline and the LSN at which it diverged.
struct Ancestor {
    parent: Arc<Timeline>,
    ancestor_lsn: Lsn,
}

/// One branch of a tenant's history: its own page store plus an optional
/// ancestor it inherits unchanged pages from.
pub struct Timeline {
    id: TimelineId,
    repo: Arc<Repository>,
    ancestor: Option<Ancestor>,
}

impl Timeline {
    /// A root timeline with no ancestor.
    pub(crate) fn root(id: TimelineId, repo: Arc<Repository>) -> Arc<Timeline> {
        Arc::new(Timeline { id, repo, ancestor: None })
    }

    /// A timeline branched from `parent` at `ancestor_lsn`.
    pub(crate) fn branched(
        id: TimelineId,
        repo: Arc<Repository>,
        parent: Arc<Timeline>,
        ancestor_lsn: Lsn,
    ) -> Arc<Timeline> {
        Arc::new(Timeline { id, repo, ancestor: Some(Ancestor { parent, ancestor_lsn }) })
    }

    /// This timeline's identifier.
    pub fn id(&self) -> TimelineId {
        self.id
    }

    /// The parent timeline this branch diverged from, if any.
    pub fn ancestor_timeline(&self) -> Option<TimelineId> {
        self.ancestor.as_ref().map(|a| a.parent.id)
    }

    /// The branch-point LSN, if this is a branch.
    pub fn ancestor_lsn(&self) -> Option<Lsn> {
        self.ancestor.as_ref().map(|a| a.ancestor_lsn)
    }

    /// This branch's own page store (used by the offload worker).
    pub fn repository(&self) -> Arc<Repository> {
        self.repo.clone()
    }

    /// Ingest pre-decoded modifications into this branch's store.
    pub fn ingest<I: IntoIterator<Item = Modification>>(&self, mods: I) {
        self.repo.ingest(mods);
    }

    /// Ingest a raw WAL byte stream into this branch's store.
    pub fn ingest_wal(&self, start_lsn: Lsn, wal: &[u8]) -> Result<usize, WalDecodeError> {
        self.repo.ingest_wal(start_lsn, wal)
    }

    /// Ingest one already-framed WAL record into this branch's store.
    pub fn ingest_record(&self, lsn: Lsn, record: &[u8]) -> Result<(), WalDecodeError> {
        self.repo.ingest_record(lsn, record)
    }

    /// Freeze this branch's memtable (test/diagnostic helper).
    pub fn freeze(&self) {
        self.repo.freeze();
    }

    /// Compact + GC this branch's frozen layers at `gc_horizon`. The caller (the
    /// tenant) is responsible for choosing a horizon that respects any child
    /// branch's dependency on this timeline's history.
    pub fn compact(&self, gc_horizon: Lsn) -> CompactionStats {
        self.repo.compact(gc_horizon)
    }

    /// Reconstruct page `key` at `lsn` on this branch, inheriting from the
    /// ancestor chain where this branch hasn't modified the page.
    pub fn get_page(&self, key: PageKey, lsn: Lsn) -> Result<PageLookup, PageError> {
        let ancestor = match &self.ancestor {
            None => return self.repo.get_page(key, lsn),
            Some(a) => a,
        };

        // Self-sufficient on this branch (own full image / will_init)?
        if let PageLookup::Page(page) = self.repo.get_page(key, lsn)? {
            return Ok(PageLookup::Page(page));
        }

        // Otherwise inherit the page from the parent at the branch point and
        // replay this branch's deltas over it (copy-on-write).
        match ancestor.parent.get_page(key, ancestor.ancestor_lsn)? {
            PageLookup::Page(base) => {
                self.repo.get_page_with_base(key, lsn, Some((ancestor.ancestor_lsn, base)))
            }
            PageLookup::NotFound => Ok(PageLookup::NotFound),
        }
    }

    /// Relation fork size at `lsn`: the larger of this branch's own size and the
    /// size inherited from the ancestor at the branch point.
    pub fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Option<u32> {
        let own = self.repo.get_rel_size(rel, lsn);
        match &self.ancestor {
            None => own,
            Some(anc) => {
                let inherited = anc.parent.get_rel_size(rel, anc.ancestor_lsn);
                match (own, inherited) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                }
            }
        }
    }
}
