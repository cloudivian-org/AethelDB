// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Addressing a single 8 KiB page.
//!
//! When the patched compute node (Step 3) needs a page, it does not name a file
//! and offset — it asks the pageserver for a logical page identity. That
//! identity is a [`PageKey`]: the relation it belongs to, which fork of that
//! relation, and the block number within the fork. The pageserver then
//! reconstructs the page contents at a requested [`crate::Lsn`].

use serde::{Deserialize, Serialize};

/// PostgreSQL's default block size. The whole system is built around 8 KiB pages.
pub const PAGE_SIZE: usize = 8192;

/// Which physical fork of a relation a page belongs to.
///
/// Mirrors PostgreSQL's `ForkNumber` enum. The main fork holds the actual row
/// data; the others are auxiliary maps the storage manager must also serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ForkNumber {
    /// Heap / index data — the relation proper.
    Main = 0,
    /// Free Space Map.
    Fsm = 1,
    /// Visibility Map.
    VisibilityMap = 2,
    /// Initialization fork for unlogged relations.
    Init = 3,
}

impl ForkNumber {
    /// Map PostgreSQL's on-the-wire integer fork number to the typed variant.
    pub fn from_raw(raw: u8) -> Option<ForkNumber> {
        match raw {
            0 => Some(ForkNumber::Main),
            1 => Some(ForkNumber::Fsm),
            2 => Some(ForkNumber::VisibilityMap),
            3 => Some(ForkNumber::Init),
            _ => None,
        }
    }
}

/// Identifies a relation file, matching PostgreSQL's `RelFileNode`.
///
/// The triple `(spcNode, dbNode, relNode)` is globally unique within a cluster:
/// tablespace OID, database OID, and the relation's own file-node OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RelTag {
    /// Tablespace OID.
    pub spc_node: u32,
    /// Database OID.
    pub db_node: u32,
    /// Relation file-node OID.
    pub rel_node: u32,
    /// Which fork of the relation.
    pub fork: ForkNumber,
}

/// The full key for one 8 KiB page: a relation/fork plus a block number.
///
/// This is deliberately *LSN-free*. A `PageKey` names a page's identity; the LSN
/// at which you want to observe that page is supplied separately at read time,
/// which is what lets the pageserver serve any historical version of the page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageKey {
    /// The relation and fork this page lives in.
    pub rel: RelTag,
    /// Zero-based block index within the fork.
    pub block: u32,
}

impl PageKey {
    /// Convenience constructor.
    pub fn new(rel: RelTag, block: u32) -> Self {
        PageKey { rel, block }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_number_round_trips() {
        for raw in 0..=3u8 {
            let fork = ForkNumber::from_raw(raw).unwrap();
            assert_eq!(fork as u8, raw);
        }
        assert!(ForkNumber::from_raw(99).is_none());
    }

    #[test]
    fn page_keys_are_orderable_and_hashable() {
        use std::collections::BTreeSet;
        let rel = RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main };
        let a = PageKey::new(rel, 0);
        let b = PageKey::new(rel, 1);
        assert!(a < b);
        let set: BTreeSet<_> = [b, a].into_iter().collect();
        // BTreeSet must order them by block number.
        assert_eq!(set.into_iter().collect::<Vec<_>>(), vec![a, b]);
    }
}
