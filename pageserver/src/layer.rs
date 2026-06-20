// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Immutable layers — the frozen, offloadable unit of the log-structured store.
//!
//! When the in-memory memtable fills, its `(page, lsn) -> version` entries are
//! frozen into a [`Layer`]: a read-only, LSN-sorted snapshot. Layers are what
//! the offload worker serializes into a single file and pushes to object
//! storage. Because a layer is sorted on `(PageKey, Lsn)`, a reconstruction can
//! range-scan it for one page's history in log time.

use std::collections::BTreeMap;

use common::{ForkNumber, Lsn, PageKey, RelTag};

use crate::page::{read_version, write_version, PageError, PageVersion};

/// Monotonic identifier for a layer within a page server's lifetime.
pub type LayerId = u64;

const LAYER_MAGIC: u32 = 0x5350_4C31; // "SPL1"
const LAYER_VERSION: u8 = 1;

/// An immutable, LSN-sorted set of page versions.
#[derive(Debug, Clone)]
pub struct Layer {
    id: LayerId,
    entries: BTreeMap<(PageKey, Lsn), PageVersion>,
}

impl Layer {
    /// Build a layer from a sorted map of entries.
    pub fn new(id: LayerId, entries: BTreeMap<(PageKey, Lsn), PageVersion>) -> Self {
        Layer { id, entries }
    }

    /// This layer's id.
    pub fn id(&self) -> LayerId {
        self.id
    }

    /// Number of versions held.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the layer holds no versions.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// All `(page, lsn) -> version` entries, for compaction/merge.
    pub(crate) fn entries(&self) -> &BTreeMap<(PageKey, Lsn), PageVersion> {
        &self.entries
    }

    /// Iterate the versions of `key` with LSN in `[0, lsn]`, ascending.
    pub fn range(&self, key: PageKey, lsn: Lsn) -> impl Iterator<Item = (Lsn, &PageVersion)> {
        self.entries
            .range((key, Lsn::INVALID)..=(key, lsn))
            .map(|((_, l), v)| (*l, v))
    }

    /// Serialize the whole layer to a single byte buffer (the offload format).
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&LAYER_MAGIC.to_be_bytes());
        buf.push(LAYER_VERSION);
        buf.extend_from_slice(&self.id.to_be_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for ((key, lsn), version) in &self.entries {
            buf.extend_from_slice(&key.rel.spc_node.to_be_bytes());
            buf.extend_from_slice(&key.rel.db_node.to_be_bytes());
            buf.extend_from_slice(&key.rel.rel_node.to_be_bytes());
            buf.push(key.rel.fork as u8);
            buf.extend_from_slice(&key.block.to_be_bytes());
            buf.extend_from_slice(&lsn.raw().to_be_bytes());
            write_version(&mut buf, version);
        }
        buf
    }

    /// Deserialize a layer from a buffer produced by [`Layer::serialize`].
    pub fn deserialize(buf: &[u8]) -> Result<Layer, PageError> {
        let mut pos = 0usize;
        let read_u32 = |buf: &[u8], pos: &mut usize| -> Result<u32, PageError> {
            if buf.len() < *pos + 4 {
                return Err(PageError::Truncated("u32"));
            }
            let v = u32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
            *pos += 4;
            Ok(v)
        };
        let read_u64 = |buf: &[u8], pos: &mut usize| -> Result<u64, PageError> {
            if buf.len() < *pos + 8 {
                return Err(PageError::Truncated("u64"));
            }
            let mut a = [0u8; 8];
            a.copy_from_slice(&buf[*pos..*pos + 8]);
            *pos += 8;
            Ok(u64::from_be_bytes(a))
        };

        if read_u32(buf, &mut pos)? != LAYER_MAGIC {
            return Err(PageError::Truncated("layer magic"));
        }
        let ver = *buf.get(pos).ok_or(PageError::Truncated("layer version"))?;
        pos += 1;
        if ver != LAYER_VERSION {
            return Err(PageError::BadVersionTag(ver));
        }
        let id = read_u64(buf, &mut pos)?;
        let count = read_u32(buf, &mut pos)? as usize;

        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let spc_node = read_u32(buf, &mut pos)?;
            let db_node = read_u32(buf, &mut pos)?;
            let rel_node = read_u32(buf, &mut pos)?;
            let fork_raw = *buf.get(pos).ok_or(PageError::Truncated("fork"))?;
            pos += 1;
            let fork = ForkNumber::from_raw(fork_raw).ok_or(PageError::Truncated("fork"))?;
            let block = read_u32(buf, &mut pos)?;
            let lsn = Lsn(read_u64(buf, &mut pos)?);
            let (version, new_pos) = read_version(buf, pos)?;
            pos = new_pos;
            let key = PageKey { rel: RelTag { spc_node, db_node, rel_node, fork }, block };
            entries.insert((key, lsn), version);
        }
        Ok(Layer { id, entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::ByteEdit;

    fn key(block: u32) -> PageKey {
        PageKey {
            rel: RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main },
            block,
        }
    }

    #[test]
    fn layer_serialization_round_trips() {
        let mut entries = BTreeMap::new();
        entries.insert((key(0), Lsn(10)), PageVersion::Image(vec![1u8; common::PAGE_SIZE]));
        entries.insert(
            (key(0), Lsn(20)),
            PageVersion::Delta(vec![ByteEdit { offset: 5, data: vec![9, 9] }]),
        );
        entries.insert((key(1), Lsn(15)), PageVersion::Image(vec![2u8; common::PAGE_SIZE]));
        let layer = Layer::new(42, entries.clone());

        let bytes = layer.serialize();
        let back = Layer::deserialize(&bytes).unwrap();
        assert_eq!(back.id(), 42);
        assert_eq!(back.len(), 3);

        // The deserialized layer answers a range scan identically.
        let versions: Vec<_> = back.range(key(0), Lsn(100)).map(|(l, v)| (l, v.clone())).collect();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].0, Lsn(10));
        assert!(versions[0].1.is_image());
        assert_eq!(versions[1].0, Lsn(20));
    }
}
