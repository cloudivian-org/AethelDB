// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Replication of WAL to peer safekeepers.
//!
//! After a safekeeper durably appends a WAL run locally, it must get the same
//! bytes onto a quorum of peers before the position can be committed. How that
//! happens — streaming over the network to real peer processes — is behind the
//! [`Replicator`] trait so the server logic stays the same whether peers are
//! real or simulated.
//!
//! [`LocalSimReplicator`] models a single-process dev/test deployment: it
//! reports that every peer instantly accepted the bytes up to the run's end
//! LSN. That makes a lone safekeeper reach quorum on its own, while keeping the
//! real quorum arithmetic in [`crate::consensus`] exercised end to end.

use async_trait::async_trait;

use common::Lsn;

use crate::consensus::NodeId;

/// One peer's acknowledgement: it has durably flushed up to `lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerAck {
    pub node: NodeId,
    pub lsn: Lsn,
}

/// Streams WAL to peer safekeepers and collects their flush acknowledgements.
#[async_trait]
pub trait Replicator: Send + Sync {
    /// Replicate `data` (which ends at `end_lsn`) to the peers in `term` and
    /// return whatever acknowledgements arrived.
    async fn replicate(&self, term: u64, end_lsn: Lsn, data: &[u8]) -> Vec<PeerAck>;
}

/// A replicator that simulates instantly-durable peers (single-process dev).
#[derive(Debug, Clone, Default)]
pub struct LocalSimReplicator {
    peers: Vec<NodeId>,
}

impl LocalSimReplicator {
    /// Create a simulator for the given peer ids (excluding this node).
    pub fn new(peers: Vec<NodeId>) -> Self {
        LocalSimReplicator { peers }
    }
}

#[async_trait]
impl Replicator for LocalSimReplicator {
    async fn replicate(&self, _term: u64, end_lsn: Lsn, _data: &[u8]) -> Vec<PeerAck> {
        self.peers.iter().map(|&node| PeerAck { node, lsn: end_lsn }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sim_replicator_acks_all_peers_at_end_lsn() {
        let r = LocalSimReplicator::new(vec![2, 3]);
        let acks = r.replicate(1, Lsn(500), b"data").await;
        assert_eq!(acks.len(), 2);
        assert!(acks.iter().all(|a| a.lsn == Lsn(500)));
    }
}
