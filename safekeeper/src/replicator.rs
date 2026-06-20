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

use std::collections::HashMap;
use std::net::SocketAddr;

use async_trait::async_trait;
use common::wal_service::{AppendRequest, AppendResponse, RESPONSE_LEN, STATUS_OK};
use common::{Lsn, TenantId, TimelineId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::warn;

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

/// A replicator that streams WAL to real peer safekeepers over the network.
///
/// For each WAL run it sends a `TYPE_REPLICATE` message (the same bytes the
/// leader stored) to every peer and collects their flush acknowledgements; the
/// consensus layer turns those into the commit LSN. Peer connections are kept
/// open and reconnected on failure. A peer that is down simply contributes no
/// ack, so the run still commits as long as a quorum responds.
pub struct NetworkReplicator {
    peers: Vec<(NodeId, SocketAddr)>,
    conns: Mutex<HashMap<NodeId, TcpStream>>,
    tenant: TenantId,
    timeline: TimelineId,
}

impl NetworkReplicator {
    /// Replicate to `peers` (their node ids and addresses), for the single
    /// tenant/timeline this safekeeper group serves.
    pub fn new(peers: Vec<(NodeId, SocketAddr)>) -> Self {
        NetworkReplicator {
            peers,
            conns: Mutex::new(HashMap::new()),
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
        }
    }

    /// Send the replicate message to one peer over a kept-open connection,
    /// returning its flush LSN.
    async fn send_to_peer(
        conns: &mut HashMap<NodeId, TcpStream>,
        node: NodeId,
        addr: SocketAddr,
        bytes: &[u8],
    ) -> anyhow::Result<Lsn> {
        if !conns.contains_key(&node) {
            let stream = TcpStream::connect(addr).await?;
            let _ = stream.set_nodelay(true);
            conns.insert(node, stream);
        }
        let stream = conns.get_mut(&node).expect("just inserted");
        stream.write_all(bytes).await?;
        let mut resp = vec![0u8; RESPONSE_LEN];
        stream.read_exact(&mut resp).await?;
        let resp = AppendResponse::decode(&resp)?;
        if resp.status != STATUS_OK {
            anyhow::bail!("peer {node} rejected replicate (status {})", resp.status);
        }
        Ok(resp.flush_lsn)
    }
}

#[async_trait]
impl Replicator for NetworkReplicator {
    async fn replicate(&self, term: u64, end_lsn: Lsn, data: &[u8]) -> Vec<PeerAck> {
        let start_lsn = Lsn(end_lsn.raw().saturating_sub(data.len() as u64));
        let req = AppendRequest {
            tenant: self.tenant,
            timeline: self.timeline,
            term,
            start_lsn,
            payload: data.to_vec(),
        };
        let bytes = req.encode_replicate();

        let mut acks = Vec::new();
        let mut conns = self.conns.lock().await;
        for (node, addr) in &self.peers {
            match Self::send_to_peer(&mut conns, *node, *addr, &bytes).await {
                Ok(flush) => acks.push(PeerAck { node: *node, lsn: flush }),
                Err(e) => {
                    warn!(node, %addr, error = %format!("{e:#}"), "replicate to peer failed");
                    conns.remove(node); // force a reconnect next time
                }
            }
        }
        acks
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
