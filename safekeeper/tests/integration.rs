// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! End-to-end tests for the safekeeper ingest server.
//!
//! Drives the server over a real socket: streams WAL appends, checks that
//! flush/commit LSNs advance under the simulated quorum, and then reopens the
//! durable store to prove the bytes survived.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::wal_service::{
    AppendRequest, AppendResponse, ReadRequest, ReadResponse, READ_RESPONSE_HEADER_LEN,
    RESPONSE_LEN, STATUS_OK,
};
use common::{Lsn, TenantId, TimelineId};
use safekeeper::consensus::Consensus;
use safekeeper::replicator::{LocalSimReplicator, NetworkReplicator};
use safekeeper::server::{serve, Safekeeper};
use safekeeper::storage::{WalConfig, WalStorage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Unique temp dir cleaned up on drop.
struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("sp-sk-it-{}-{}", tag, std::process::id()));
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

fn wal_cfg(dir: &TempDir) -> WalConfig {
    WalConfig { data_dir: dir.0.clone(), segment_size: 4096, ring_capacity: 4096 }
}

async fn spawn_safekeeper(sk: Arc<Safekeeper>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(sk, listener).await;
    });
    addr
}

fn append(start: u64, payload: &[u8]) -> AppendRequest {
    AppendRequest {
        tenant: TenantId::ZERO,
        timeline: TimelineId::ZERO,
        term: 1,
        start_lsn: Lsn(start),
        payload: payload.to_vec(),
    }
}

async fn send_append(stream: &mut TcpStream, req: &AppendRequest) -> AppendResponse {
    stream.write_all(&req.encode()).await.unwrap();
    let mut resp = vec![0u8; RESPONSE_LEN];
    stream.read_exact(&mut resp).await.unwrap();
    AppendResponse::decode(&resp).unwrap()
}

async fn send_read(stream: &mut TcpStream, start: u64, max: u32) -> ReadResponse {
    let req = ReadRequest {
        tenant: TenantId::ZERO,
        timeline: TimelineId::ZERO,
        start_lsn: Lsn(start),
        max_bytes: max,
    };
    stream.write_all(&req.encode()).await.unwrap();
    let mut full = vec![0u8; READ_RESPONSE_HEADER_LEN];
    stream.read_exact(&mut full).await.unwrap();
    let plen = ReadResponse::payload_len(&full).unwrap();
    full.resize(READ_RESPONSE_HEADER_LEN + plen, 0);
    stream.read_exact(&mut full[READ_RESPONSE_HEADER_LEN..]).await.unwrap();
    ReadResponse::decode(&full).unwrap()
}

#[tokio::test]
async fn ingest_advances_flush_and_commit_under_quorum() {
    let dir = TempDir::new("quorum");
    // 3-member group; peers 2 and 3 are simulated as instantly durable, so a
    // single real node still reaches the 2-of-3 quorum.
    let storage = WalStorage::open(wal_cfg(&dir)).unwrap();
    let consensus = Consensus::new(1, vec![1, 2, 3]);
    let replicator = Arc::new(LocalSimReplicator::new(vec![2, 3]));
    let sk = Safekeeper::new(storage, consensus, replicator);
    let addr = spawn_safekeeper(sk).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(addr).await.unwrap();

        let r1 = send_append(&mut stream, &append(0, b"first-wal-run")).await;
        assert_eq!(r1.status, STATUS_OK);
        assert_eq!(r1.flush_lsn, Lsn(13));
        assert_eq!(r1.commit_lsn, Lsn(13), "quorum should commit the run");

        let r2 = send_append(&mut stream, &append(13, b"second")).await;
        assert_eq!(r2.flush_lsn, Lsn(19));
        assert_eq!(r2.commit_lsn, Lsn(19));
    })
    .await;
    assert!(outcome.is_ok(), "ingest test timed out");

    // Prove durability: reopen the store and read the streamed WAL back.
    let store = WalStorage::open(wal_cfg(&dir)).unwrap();
    assert_eq!(store.flush_lsn(), Lsn(19));
    let mut buf = [0u8; 19];
    store.read_at(Lsn(0), &mut buf).unwrap();
    assert_eq!(&buf, b"first-wal-runsecond");
}

#[tokio::test]
async fn lone_safekeeper_commits_on_its_own() {
    let dir = TempDir::new("solo");
    // A single-member group: quorum is 1, so the local flush is the commit.
    let storage = WalStorage::open(wal_cfg(&dir)).unwrap();
    let consensus = Consensus::new(1, vec![1]);
    let replicator = Arc::new(LocalSimReplicator::new(vec![]));
    let sk = Safekeeper::new(storage, consensus, replicator);
    let addr = spawn_safekeeper(sk).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let r = send_append(&mut stream, &append(0, b"solo")).await;
        assert_eq!(r.commit_lsn, Lsn(4));
    })
    .await;
    assert!(outcome.is_ok(), "solo test timed out");
}

#[tokio::test]
async fn real_replication_commits_on_quorum_and_is_durable_on_peers() {
    // Two acceptor safekeepers (nodes 2 and 3), each on its own store.
    let dir2 = TempDir::new("peer2");
    let dir3 = TempDir::new("peer3");
    let peer2 = Safekeeper::new(
        WalStorage::open(wal_cfg(&dir2)).unwrap(),
        Consensus::new(2, vec![1, 2, 3]),
        Arc::new(LocalSimReplicator::new(vec![])),
    );
    let peer3 = Safekeeper::new(
        WalStorage::open(wal_cfg(&dir3)).unwrap(),
        Consensus::new(3, vec![1, 2, 3]),
        Arc::new(LocalSimReplicator::new(vec![])),
    );
    let addr2 = spawn_safekeeper(peer2).await;
    let addr3 = spawn_safekeeper(peer3).await;

    // The leader (node 1) replicates to the two peers over the network. Quorum
    // is 2 of 3, so the leader plus one peer suffices.
    let dir1 = TempDir::new("leader");
    let leader = Safekeeper::new(
        WalStorage::open(wal_cfg(&dir1)).unwrap(),
        Consensus::new(1, vec![1, 2, 3]),
        Arc::new(NetworkReplicator::new(vec![(2, addr2), (3, addr3)])),
    );
    let leader_addr = spawn_safekeeper(leader).await;

    let payload = b"replicated-wal-run";
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(leader_addr).await.unwrap();
        let r = send_append(&mut stream, &append(0, payload)).await;
        assert_eq!(r.status, STATUS_OK);
        assert_eq!(
            r.commit_lsn,
            Lsn(payload.len() as u64),
            "the run commits once a quorum of safekeepers has it",
        );
    })
    .await;
    assert!(outcome.is_ok(), "replication test timed out");

    // The bytes physically reached both peers and are durable on their disks.
    for dir in [&dir2, &dir3] {
        let store = WalStorage::open(wal_cfg(dir)).unwrap();
        assert_eq!(store.flush_lsn(), Lsn(payload.len() as u64), "peer flushed the run");
        let mut buf = vec![0u8; payload.len()];
        store.read_at(Lsn(0), &mut buf).unwrap();
        assert_eq!(&buf, payload, "peer durably stored the replicated WAL");
    }
}

#[tokio::test]
async fn commit_advances_with_one_peer_down() {
    // Only one peer is up; with quorum 2 of 3, the leader plus that one peer
    // still commits. The second peer's address points nowhere.
    let dir2 = TempDir::new("up-peer");
    let peer2 = Safekeeper::new(
        WalStorage::open(wal_cfg(&dir2)).unwrap(),
        Consensus::new(2, vec![1, 2, 3]),
        Arc::new(LocalSimReplicator::new(vec![])),
    );
    let addr2 = spawn_safekeeper(peer2).await;
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap(); // unroutable peer 3

    let dir1 = TempDir::new("leader2");
    let leader = Safekeeper::new(
        WalStorage::open(wal_cfg(&dir1)).unwrap(),
        Consensus::new(1, vec![1, 2, 3]),
        Arc::new(NetworkReplicator::new(vec![(2, addr2), (3, dead)])),
    );
    let leader_addr = spawn_safekeeper(leader).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(leader_addr).await.unwrap();
        let r = send_append(&mut stream, &append(0, b"still-commits")).await;
        assert_eq!(r.commit_lsn, Lsn("still-commits".len() as u64), "quorum reached without peer 3");
    })
    .await;
    assert!(outcome.is_ok(), "one-peer-down test timed out");
}

#[tokio::test]
async fn read_back_returns_committed_wal() {
    let dir = TempDir::new("readback");
    let storage = WalStorage::open(wal_cfg(&dir)).unwrap();
    let consensus = Consensus::new(1, vec![1]);
    let replicator = Arc::new(LocalSimReplicator::new(vec![]));
    let sk = Safekeeper::new(storage, consensus, replicator);
    let addr = spawn_safekeeper(sk).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Commit 19 bytes of WAL.
        let a = send_append(&mut stream, &append(0, b"first-wal-runsecond")).await;
        assert_eq!(a.commit_lsn, Lsn(19));

        // Full read from the start.
        let r = send_read(&mut stream, 0, 1000).await;
        assert_eq!(r.status, STATUS_OK);
        assert_eq!(r.commit_lsn, Lsn(19));
        assert_eq!(r.start_lsn, Lsn(0));
        assert_eq!(r.payload, b"first-wal-runsecond");

        // Read from a cursor partway through.
        let r2 = send_read(&mut stream, 13, 1000).await;
        assert_eq!(r2.start_lsn, Lsn(13));
        assert_eq!(r2.payload, b"second");

        // Caught up: empty payload, but the commit position is still reported.
        let r3 = send_read(&mut stream, 19, 1000).await;
        assert!(r3.payload.is_empty());
        assert_eq!(r3.commit_lsn, Lsn(19));

        // max_bytes caps the chunk size.
        let r4 = send_read(&mut stream, 0, 5).await;
        assert_eq!(r4.payload, b"first");
    })
    .await;
    assert!(outcome.is_ok(), "read-back test timed out");
}
