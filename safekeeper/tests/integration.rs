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

use common::wal_service::{AppendRequest, AppendResponse, RESPONSE_LEN, STATUS_OK};
use common::{Lsn, TenantId, TimelineId};
use safekeeper::consensus::Consensus;
use safekeeper::replicator::LocalSimReplicator;
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
