// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! End-to-end test of the safekeeper → page-server WAL link (Phase 4).
//!
//! Spins up a real safekeeper, streams a format-accurate PostgreSQL WAL record
//! (a full-page image) into it as a compute node would, then runs the page
//! server's [`WalReceiver`] against it and asserts the page materializes in the
//! repository — exercising the read protocol, framing, decode, and store in one
//! live pipeline over real sockets.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::wal_service::{AppendRequest, AppendResponse, RESPONSE_LEN, STATUS_OK};
use common::{ForkNumber, Lsn, PageKey, RelTag, TenantId, TimelineId};
use pageserver::waldecode::{SIZE_OF_XLOG_LONG_PHD, XLOG_PAGE_MAGIC_PG16};
use pageserver::{PageLookup, Tenant, WalReceiver, WalReceiverConfig};
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
        let p = std::env::temp_dir().join(format!("sp-wr-it-{}-{}", tag, std::process::id()));
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

fn rel() -> RelTag {
    RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
}

/// Build a one-page WAL stream (long header + a single full-page-image record
/// for block 0 of `rel()`). The image surrounds an all-zero hole so it stays
/// small; the 8 stored bytes are `[1..=8]`.
fn wal_with_fpi() -> Vec<u8> {
    // Long page header at LSN 0: magic + XLP_LONG_HEADER, remainder zeroed.
    let mut page = vec![0u8; SIZE_OF_XLOG_LONG_PHD];
    page[0..2].copy_from_slice(&XLOG_PAGE_MAGIC_PG16.to_le_bytes());
    page[2..4].copy_from_slice(&0x0002u16.to_le_bytes()); // XLP_LONG_HEADER

    let stored: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    let hole_offset: u16 = 4;

    // Record body: one block header with a full-page image, then the image.
    let mut body = Vec::new();
    body.push(0u8); // block_id
    body.push(0x10 | (ForkNumber::Main as u8)); // BKPBLOCK_HAS_IMAGE | fork
    body.extend_from_slice(&0u16.to_le_bytes()); // data_len
    body.extend_from_slice(&(stored.len() as u16).to_le_bytes()); // bimg_len
    body.extend_from_slice(&hole_offset.to_le_bytes()); // hole_offset
    body.push(0x01 | 0x02); // BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY (uncompressed)
    body.extend_from_slice(&rel().spc_node.to_le_bytes());
    body.extend_from_slice(&rel().db_node.to_le_bytes());
    body.extend_from_slice(&rel().rel_node.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // blkno
    body.push(255u8); // XLR_BLOCK_ID_DATA_SHORT
    body.push(0u8); // main_data_len = 0
    body.extend_from_slice(&stored); // image bytes

    // XLogRecord header (24 bytes) with a valid xl_tot_len.
    let tot = 24 + body.len();
    let mut rec = Vec::with_capacity(tot);
    rec.extend_from_slice(&(tot as u32).to_le_bytes()); // xl_tot_len
    rec.extend_from_slice(&0u32.to_le_bytes()); // xl_xid
    rec.extend_from_slice(&0u64.to_le_bytes()); // xl_prev
    rec.push(0); // xl_info
    rec.push(10); // xl_rmid
    rec.extend_from_slice(&[0, 0]); // padding
    rec.extend_from_slice(&0u32.to_le_bytes()); // xl_crc
    rec.extend_from_slice(&body);

    page.extend_from_slice(&rec);
    page
}

async fn spawn_safekeeper(dir: &TempDir) -> SocketAddr {
    let cfg = WalConfig { data_dir: dir.0.clone(), segment_size: 1 << 20, ring_capacity: 1 << 20 };
    let storage = WalStorage::open(cfg).unwrap();
    let consensus = Consensus::new(1, vec![1]); // solo quorum: local flush commits
    let replicator = Arc::new(LocalSimReplicator::new(vec![]));
    let sk = Safekeeper::new(storage, consensus, replicator);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(sk, listener).await;
    });
    addr
}

/// Stream a WAL run into the safekeeper as compute would, returning the commit LSN.
async fn append_wal(addr: SocketAddr, payload: &[u8]) -> Lsn {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = AppendRequest {
        tenant: TenantId::ZERO,
        timeline: TimelineId::ZERO,
        term: 1,
        start_lsn: Lsn(0),
        payload: payload.to_vec(),
    };
    stream.write_all(&req.encode()).await.unwrap();
    let mut resp = vec![0u8; RESPONSE_LEN];
    stream.read_exact(&mut resp).await.unwrap();
    let resp = AppendResponse::decode(&resp).unwrap();
    assert_eq!(resp.status, STATUS_OK);
    resp.commit_lsn
}

#[tokio::test]
async fn wal_streams_from_safekeeper_into_the_page_store() {
    let dir = TempDir::new("e2e");
    let addr = spawn_safekeeper(&dir).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // Compute streams a WAL record (a full-page image) to the safekeeper.
        let wal = wal_with_fpi();
        let commit = append_wal(addr, &wal).await;
        assert_eq!(commit, Lsn(wal.len() as u64), "the whole run should commit");

        // The page server's receiver pulls it back and ingests it into the
        // tenant's root timeline.
        let tenant = Tenant::new(1_000);
        let root = tenant.create_timeline(TimelineId::ZERO).unwrap();
        let cfg = WalReceiverConfig::new(addr, TenantId::ZERO, TimelineId::ZERO, Lsn(0));
        let mut receiver = WalReceiver::connect(root.clone(), cfg).await.unwrap();

        let ingested = receiver.poll_once().await.unwrap();
        assert_eq!(ingested, 1, "one WAL record should be decoded and ingested");
        assert_eq!(receiver.cursor(), commit, "cursor advances to the commit LSN");

        // The page materializes exactly as the WAL described it.
        match root.get_page(PageKey { rel: rel(), block: 0 }, Lsn(commit.raw())).unwrap() {
            PageLookup::Page(p) => {
                assert_eq!(p.len(), 8192);
                assert_eq!(&p[0..4], &[1, 2, 3, 4]); // before the hole
                assert_eq!(&p[8188..8192], &[5, 6, 7, 8]); // after the hole
                assert!(p[4..8188].iter().all(|&b| b == 0)); // the hole
            }
            other => panic!("expected reconstructed page, got {other:?}"),
        }

        // Once caught up, the receiver reports nothing new.
        assert_eq!(receiver.poll_once().await.unwrap(), 0);
    })
    .await;
    assert!(outcome.is_ok(), "end-to-end WAL receiver test timed out");
}
