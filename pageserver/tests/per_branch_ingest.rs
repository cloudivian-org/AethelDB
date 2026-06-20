// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Per-branch network WAL ingest: a timeline is created and told to stream WAL
//! from a safekeeper via the control endpoint, and the page materializes in
//! that timeline — all over real sockets.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::wal_service::{AppendRequest, AppendResponse, RESPONSE_LEN, STATUS_OK};
use common::{ForkNumber, Lsn, PageKey, RelTag, TenantId, TimelineId};
use pageserver::tenant::Tenant;
use pageserver::waldecode::{SIZE_OF_XLOG_LONG_PHD, XLOG_PAGE_MAGIC_PG16};
use pageserver::{serve_control, PageLookup};
use safekeeper::consensus::Consensus;
use safekeeper::replicator::LocalSimReplicator;
use safekeeper::server::{serve, Safekeeper};
use safekeeper::storage::{WalConfig, WalStorage};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

fn rel() -> RelTag {
    RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
}

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("sp-pbi-{}-{}", tag, std::process::id()));
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

/// One-page WAL stream: long header + a full-page-image record for block 0.
fn wal_with_fpi() -> Vec<u8> {
    let mut page = vec![0u8; SIZE_OF_XLOG_LONG_PHD];
    page[0..2].copy_from_slice(&XLOG_PAGE_MAGIC_PG16.to_le_bytes());
    page[2..4].copy_from_slice(&0x0002u16.to_le_bytes()); // XLP_LONG_HEADER

    let stored: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    let hole_offset: u16 = 4;
    let mut body = Vec::new();
    body.push(0u8);
    body.push(0x10 | (ForkNumber::Main as u8)); // HAS_IMAGE | fork
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&(stored.len() as u16).to_le_bytes());
    body.extend_from_slice(&hole_offset.to_le_bytes());
    body.push(0x01 | 0x02); // HAS_HOLE | APPLY
    body.extend_from_slice(&rel().spc_node.to_le_bytes());
    body.extend_from_slice(&rel().db_node.to_le_bytes());
    body.extend_from_slice(&rel().rel_node.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // blkno
    body.push(255u8); // DATA_SHORT
    body.push(0u8);
    body.extend_from_slice(&stored);

    let tot = 24 + body.len();
    let mut rec = Vec::with_capacity(tot);
    rec.extend_from_slice(&(tot as u32).to_le_bytes());
    rec.extend_from_slice(&0u32.to_le_bytes());
    rec.extend_from_slice(&0u64.to_le_bytes());
    rec.push(0);
    rec.push(10);
    rec.extend_from_slice(&[0, 0]);
    rec.extend_from_slice(&0u32.to_le_bytes());
    rec.extend_from_slice(&body);
    page.extend_from_slice(&rec);
    page
}

async fn spawn_safekeeper(dir: &TempDir) -> SocketAddr {
    let cfg = WalConfig { data_dir: dir.0.clone(), segment_size: 1 << 20, ring_capacity: 1 << 20 };
    let sk = Safekeeper::new(
        WalStorage::open(cfg).unwrap(),
        Consensus::new(1, vec![1]),
        Arc::new(LocalSimReplicator::new(vec![])),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(sk, listener).await;
    });
    addr
}

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

async fn control(stream: &mut BufReader<TcpStream>, cmd: &str) -> String {
    stream.get_mut().write_all(format!("{cmd}\n").as_bytes()).await.unwrap();
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    line.trim_end().to_string()
}

#[tokio::test]
async fn timeline_ingests_wal_from_a_safekeeper_via_control() {
    let dir = TempDir::new("sk");
    let sk_addr = spawn_safekeeper(&dir).await;
    let commit = append_wal(sk_addr, &wal_with_fpi()).await;

    let tenant = Tenant::new(1_000);
    let (l, control_addr) = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        (l, a)
    };
    tokio::spawn(serve_control(tenant.clone(), None, l));

    let tl_id = TimelineId::from_bytes([7; 16]);

    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        let mut c = BufReader::new(TcpStream::connect(control_addr).await.unwrap());
        assert!(control(&mut c, &format!("create {tl_id}")).await.starts_with("ok created"));
        let reply = control(&mut c, &format!("receive {tl_id} {sk_addr} 0")).await;
        assert!(reply.starts_with("ok receiving"), "receive reply: {reply}");

        // The background receiver streams the WAL into the timeline; poll until
        // the page materializes.
        let tl = tenant.get_timeline(tl_id).unwrap();
        let key = PageKey { rel: rel(), block: 0 };
        for _ in 0..100 {
            if let PageLookup::Page(p) = tl.get_page(key, commit).unwrap() {
                assert_eq!(&p[0..4], &[1, 2, 3, 4]);
                assert_eq!(&p[8188..8192], &[5, 6, 7, 8]);
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timeline did not ingest the WAL in time");
    })
    .await;
    assert!(outcome.is_ok(), "per-branch ingest test timed out");
}
