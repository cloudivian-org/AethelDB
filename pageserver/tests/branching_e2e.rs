// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! End-to-end test of branching over the page server's network endpoints.
//!
//! Creates a branch via the control endpoint, then reads the same page from the
//! parent and the branch over the page-service endpoint, asserting copy-on-write
//! isolation — all over real sockets.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::page_service::{Request, Response};
use common::{ForkNumber, Lsn, RelTag, TenantId, TimelineId, PAGE_SIZE};
use pageserver::page::{ByteEdit, Modification, PageVersion};
use pageserver::server::{serve_ingest, serve_pages};
use pageserver::tenant::Tenant;
use pageserver::{serve_control, Timeline};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

fn rel() -> RelTag {
    RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main }
}

async fn bind() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    (l, a)
}

async fn ingest(stream: &mut TcpStream, m: &Modification) {
    let body = m.encode();
    stream.write_all(&(body.len() as u32).to_be_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack[0], 0);
}

async fn get_page(stream: &mut TcpStream, timeline: TimelineId, block: u32, lsn: u64) -> Response {
    let req =
        Request::GetPage { tenant: TenantId::ZERO, timeline, rel: rel(), block, lsn: Lsn(lsn) };
    stream.write_all(&req.encode()).await.unwrap();
    let mut head = [0u8; 12];
    stream.read_exact(&mut head).await.unwrap();
    let plen = u32::from_be_bytes([head[8], head[9], head[10], head[11]]) as usize;
    let mut full = head.to_vec();
    full.resize(12 + plen, 0);
    stream.read_exact(&mut full[12..]).await.unwrap();
    Response::decode(&full).unwrap()
}

async fn control(stream: &mut BufReader<TcpStream>, cmd: &str) -> String {
    stream.get_mut().write_all(format!("{cmd}\n").as_bytes()).await.unwrap();
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    line.trim_end().to_string()
}

#[tokio::test]
async fn branch_over_the_network_is_isolated() {
    let tenant = Tenant::new(100_000);
    let root: Arc<Timeline> = tenant.create_timeline(TimelineId::ZERO).unwrap();

    let (l, ingest_addr) = bind().await;
    tokio::spawn(serve_ingest(root.clone(), l));
    let (l, page_addr) = bind().await;
    tokio::spawn(serve_pages(pageserver::TenantManager::single(tenant.clone()), l));
    let (l, control_addr) = bind().await;
    tokio::spawn(serve_control(pageserver::TenantManager::single(tenant.clone()), None, l));

    let branch_id = TimelineId::from_bytes([2; 16]);

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // 1. Write a base image to the root timeline at LSN 10.
        let mut isock = TcpStream::connect(ingest_addr).await.unwrap();
        ingest(
            &mut isock,
            &Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(10),
                version: PageVersion::Image(vec![1u8; PAGE_SIZE]),
            },
        )
        .await;

        // 2. Branch off the root at LSN 20 via the control endpoint.
        let mut csock = BufReader::new(TcpStream::connect(control_addr).await.unwrap());
        let reply =
            control(&mut csock, &format!("branch {branch_id} {} 20", TimelineId::ZERO)).await;
        assert!(reply.starts_with("ok branched"), "branch reply: {reply}");
        let listed = control(&mut csock, "list").await;
        assert!(listed.contains(&branch_id.to_string()));

        // 3. Diverge the branch: modify byte 0 on the branch only (the per-branch
        //    network ingest path lands with the control plane; write via handle).
        tenant.get_timeline(branch_id).unwrap().ingest([Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(30),
            version: PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![0xAB] }]),
        }]);

        // 4. Read both timelines over the page service: isolation holds.
        let mut psock = TcpStream::connect(page_addr).await.unwrap();
        match get_page(&mut psock, branch_id, 0, 100).await {
            Response::Page(p) => assert_eq!(p[0], 0xAB, "branch sees its own write"),
            other => panic!("branch read: {other:?}"),
        }
        match get_page(&mut psock, TimelineId::ZERO, 0, 100).await {
            Response::Page(p) => assert_eq!(p[0], 1, "root is unaffected by the branch"),
            other => panic!("root read: {other:?}"),
        }

        // 5. An unknown timeline is a clean error, not a panic.
        match get_page(&mut psock, TimelineId::from_bytes([9; 16]), 0, 100).await {
            Response::Error(msg) => assert!(msg.contains("unknown"), "msg: {msg}"),
            other => panic!("expected error, got {other:?}"),
        }
    })
    .await;
    assert!(outcome.is_ok(), "branching e2e timed out");
}
