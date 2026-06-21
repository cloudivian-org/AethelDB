// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! End-to-end test of the page server over real sockets.
//!
//! Streams page modifications into the ingest endpoint, then issues `GetPage`
//! and `GetRelSize` over the page-service endpoint using `common::page_service`
//! — the same protocol the `aethel_smgr` compute extension speaks — and checks that
//! the reconstructed page matches the image-plus-deltas history.

use std::net::SocketAddr;
use std::time::Duration;

use common::page_service::{Request, Response};
use common::{ForkNumber, Lsn, RelTag, TenantId, TimelineId, PAGE_SIZE};
use pageserver::page::{ByteEdit, Modification, PageVersion};
use pageserver::server::{serve_ingest, serve_pages};
use pageserver::tenant::Tenant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rel() -> RelTag {
    RelTag { spc_node: 1663, db_node: 5, rel_node: 16384, fork: ForkNumber::Main }
}

async fn spawn<F, Fut>(f: F) -> SocketAddr
where
    F: FnOnce(TcpListener) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(f(listener));
    addr
}

/// Send one framed modification over the ingest socket and await its ack.
async fn ingest(stream: &mut TcpStream, m: &Modification) {
    let body = m.encode();
    stream.write_all(&(body.len() as u32).to_be_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack[0], 0, "ingest should ack with status 0");
}

/// Send a page-service request and decode the response.
async fn request(stream: &mut TcpStream, req: &Request) -> Response {
    stream.write_all(&req.encode()).await.unwrap();
    // Read the 12-byte response header, then its declared payload.
    let mut head = [0u8; 12];
    stream.read_exact(&mut head).await.unwrap();
    let payload_len = u32::from_be_bytes([head[8], head[9], head[10], head[11]]) as usize;
    let mut full = head.to_vec();
    full.resize(12 + payload_len, 0);
    stream.read_exact(&mut full[12..]).await.unwrap();
    Response::decode(&full).unwrap()
}

#[tokio::test]
async fn ingest_then_getpage_reconstructs_history() {
    let tenant = Tenant::new(100_000);
    let root = tenant.create_timeline(TimelineId::ZERO).unwrap();

    let ingest_addr = {
        let root = root.clone();
        spawn(move |l| async move {
            let _ = serve_ingest(root, l).await;
        })
        .await
    };
    let page_addr = {
        let tenant = tenant.clone();
        spawn(move |l| async move {
            let _ = serve_pages(tenant, l).await;
        })
        .await
    };

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // 1. Stream a base image at LSN 10 and two deltas at 20 and 30.
        let mut isock = TcpStream::connect(ingest_addr).await.unwrap();
        ingest(
            &mut isock,
            &Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(10),
                version: PageVersion::Image(vec![0u8; PAGE_SIZE]),
            },
        )
        .await;
        ingest(
            &mut isock,
            &Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(20),
                version: PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![0xAA] }]),
            },
        )
        .await;
        ingest(
            &mut isock,
            &Modification {
                rel: rel(),
                block: 0,
                lsn: Lsn(30),
                version: PageVersion::Delta(vec![ByteEdit { offset: 1, data: vec![0xBB] }]),
            },
        )
        .await;

        // 2. Ask the page-service endpoint for the page at several LSNs.
        let mut psock = TcpStream::connect(page_addr).await.unwrap();
        let get = |lsn| Request::GetPage {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            rel: rel(),
            block: 0,
            lsn,
        };

        match request(&mut psock, &get(Lsn(25))).await {
            Response::Page(p) => {
                assert_eq!(p.len(), PAGE_SIZE);
                assert_eq!(p[0], 0xAA); // first delta applied
                assert_eq!(p[1], 0x00); // second delta not yet
            }
            other => panic!("expected Page, got {other:?}"),
        }
        match request(&mut psock, &get(Lsn(30))).await {
            Response::Page(p) => {
                assert_eq!(p[0], 0xAA);
                assert_eq!(p[1], 0xBB); // both deltas applied
            }
            other => panic!("expected Page, got {other:?}"),
        }

        // 3. Relation size is reported from the ingested writes.
        let relsize = Request::GetRelSize {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            rel: rel(),
            lsn: Lsn(30),
        };
        match request(&mut psock, &relsize).await {
            Response::RelSize(n) => assert_eq!(n, 1),
            other => panic!("expected RelSize, got {other:?}"),
        }

        // 4. A page that doesn't exist is reported NotFound.
        let missing = Request::GetPage {
            tenant: TenantId::ZERO,
            timeline: TimelineId::ZERO,
            rel: rel(),
            block: 999,
            lsn: Lsn(30),
        };
        assert_eq!(request(&mut psock, &missing).await, Response::NotFound);
    })
    .await;

    assert!(outcome.is_ok(), "page server e2e test timed out");
}
