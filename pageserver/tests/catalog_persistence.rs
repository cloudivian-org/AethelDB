// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The tenant/timeline topology survives a page-server "restart": create tenants
//! and branches over the HTTP control plane (which persists to the object
//! store), then rebuild a fresh `TenantManager` from the same store and confirm
//! everything came back.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use common::{Lsn, TenantId, TimelineId};
use pageserver::objstore::{LocalObjectStore, ObjectStore};
use pageserver::serve_http_api;
use pageserver::TenantManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("sp-catalog-it-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        TempDir(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> u16 {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    text.split_whitespace().nth(1).unwrap().parse().unwrap()
}

fn tid(n: u8) -> String {
    TenantId::from_bytes([n; 16]).to_string()
}
fn tl(n: u8) -> String {
    TimelineId::from_bytes([n; 16]).to_string()
}

#[tokio::test]
async fn topology_survives_restart() {
    let dir = TempDir::new("restart");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(&dir.0).unwrap());

    // --- "Boot 1": a catalog-backed manager behind the HTTP control plane. ---
    let m1 = TenantManager::with_catalog(1_000, None, store.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(serve_http_api(m1, Some(store.clone()), listener));

    let acme = tid(0xAC);
    // Create a tenant, a root timeline, and a branch off it — each persists.
    assert_eq!(request(addr, "POST", "/v1/tenants", &format!(r#"{{"id":"{acme}"}}"#)).await, 201);
    assert_eq!(
        request(
            addr,
            "POST",
            "/v1/timelines",
            &format!(r#"{{"id":"{}","tenant":"{acme}"}}"#, tl(0))
        )
        .await,
        201
    );
    assert_eq!(
        request(
            addr,
            "POST",
            "/v1/branches",
            &format!(
                r#"{{"timeline":"{}","parent":"{}","lsn":250,"tenant":"{acme}"}}"#,
                tl(2),
                tl(0)
            ),
        )
        .await,
        201
    );
    handle.abort(); // "shut down" boot 1

    // --- "Boot 2": a brand-new manager restored from the same object store. ---
    let m2 = TenantManager::with_catalog(1_000, None, store.clone());
    m2.load_persisted().await;

    let acme_id = TenantId::from_bytes([0xAC; 16]);
    let tenant = m2.get(acme_id).expect("tenant restored after restart");
    assert!(tenant.get_timeline(TimelineId::from_bytes([0; 16])).is_some(), "root restored");
    let branch = tenant
        .get_timeline(TimelineId::from_bytes([2; 16]))
        .expect("branch restored after restart");
    assert_eq!(branch.ancestor_timeline(), Some(TimelineId::from_bytes([0; 16])));
    assert_eq!(branch.ancestor_lsn(), Some(Lsn(250)));
}
