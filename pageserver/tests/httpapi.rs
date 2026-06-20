// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! HTTP/JSON control-plane API: drive timeline/branch/GC operations over real
//! HTTP requests.

use std::net::SocketAddr;

use common::TimelineId;
use pageserver::serve_http_api;
use pageserver::tenant::Tenant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn api() -> SocketAddr {
    let tenant = Tenant::new(1_000);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_http_api(tenant, None, listener));
    addr
}

/// Send an HTTP request, returning `(status, body)`.
async fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn id(n: u8) -> String {
    TimelineId::from_bytes([n; 16]).to_string()
}

#[tokio::test]
async fn http_control_plane_manages_timelines() {
    let addr = api().await;

    // Health.
    let (s, b) = request(addr, "GET", "/healthz", "").await;
    assert_eq!(s, 200);
    assert!(b.contains("ok"));

    // Create a root timeline.
    let (s, b) = request(addr, "POST", "/v1/timelines", &format!(r#"{{"id":"{}"}}"#, id(1))).await;
    assert_eq!(s, 201, "body: {b}");
    assert!(b.contains(&id(1)));

    // Creating it again conflicts.
    let (s, _) = request(addr, "POST", "/v1/timelines", &format!(r#"{{"id":"{}"}}"#, id(1))).await;
    assert_eq!(s, 409);

    // Branch off it.
    let (s, b) = request(
        addr,
        "POST",
        "/v1/branches",
        &format!(r#"{{"timeline":"{}","parent":"{}","lsn":100}}"#, id(2), id(1)),
    )
    .await;
    assert_eq!(s, 201, "body: {b}");

    // List shows both.
    let (s, b) = request(addr, "GET", "/v1/timelines", "").await;
    assert_eq!(s, 200);
    assert!(b.contains(&id(1)) && b.contains(&id(2)));

    // GC runs and reports stats.
    let (s, b) = request(addr, "POST", "/v1/gc", r#"{"horizon_lsn":50}"#).await;
    assert_eq!(s, 200);
    assert!(b.contains("versions_removed"));

    // Errors: bad body, unknown route.
    let (s, _) = request(addr, "POST", "/v1/timelines", "not json").await;
    assert_eq!(s, 400);
    let (s, _) = request(addr, "GET", "/v1/nope", "").await;
    assert_eq!(s, 404);
}
