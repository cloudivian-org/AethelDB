// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! End-to-end tests for the activation proxy over real TCP sockets.
//!
//! These exercise the two headline behaviours of Step 2:
//! 1. SSL decline + startup parsing + bidirectional splice to a live backend.
//! 2. Cold start: a tenant that begins "asleep" triggers the activator before
//!    traffic is piped through.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use proxy::activator::{CommandActivator, NoopActivator};
use proxy::proxy::{serve, HealthConfig, Proxy};
use proxy::tenant::{Registry, TenantState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// PROTOCOL_V3 startup packet for a given database, as a real client would send.
fn startup_packet(database: &str) -> Vec<u8> {
    let mut body = 0x0003_0000i32.to_be_bytes().to_vec(); // protocol 3.0
    for (k, v) in [("user", "tester"), ("database", database)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0); // end of parameters
    let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    msg
}

/// An SSLRequest packet (length 8 + magic code).
fn ssl_request() -> Vec<u8> {
    let mut buf = 8i32.to_be_bytes().to_vec();
    buf.extend_from_slice(&80_877_103i32.to_be_bytes());
    buf
}

/// Spawn a backend that consumes one startup packet then echoes everything else,
/// standing in for a real PostgreSQL compute node. Returns its address.
async fn spawn_echo_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            tokio::spawn(async move {
                // Consume the startup packet the proxy replays.
                let mut len = [0u8; 4];
                if sock.read_exact(&mut len).await.is_err() {
                    return;
                }
                let total = i32::from_be_bytes(len) as usize;
                let mut rest = vec![0u8; total.saturating_sub(4)];
                if sock.read_exact(&mut rest).await.is_err() {
                    return;
                }
                // Echo the remainder of the session.
                let mut buf = [0u8; 1024];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if sock.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

/// Bind the proxy on an ephemeral port, spawn its accept loop, return its addr.
async fn spawn_proxy(proxy: Arc<Proxy>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(proxy, listener).await;
    });
    addr
}

#[tokio::test]
async fn splices_client_to_running_backend() {
    let backend = spawn_echo_backend().await;
    let registry = Arc::new(Registry::from_iter([(
        "echo".to_string(),
        TenantState::new(backend, true), // already running
    )]));
    let proxy = Proxy::new(registry, Arc::new(NoopActivator), HealthConfig::default());
    let proxy_addr = spawn_proxy(proxy).await;

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();

        // SSL negotiation: proxy must decline with 'N'.
        client.write_all(&ssl_request()).await.unwrap();
        let mut n = [0u8; 1];
        client.read_exact(&mut n).await.unwrap();
        assert_eq!(&n, b"N", "proxy should decline SSL");

        // Startup, then a payload that must echo back through the splice.
        client.write_all(&startup_packet("echo")).await.unwrap();
        client.write_all(b"PING").await.unwrap();

        let mut echoed = [0u8; 4];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"PING", "payload must round-trip through the backend");
    })
    .await;

    assert!(result.is_ok(), "splice test timed out");
}

#[tokio::test]
async fn cold_start_invokes_activator() {
    let backend = spawn_echo_backend().await;

    // A start command that drops a marker file proves the activator ran.
    let marker = std::env::temp_dir().join(format!("aethel-proxy-coldstart-{}", std::process::id()));
    let _ = std::fs::remove_file(&marker);
    let start_cmd = format!("touch {}", marker.display());
    let activator = CommandActivator::new(start_cmd, "true");

    let registry = Arc::new(Registry::from_iter([(
        "sleepy".to_string(),
        TenantState::new(backend, false), // begins asleep -> must be woken
    )]));
    let proxy = Proxy::new(registry, Arc::new(activator), HealthConfig::default());
    let proxy_addr = spawn_proxy(proxy).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(&startup_packet("sleepy")).await.unwrap();
        client.write_all(b"DATA").await.unwrap();
        let mut echoed = [0u8; 4];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"DATA");
    })
    .await;

    assert!(outcome.is_ok(), "cold-start test timed out");
    assert!(marker.exists(), "activator start command should have created the marker");
    let _ = std::fs::remove_file(&marker);
}

#[tokio::test]
async fn unknown_tenant_is_rejected_with_error_response() {
    let registry = Arc::new(Registry::from_iter([])); // no tenants
    let proxy = Proxy::new(registry, Arc::new(NoopActivator), HealthConfig::default());
    let proxy_addr = spawn_proxy(proxy).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(&startup_packet("ghost")).await.unwrap();

        // Expect a backend ErrorResponse ('E') rather than a silent close.
        let mut tag = [0u8; 1];
        client.read_exact(&mut tag).await.unwrap();
        assert_eq!(&tag, b"E", "unknown tenant should get an ErrorResponse");
    })
    .await;

    assert!(outcome.is_ok(), "rejection test timed out");
}
