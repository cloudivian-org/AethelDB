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

/// A backend that, on each session, sends a `BackendKeyData(pid, secret)` then
/// echoes; and, when the proxy forwards a `CancelRequest`, reports the carried
/// `(process_id, secret_key)` on the returned channel. Stands in for compute.
async fn spawn_keyed_backend(
    pid: i32,
    secret: i32,
) -> (SocketAddr, tokio::sync::mpsc::UnboundedReceiver<(i32, i32)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                // Read one length-prefixed packet (a startup or a CancelRequest).
                let mut len = [0u8; 4];
                if sock.read_exact(&mut len).await.is_err() {
                    return;
                }
                let total = i32::from_be_bytes(len) as usize;
                let mut rest = vec![0u8; total.saturating_sub(4)];
                if sock.read_exact(&mut rest).await.is_err() {
                    return;
                }
                // A CancelRequest carries the cancel code in its first 4 bytes.
                if rest.len() >= 12
                    && i32::from_be_bytes(rest[0..4].try_into().unwrap()) == 80_877_102
                {
                    let p = i32::from_be_bytes(rest[4..8].try_into().unwrap());
                    let s = i32::from_be_bytes(rest[8..12].try_into().unwrap());
                    let _ = tx.send((p, s));
                    return;
                }
                // A normal session: announce BackendKeyData, then echo.
                let mut k = vec![b'K'];
                k.extend_from_slice(&12i32.to_be_bytes());
                k.extend_from_slice(&pid.to_be_bytes());
                k.extend_from_slice(&secret.to_be_bytes());
                if sock.write_all(&k).await.is_err() {
                    return;
                }
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
    (addr, rx)
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
        TenantState::new(backend.to_string(), true), // already running
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
    let marker =
        std::env::temp_dir().join(format!("aethel-proxy-coldstart-{}", std::process::id()));
    let _ = std::fs::remove_file(&marker);
    let start_cmd = format!("touch {}", marker.display());
    let activator = CommandActivator::new(start_cmd, "true");

    let registry = Arc::new(Registry::from_iter([(
        "sleepy".to_string(),
        TenantState::new(backend.to_string(), false), // begins asleep -> must be woken
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
async fn routes_cancel_request_to_owning_backend() {
    let (backend, mut cancels) = spawn_keyed_backend(4242, 2024).await;
    let registry = Arc::new(Registry::from_iter([(
        "echo".to_string(),
        TenantState::new(backend.to_string(), true),
    )]));
    let proxy = Proxy::new(registry, Arc::new(NoopActivator), HealthConfig::default());
    let proxy_addr = spawn_proxy(proxy).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // Session connection: startup, then read the BackendKeyData the proxy
        // forwards (and registers under the session's cancel key).
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(&startup_packet("echo")).await.unwrap();
        let mut k = [0u8; 13]; // 'K' + len(4) + pid(4) + secret(4)
        client.read_exact(&mut k).await.unwrap();
        assert_eq!(k[0], b'K', "expected BackendKeyData");
        assert_eq!(i32::from_be_bytes(k[5..9].try_into().unwrap()), 4242);
        assert_eq!(i32::from_be_bytes(k[9..13].try_into().unwrap()), 2024);

        // A second connection cancels using that key; the proxy must forward the
        // CancelRequest to the same backend that owns the session.
        let mut canceller = TcpStream::connect(proxy_addr).await.unwrap();
        canceller.write_all(&proxy::protocol::cancel_request_bytes(4242, 2024)).await.unwrap();

        let got = cancels.recv().await.expect("backend should receive the cancel");
        assert_eq!(got, (4242, 2024), "cancel must carry the backend's key");

        drop(client); // keep the session alive until the cancel is routed
    })
    .await;

    assert!(outcome.is_ok(), "cancel routing test timed out");
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
