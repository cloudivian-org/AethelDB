// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Proxy-side SCRAM authentication as a pre-wake gate: a bad credential is
//! rejected *without* starting compute (scale-to-zero protection); a good one
//! authenticates and the session splices through.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proxy::activator::Activator;
use proxy::proxy::{serve, HealthConfig, Proxy};
use proxy::scram::{client_authenticate, ScramSecret};
use proxy::tenant::{Registry, TenantState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn startup_packet(database: &str) -> Vec<u8> {
    let mut body = 0x0003_0000i32.to_be_bytes().to_vec();
    for (k, v) in [("user", "tester"), ("database", database)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    msg
}

/// An activator that records whether compute was ever asked to start.
struct RecordingActivator {
    started: Arc<AtomicBool>,
}

#[async_trait]
impl Activator for RecordingActivator {
    async fn start(&self, _tenant: &str) -> anyhow::Result<()> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn stop(&self, _tenant: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn spawn_echo_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut len = [0u8; 4];
                if sock.read_exact(&mut len).await.is_err() {
                    return;
                }
                let total = i32::from_be_bytes(len) as usize;
                let mut rest = vec![0u8; total.saturating_sub(4)];
                let _ = sock.read_exact(&mut rest).await;
                let mut buf = [0u8; 256];
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

async fn spawn_proxy(proxy: Arc<Proxy>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(proxy, listener).await;
    });
    addr
}

fn setup(backend: SocketAddr) -> (Arc<Proxy>, Arc<AtomicBool>) {
    let secret = ScramSecret::from_password("hunter2", b"tenant-salt-123", 4096);
    let registry = Arc::new(Registry::from_iter([(
        "db".to_string(),
        TenantState::with_scram(backend, false, secret), // asleep
    )]));
    let started = Arc::new(AtomicBool::new(false));
    let activator = Arc::new(RecordingActivator { started: started.clone() });
    (Proxy::new(registry, activator, HealthConfig::default()), started)
}

#[tokio::test]
async fn bad_password_is_rejected_without_waking_compute() {
    let backend = spawn_echo_backend().await;
    let (proxy, started) = setup(backend);
    let addr = spawn_proxy(proxy).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_packet("db")).await.unwrap();
        // Authenticate with the wrong password — the proxy must reject.
        let result = client_authenticate(&mut client, "tester", "wrong-password").await;
        assert!(result.is_err(), "auth with a bad password must fail");
    })
    .await;
    assert!(outcome.is_ok(), "bad-auth test timed out");

    // Give the proxy a moment, then assert compute was never started.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!started.load(Ordering::SeqCst), "bad auth must NOT wake compute");
}

#[tokio::test]
async fn good_password_authenticates_and_splices() {
    let backend = spawn_echo_backend().await;
    let (proxy, started) = setup(backend);
    let addr = spawn_proxy(proxy).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_packet("db")).await.unwrap();
        // Correct password: SCRAM completes, the proxy wakes compute and splices.
        client_authenticate(&mut client, "tester", "hunter2").await.expect("auth should succeed");

        // Past auth, traffic flows through to the backend (which echoes).
        client.write_all(b"PING").await.unwrap();
        let mut echoed = [0u8; 4];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"PING");
    })
    .await;
    assert!(outcome.is_ok(), "good-auth test timed out");
    assert!(started.load(Ordering::SeqCst), "good auth should wake compute");
}
