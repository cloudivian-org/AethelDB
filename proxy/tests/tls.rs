// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! TLS termination: a client negotiates TLS via `SSLRequest`, the proxy answers
//! `S`, performs the handshake, then splices the encrypted session through to
//! the (plaintext) backend. Drives the proxy with a real rustls client over a
//! self-signed certificate.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use proxy::proxy::{serve, HealthConfig, Proxy};
use proxy::tenant::{Registry, TenantState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

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

fn ssl_request() -> Vec<u8> {
    let mut buf = 8i32.to_be_bytes().to_vec();
    buf.extend_from_slice(&80_877_103i32.to_be_bytes());
    buf
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
                if sock.read_exact(&mut rest).await.is_err() {
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
    addr
}

#[tokio::test]
async fn terminates_tls_and_splices_to_backend() {
    let backend = spawn_echo_backend().await;
    let registry = Arc::new(Registry::from_iter([(
        "echo".to_string(),
        TenantState::new(backend, true),
    )]));

    // A self-signed cert for "localhost".
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = ck.cert.pem();
    let key_pem = ck.key_pair.serialize_pem();
    let acceptor = proxy::tls::acceptor_from_pem_bytes(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap();

    let proxy = Proxy::with_tls(registry, Arc::new(proxy::activator::NoopActivator), HealthConfig::default(), acceptor);
    let proxy_addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve(proxy, listener).await;
        });
        addr
    };

    // A rustls client that trusts our self-signed cert.
    let mut roots = RootCertStore::empty();
    roots.add(ck.cert.der().clone()).unwrap();
    let config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // 1. SSLRequest in clear text; the proxy must answer 'S'.
        let mut tcp = TcpStream::connect(proxy_addr).await.unwrap();
        tcp.write_all(&ssl_request()).await.unwrap();
        let mut ans = [0u8; 1];
        tcp.read_exact(&mut ans).await.unwrap();
        assert_eq!(ans[0], b'S', "proxy must accept TLS");

        // 2. TLS handshake over the same socket.
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, tcp).await.expect("TLS handshake");

        // 3. Startup + a payload, all encrypted; the backend echoes it back.
        tls.write_all(&startup_packet("echo")).await.unwrap();
        tls.write_all(b"PING").await.unwrap();
        let mut echoed = [0u8; 4];
        tls.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"PING", "payload round-trips through TLS + backend");
    })
    .await;
    assert!(outcome.is_ok(), "TLS termination test timed out");
}

#[tokio::test]
async fn declines_tls_when_not_configured() {
    let backend = spawn_echo_backend().await;
    let registry = Arc::new(Registry::from_iter([(
        "echo".to_string(),
        TenantState::new(backend, true),
    )]));
    // No TLS configured -> the proxy must decline with 'N' and still work plaintext.
    let proxy = Proxy::new(registry, Arc::new(proxy::activator::NoopActivator), HealthConfig::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(proxy, listener).await;
    });

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut tcp = TcpStream::connect(proxy_addr).await.unwrap();
        tcp.write_all(&ssl_request()).await.unwrap();
        let mut ans = [0u8; 1];
        tcp.read_exact(&mut ans).await.unwrap();
        assert_eq!(ans[0], b'N', "proxy without a cert declines TLS");

        // The client retries in clear text and the session still works.
        tcp.write_all(&startup_packet("echo")).await.unwrap();
        tcp.write_all(b"PONG").await.unwrap();
        let mut echoed = [0u8; 4];
        tcp.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"PONG");
    })
    .await;
    assert!(outcome.is_ok(), "plaintext-decline test timed out");
}
