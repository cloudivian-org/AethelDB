// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! TLS termination for client connections.
//!
//! PostgreSQL clients negotiate TLS with an `SSLRequest` before the startup
//! packet. When the proxy is configured with a certificate it answers `S` and
//! performs a rustls handshake, then speaks the rest of the protocol over the
//! encrypted stream (terminating TLS at the proxy; the backend hop stays on the
//! trusted local network). Without a certificate it declines (`N`) as before.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Ensure a process-wide rustls crypto provider is installed (idempotent).
fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Ignore the error if another component already installed one.
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

/// Build a [`TlsAcceptor`] from PEM certificate-chain and private-key files.
pub fn acceptor_from_pem(cert_path: &Path, key_path: &Path) -> anyhow::Result<TlsAcceptor> {
    let cert_pem = std::fs::read(cert_path)
        .with_context(|| format!("reading TLS cert {}", cert_path.display()))?;
    let key_pem = std::fs::read(key_path)
        .with_context(|| format!("reading TLS key {}", key_path.display()))?;
    acceptor_from_pem_bytes(&cert_pem, &key_pem)
}

/// Build a [`TlsAcceptor`] from in-memory PEM bytes (used by tests).
pub fn acceptor_from_pem_bytes(cert_pem: &[u8], key_pem: &[u8]) -> anyhow::Result<TlsAcceptor> {
    ensure_crypto_provider();

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<_, _>>()
        .context("parsing TLS certificate chain")?;
    anyhow::ensure!(!certs.is_empty(), "no certificates found in the cert PEM");

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &key_pem[..])
        .context("parsing TLS private key")?
        .context("no private key found in the key PEM")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building the rustls server config")?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
