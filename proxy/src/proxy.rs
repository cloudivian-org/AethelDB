// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The proxy data path: accept a client, learn its tenant, wake compute if
//! needed, and splice the two sockets together.
//!
//! Lifecycle of one connection:
//! 1. Negotiate SSL: terminate TLS when configured (else decline), then read the
//!    `StartupMessage` over the negotiated stream and extract the tenant.
//! 2. Resolve the tenant in the [`Registry`]; reject unknown tenants with a
//!    protocol `ErrorResponse`.
//! 3. If compute isn't running, ask the [`Activator`] to start it; either way,
//!    hold the client socket open until the backend passes its readiness probe.
//! 4. Connect to the backend, replay the original startup bytes, and run a
//!    bidirectional copy until either side closes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::activator::{wait_until_ready, Activator};
use crate::protocol::{
    self, parse_first_message, FirstMessage, StartupMessage, MAX_STARTUP_LEN, MIN_STARTUP_LEN,
};
use crate::tenant::{Registry, TenantState};

/// Tunables for the readiness probe.
#[derive(Debug, Clone, Copy)]
pub struct HealthConfig {
    /// Total time the proxy will hold a client socket waiting for compute.
    pub budget: Duration,
    /// Delay between connection attempts during the probe.
    pub interval: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        // 500 ms wake budget, probed every 10 ms.
        HealthConfig { budget: Duration::from_millis(500), interval: Duration::from_millis(10) }
    }
}

/// Shared, cloneable proxy state.
pub struct Proxy {
    registry: Arc<Registry>,
    activator: Arc<dyn Activator>,
    health: HealthConfig,
    /// When set, the proxy terminates TLS for clients that send an `SSLRequest`.
    tls: Option<TlsAcceptor>,
}

impl Proxy {
    /// Assemble the proxy from its collaborators (no TLS).
    pub fn new(registry: Arc<Registry>, activator: Arc<dyn Activator>, health: HealthConfig) -> Arc<Self> {
        Arc::new(Proxy { registry, activator, health, tls: None })
    }

    /// Assemble the proxy with TLS termination enabled.
    pub fn with_tls(
        registry: Arc<Registry>,
        activator: Arc<dyn Activator>,
        health: HealthConfig,
        tls: TlsAcceptor,
    ) -> Arc<Self> {
        Arc::new(Proxy { registry, activator, health, tls: Some(tls) })
    }

    /// The tenant registry (used by the idle reaper).
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// The activator (used by the idle reaper to stop compute).
    pub fn activator(&self) -> &Arc<dyn Activator> {
        &self.activator
    }

    /// Handle a single accepted client connection end to end.
    pub async fn handle_connection(self: &Arc<Self>, client: TcpStream, peer: SocketAddr) {
        if let Err(err) = self.serve_client(client, peer).await {
            // Best-effort: connection-scoped errors are logged, not fatal.
            warn!(%peer, error = %format!("{err:#}"), "connection closed with error");
        }
    }

    /// Read the first packet at the raw TCP level (pre-TLS), handle SSL/GSS
    /// negotiation, and continue over the resulting stream (TCP or TLS).
    async fn serve_client(self: &Arc<Self>, mut client: TcpStream, peer: SocketAddr) -> anyhow::Result<()> {
        let raw = match read_raw_message(&mut client).await? {
            Some(raw) => raw,
            None => return Ok(()), // clean EOF before any startup
        };
        match parse_first_message(raw).context("parsing client startup packet")? {
            // Already the startup packet (no encryption requested): proceed plaintext.
            FirstMessage::Startup(s) => self.serve_over(client, peer, Some(s)).await,

            FirstMessage::SslRequest => match &self.tls {
                // TLS configured: accept ('S'), handshake, then speak over TLS.
                Some(acceptor) => {
                    client.write_all(b"S").await.context("accepting SSLRequest")?;
                    client.flush().await.ok();
                    let tls = acceptor.accept(client).await.context("TLS handshake")?;
                    debug!(%peer, "TLS established");
                    self.serve_over(tls, peer, None).await
                }
                // No TLS: decline ('N'); the client retries the startup in clear.
                None => {
                    client.write_all(b"N").await.context("declining SSLRequest")?;
                    client.flush().await.ok();
                    self.serve_over(client, peer, None).await
                }
            },

            FirstMessage::GssEncRequest => {
                client.write_all(b"N").await.context("declining GSS")?;
                client.flush().await.ok();
                self.serve_over(client, peer, None).await
            }

            FirstMessage::CancelRequest { process_id, .. } => {
                // Routing cancels requires tracking backend key data per session;
                // deferred. Close cleanly so the client isn't left hanging.
                debug!(%peer, process_id, "received CancelRequest (unsupported); closing");
                Ok(())
            }
        }
    }

    /// Drive the connection over the negotiated client stream `S` (plaintext
    /// `TcpStream` or a TLS stream). `startup` is `Some` when it was already read
    /// before negotiation; otherwise it is read here over the negotiated stream.
    async fn serve_over<S>(
        self: &Arc<Self>,
        mut client: S,
        peer: SocketAddr,
        startup: Option<StartupMessage>,
    ) -> anyhow::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        // --- 1. Obtain the startup packet over the negotiated stream. ---
        let startup = match startup {
            Some(s) => s,
            None => loop {
                let raw = match read_raw_message(&mut client).await? {
                    Some(raw) => raw,
                    None => return Ok(()),
                };
                match parse_first_message(raw).context("parsing client startup packet")? {
                    FirstMessage::Startup(s) => break s,
                    // A repeat SSL/GSS request (e.g. after an 'N' decline) — decline again.
                    FirstMessage::SslRequest | FirstMessage::GssEncRequest => {
                        client.write_all(b"N").await.context("declining SSL/GSS")?;
                        client.flush().await.ok();
                    }
                    FirstMessage::CancelRequest { .. } => return Ok(()),
                }
            },
        };

        let tenant_name = match startup.tenant() {
            Some(t) => t.to_owned(),
            None => {
                self.reject(&mut client, "3D000", "no database or user specified").await;
                return Ok(());
            }
        };

        // --- 2. Resolve the tenant. ---
        let Some(state) = self.registry.get(&tenant_name) else {
            info!(%peer, tenant = %tenant_name, "rejecting unknown tenant");
            self.reject(&mut client, "3D000", &format!("unknown tenant \"{tenant_name}\"")).await;
            return Ok(());
        };

        // --- 3. Ensure compute is awake, holding this socket open meanwhile. ---
        self.ensure_awake(&tenant_name, &state).await?;

        // --- 4. Connect to the backend, replay startup, and splice. ---
        let backend_addr = state.backend();
        let mut backend = TcpStream::connect(backend_addr)
            .await
            .with_context(|| format!("connecting to backend {backend_addr}"))?;
        backend
            .write_all(&startup.raw)
            .await
            .context("forwarding startup packet to backend")?;

        // Account for this connection for the lifetime of the splice; the guard
        // guarantees the gauge is decremented even on error or panic.
        let _guard = ConnGuard::new(state.clone());
        info!(%peer, tenant = %tenant_name, %backend_addr, "splicing connection");

        let (c2b, b2c) = tokio::io::copy_bidirectional(&mut client, &mut backend)
            .await
            .context("while proxying client <-> backend")?;
        debug!(%peer, tenant = %tenant_name, client_to_backend = c2b, backend_to_client = b2c, "connection finished");
        Ok(())
    }

    /// Make sure the tenant's compute is running and reachable, holding the
    /// caller's client socket open for the duration.
    async fn ensure_awake(self: &Arc<Self>, tenant: &str, state: &TenantState) -> anyhow::Result<()> {
        if !state.is_running() {
            info!(tenant, "cold start: triggering activator");
            self.activator
                .start(tenant)
                .await
                .with_context(|| format!("activator failed to start tenant {tenant}"))?;
        }

        let elapsed = wait_until_ready(state.backend(), self.health.budget, self.health.interval)
            .await
            .with_context(|| format!("compute for tenant {tenant} did not become ready"))?;

        // Probe succeeded: record compute as running so the next connection
        // skips the activator.
        state.set_running(true);
        if elapsed > Duration::from_millis(50) {
            info!(tenant, ?elapsed, "compute ready (cold start)");
        }
        Ok(())
    }

    /// Send a protocol `ErrorResponse` then drop the connection.
    async fn reject<S: AsyncWrite + Unpin>(
        self: &Arc<Self>,
        client: &mut S,
        sqlstate: &str,
        message: &str,
    ) {
        let bytes = protocol::error_response("FATAL", sqlstate, message);
        let _ = client.write_all(&bytes).await;
        let _ = client.flush().await;
    }
}

/// Run the accept loop on `listener`, spawning each connection onto its own task.
pub async fn serve(proxy: Arc<Proxy>, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "accept failed; continuing");
                continue;
            }
        };
        // Disable Nagle: this is an interactive request/response path where
        // latency matters more than packing bytes.
        let _ = socket.set_nodelay(true);

        let proxy = proxy.clone();
        tokio::spawn(async move { proxy.handle_connection(socket, peer).await });
    }
}

/// Read one length-prefixed startup-style packet into a buffer (length prefix
/// included). Returns `None` on a clean EOF before any bytes arrive.
async fn read_raw_message<S: AsyncRead + Unpin>(stream: &mut S) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        // A clean disconnect before sending anything is not an error.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("reading packet length"),
    }

    let declared = i32::from_be_bytes(len_buf) as usize;
    anyhow::ensure!(
        (MIN_STARTUP_LEN..=MAX_STARTUP_LEN).contains(&declared),
        "startup packet length {declared} out of range"
    );

    let mut raw = vec![0u8; declared];
    raw[..4].copy_from_slice(&len_buf);
    stream
        .read_exact(&mut raw[4..])
        .await
        .context("reading packet body")?;
    Ok(Some(raw))
}

/// RAII guard that increments the tenant's connection gauge on creation and
/// decrements it when dropped, so the count is correct on every exit path.
struct ConnGuard {
    state: Arc<TenantState>,
}

impl ConnGuard {
    fn new(state: Arc<TenantState>) -> Self {
        state.connection_started();
        ConnGuard { state }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.state.connection_finished();
    }
}
