// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The page server's two network endpoints.
//!
//! * **Page service** ([`serve_pages`]) answers the compute node's `GetPage` and
//!   `GetRelSize` requests using the protocol in `common::page_service` — the
//!   exact protocol the `aethel_smgr` extension speaks. Each request is reconstructed
//!   from the repository and returned.
//! * **Ingest** ([`serve_ingest`]) accepts a stream of length-prefixed
//!   [`Modification`]s (as a WAL decoder feeds in committed WAL) and applies
//!   them to the repository.

use std::sync::Arc;

use anyhow::Context;
use common::page_service::{Request, Response};
use common::PageKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::page::Modification;
use crate::repository::PageLookup;
use crate::tenant::Tenant;
use crate::timeline::Timeline;

/// Header length of a page-service request (to size the first read).
const PAGE_REQ_HEADER: usize = 8;
/// Maximum accepted ingest payload, guarding against bad framing.
const MAX_INGEST_BODY: usize = 1 << 20;

/// Serve the page-service endpoint: `GetPage` / `GetRelSize`, routed to the
/// timeline named in each request.
pub async fn serve_pages(tenant: Arc<Tenant>, listener: TcpListener) -> anyhow::Result<()> {
    accept_loop(listener, move |socket| {
        let tenant = tenant.clone();
        async move { handle_page_conn(tenant, socket).await }
    })
    .await
}

/// Serve the ingest endpoint: apply incoming `Modification`s to `timeline`.
///
/// This is the legacy push path (the primary ingest path is the WAL receiver);
/// it targets a single timeline, typically the tenant's root.
pub async fn serve_ingest(timeline: Arc<Timeline>, listener: TcpListener) -> anyhow::Result<()> {
    accept_loop(listener, move |socket| {
        let timeline = timeline.clone();
        async move { handle_ingest_conn(timeline, socket).await }
    })
    .await
}

/// Shared accept loop that spawns `handler` per connection.
async fn accept_loop<F, Fut>(listener: TcpListener, handler: F) -> anyhow::Result<()>
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let handler = Arc::new(handler);
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "accept failed; continuing");
                continue;
            }
        };
        let _ = socket.set_nodelay(true);
        let handler = handler.clone();
        tokio::spawn(async move {
            if let Err(err) = handler(socket).await {
                warn!(%peer, error = %format!("{err:#}"), "connection error");
            }
        });
    }
}

async fn handle_page_conn(tenant: Arc<Tenant>, mut socket: TcpStream) -> anyhow::Result<()> {
    let mut header = [0u8; PAGE_REQ_HEADER];
    loop {
        match socket.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e).context("reading page request header"),
        }
        let total = Request::total_len(&header).context("parsing page request header")?;

        let mut full = vec![0u8; total];
        full[..PAGE_REQ_HEADER].copy_from_slice(&header);
        socket
            .read_exact(&mut full[PAGE_REQ_HEADER..])
            .await
            .context("reading page request body")?;
        let req = Request::decode(&full).context("decoding page request")?;

        // Route to the requested timeline; an unknown timeline is an error.
        let timeline_id = match &req {
            Request::GetPage { timeline, .. } | Request::GetRelSize { timeline, .. } => *timeline,
        };
        let resp = match tenant.get_timeline(timeline_id) {
            None => Response::Error(format!("unknown timeline {timeline_id}")),
            Some(timeline) => match req {
                Request::GetPage { rel, block, lsn, .. } => {
                    match timeline.get_page(PageKey { rel, block }, lsn) {
                        Ok(PageLookup::Page(page)) => Response::Page(page),
                        Ok(PageLookup::NotFound) => Response::NotFound,
                        Err(e) => Response::Error(format!("reconstruction failed: {e}")),
                    }
                }
                Request::GetRelSize { rel, lsn, .. } => match timeline.get_rel_size(rel, lsn) {
                    Some(n) => Response::RelSize(n),
                    None => Response::NotFound,
                },
            },
        };
        socket.write_all(&resp.encode()).await.context("writing page response")?;
        socket.flush().await.ok();
    }
}

async fn handle_ingest_conn(timeline: Arc<Timeline>, mut socket: TcpStream) -> anyhow::Result<()> {
    loop {
        // Each record is `[u32 length][modification body]`.
        let mut len_buf = [0u8; 4];
        match socket.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e).context("reading ingest length"),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        anyhow::ensure!(len > 0 && len <= MAX_INGEST_BODY, "ingest body length {len} out of range");

        let mut body = vec![0u8; len];
        socket.read_exact(&mut body).await.context("reading ingest body")?;
        let m = Modification::decode(&body).context("decoding modification")?;
        debug!(rel = ?m.rel, block = m.block, lsn = %m.lsn, "ingesting modification");
        timeline.ingest([m]);

        // Acknowledge with a single status byte (0 = applied).
        socket.write_all(&[0u8]).await.context("writing ingest ack")?;
        socket.flush().await.ok();
    }
}
