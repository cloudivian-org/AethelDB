// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Operational metrics: a `/metrics` endpoint in the Prometheus text format.
//!
//! Every service registers its counters and gauges with the process-global
//! Prometheus registry (via the `prometheus` crate's `register_*!` macros) and
//! then serves them with [`serve_metrics`]. Keeping the HTTP server here means
//! all four services expose metrics identically; the metric *definitions* live
//! in each service.
//!
//! The server speaks just enough HTTP to answer a scrape: it reads and ignores
//! the request and replies with the encoded metrics. That avoids pulling a full
//! HTTP framework into the hot binaries.

use prometheus::{Encoder, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::warn;

// Re-export so services can define metrics without a separate prometheus dep
// version to keep in lockstep.
pub use prometheus;

/// Serve the process's Prometheus metrics over HTTP on `listener`.
///
/// Answers any request with `200 OK` and the metrics in the Prometheus text
/// exposition format — point a Prometheus scrape (or `curl`) at it.
pub async fn serve_metrics(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "metrics accept failed; continuing");
                continue;
            }
        };
        tokio::spawn(async move {
            if let Err(err) = handle(socket).await {
                warn!(error = %format!("{err:#}"), "metrics connection error");
            }
        });
    }
}

/// Encode the current metrics into the Prometheus text format.
pub fn render() -> Vec<u8> {
    let families = prometheus::gather();
    let mut buf = Vec::new();
    let _ = TextEncoder::new().encode(&families, &mut buf);
    buf
}

async fn handle(mut socket: TcpStream) -> anyhow::Result<()> {
    // Read and discard the request (we only ever serve metrics).
    let mut scratch = [0u8; 1024];
    let _ = socket.read(&mut scratch).await?;

    let body = render();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(&body).await?;
    socket.flush().await?;
    Ok(())
}
