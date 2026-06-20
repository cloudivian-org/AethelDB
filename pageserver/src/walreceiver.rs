// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The WAL receiver: streams committed WAL from a safekeeper into the page
//! store (Phase 4 of the WAL decode/redo subsystem; see `docs/design/wal-redo.md`).
//!
//! This closes the safekeeper→page-server link. The receiver opens a connection
//! to a safekeeper and repeatedly asks for committed WAL from a cursor LSN; each
//! [`ReadResponse`] chunk is fed into a **long-lived** [`WalStreamDecoder`] so
//! records that straddle chunk (or page) boundaries are stitched correctly. Each
//! decoded record is handed to [`Repository::ingest_record`].
//!
//! The cursor tracks how far WAL has been *fed* into the decoder, not how far it
//! has been *decoded* — a partially-received trailing record stays buffered in
//! the decoder and completes when the next chunk arrives. That distinction is
//! what makes arbitrary chunking safe.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::wal_service::{ReadRequest, ReadResponse, READ_RESPONSE_HEADER_LEN};
use common::{Lsn, TenantId, TimelineId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

use crate::repository::Repository;
use crate::waldecode::WalStreamDecoder;

/// How to connect to a safekeeper and what slice of WAL to pull.
#[derive(Debug, Clone)]
pub struct WalReceiverConfig {
    /// Safekeeper address to stream committed WAL from.
    pub safekeeper_addr: SocketAddr,
    /// Tenant whose WAL to receive.
    pub tenant: TenantId,
    /// Timeline (branch) to receive.
    pub timeline: TimelineId,
    /// LSN to begin ingesting from (the page server's persisted ingest cursor).
    pub start_lsn: Lsn,
    /// Maximum WAL bytes to request per round trip.
    pub max_chunk: u32,
    /// How long to wait before re-polling once caught up to the commit LSN.
    pub poll_interval: Duration,
}

impl WalReceiverConfig {
    /// Config with sensible defaults: 1 MiB chunks, 100 ms idle poll.
    pub fn new(safekeeper_addr: SocketAddr, tenant: TenantId, timeline: TimelineId, start_lsn: Lsn) -> Self {
        WalReceiverConfig {
            safekeeper_addr,
            tenant,
            timeline,
            start_lsn,
            max_chunk: 1 << 20,
            poll_interval: Duration::from_millis(100),
        }
    }
}

/// A live connection that pulls committed WAL into a [`Repository`].
pub struct WalReceiver {
    repo: Arc<Repository>,
    cfg: WalReceiverConfig,
    stream: TcpStream,
    /// Long-lived decoder; `None` until the first non-empty chunk arrives.
    decoder: Option<WalStreamDecoder>,
    /// Next LSN to request — equals how far WAL has been fed into the decoder.
    cursor: Lsn,
}

impl WalReceiver {
    /// Connect to the safekeeper and prepare to stream from `cfg.start_lsn`.
    pub async fn connect(repo: Arc<Repository>, cfg: WalReceiverConfig) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(cfg.safekeeper_addr).await?;
        let _ = stream.set_nodelay(true);
        let cursor = cfg.start_lsn;
        info!(addr = %cfg.safekeeper_addr, %cursor, "WAL receiver connected to safekeeper");
        Ok(WalReceiver { repo, cfg, stream, decoder: None, cursor })
    }

    /// The next LSN that will be requested (the ingest frontier).
    pub fn cursor(&self) -> Lsn {
        self.cursor
    }

    /// Perform one read round trip: request WAL from the cursor, feed the chunk
    /// to the decoder, and ingest every complete record. Returns the number of
    /// records ingested; `0` means the receiver is caught up to the commit LSN.
    pub async fn poll_once(&mut self) -> anyhow::Result<usize> {
        let req = ReadRequest {
            tenant: self.cfg.tenant,
            timeline: self.cfg.timeline,
            start_lsn: self.cursor,
            max_bytes: self.cfg.max_chunk,
        };
        self.stream.write_all(&req.encode()).await?;

        // Read the fixed response header, then its WAL payload.
        let mut buf = vec![0u8; READ_RESPONSE_HEADER_LEN];
        self.stream.read_exact(&mut buf).await?;
        let plen = ReadResponse::payload_len(&buf)?;
        buf.resize(READ_RESPONSE_HEADER_LEN + plen, 0);
        self.stream.read_exact(&mut buf[READ_RESPONSE_HEADER_LEN..]).await?;
        let resp = ReadResponse::decode(&buf)?;

        if resp.payload.is_empty() {
            return Ok(0);
        }

        // (Re)create the decoder on the first chunk, or if the safekeeper moved
        // our cursor forward past a retention horizon (a gap we can't stitch).
        let fresh = match &self.decoder {
            Some(_) => resp.start_lsn != self.cursor,
            None => true,
        };
        if fresh {
            self.decoder = Some(WalStreamDecoder::new(resp.start_lsn));
        }
        let decoder = self.decoder.as_mut().expect("decoder initialized above");

        decoder.feed_bytes(&resp.payload);
        self.cursor = Lsn(resp.start_lsn.raw() + resp.payload.len() as u64);

        let mut ingested = 0;
        while let Some((lsn, record)) = decoder.poll_decode()? {
            self.repo.ingest_record(lsn, &record)?;
            ingested += 1;
        }
        debug!(records = ingested, cursor = %self.cursor, "ingested WAL chunk");
        Ok(ingested)
    }

    /// Run forever: poll continuously, backing off `poll_interval` whenever the
    /// receiver has caught up to the commit position. Intended for the binary.
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            if self.poll_once().await? == 0 {
                tokio::time::sleep(self.cfg.poll_interval).await;
            }
        }
    }
}
