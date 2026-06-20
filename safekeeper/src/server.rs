// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The safekeeper server: turns the network ingest stream into durable,
//! quorum-committed WAL.
//!
//! For each [`AppendRequest`] the server:
//! 1. checks the proposer's term against consensus (rejecting stale proposers);
//! 2. durably appends and flushes the WAL locally;
//! 3. records its own flush and replicates to peers, recording their acks;
//! 4. replies with its `flush_lsn` and the new quorum `commit_lsn`.
//!
//! The storage and consensus state are each behind a `std::sync::Mutex`; the
//! handler is careful never to hold a lock across the `.await` for peer
//! replication.

use std::sync::{Arc, Mutex};

use anyhow::Context;
use common::wal_service::{AppendRequest, AppendResponse, REQUEST_HEADER_LEN, STATUS_OK, STATUS_STALE_TERM};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::consensus::Consensus;
use crate::replicator::Replicator;
use crate::storage::WalStorage;

/// A running safekeeper node.
pub struct Safekeeper {
    storage: Mutex<WalStorage>,
    consensus: Mutex<Consensus>,
    replicator: Arc<dyn Replicator>,
}

impl Safekeeper {
    /// Assemble a safekeeper from its durable store, consensus state, and peer
    /// replicator.
    pub fn new(
        storage: WalStorage,
        consensus: Consensus,
        replicator: Arc<dyn Replicator>,
    ) -> Arc<Self> {
        Arc::new(Safekeeper {
            storage: Mutex::new(storage),
            consensus: Mutex::new(consensus),
            replicator,
        })
    }

    /// Current quorum-committed LSN (handy for tests and diagnostics).
    pub fn commit_lsn(&self) -> common::Lsn {
        self.consensus.lock().unwrap().commit_lsn()
    }

    /// Process one append: durably store, replicate, and compute the new commit.
    pub async fn handle_append(&self, req: &AppendRequest) -> anyhow::Result<AppendResponse> {
        let node_id = {
            let c = self.consensus.lock().unwrap();
            // Reject a proposer whose term is behind ours: a newer leader exists.
            if req.term < c.term() {
                return Ok(AppendResponse {
                    status: STATUS_STALE_TERM,
                    term: c.term(),
                    flush_lsn: self.storage.lock().unwrap().flush_lsn(),
                    commit_lsn: c.commit_lsn(),
                });
            }
            c.node_id()
        };

        // Durably append + flush. (No await while the storage lock is held.)
        let flush_lsn = {
            let mut s = self.storage.lock().unwrap();
            s.append(req.start_lsn, &req.payload).context("append to WAL store")?;
            s.flush().context("flush WAL store")?
        };
        let end_lsn = req.end_lsn();

        // Record our own flush, then release the lock before awaiting peers.
        {
            let mut c = self.consensus.lock().unwrap();
            c.record_flush(node_id, flush_lsn);
        }

        // Replicate to peers and gather their acknowledgements.
        let acks = self.replicator.replicate(req.term, end_lsn, &req.payload).await;

        // Fold peer acks in and read off the new commit position.
        let (term, commit_lsn) = {
            let mut c = self.consensus.lock().unwrap();
            for ack in &acks {
                c.record_flush(ack.node, ack.lsn);
            }
            (c.term(), c.commit_lsn())
        };

        debug!(%flush_lsn, %commit_lsn, peers = acks.len(), "append committed");
        Ok(AppendResponse { status: STATUS_OK, term, flush_lsn, commit_lsn })
    }

    /// Serve one ingest connection: read framed appends, reply to each.
    pub async fn serve_connection(self: &Arc<Self>, mut stream: TcpStream) -> anyhow::Result<()> {
        let mut header = [0u8; REQUEST_HEADER_LEN];
        loop {
            // Read the fixed header (clean EOF between messages ends the stream).
            match stream.read_exact(&mut header).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e).context("reading append header"),
            }
            let plen = AppendRequest::payload_len(&header).context("parsing append header")?;

            // Read the payload and assemble the full message.
            let mut full = Vec::with_capacity(REQUEST_HEADER_LEN + plen);
            full.extend_from_slice(&header);
            full.resize(REQUEST_HEADER_LEN + plen, 0);
            stream
                .read_exact(&mut full[REQUEST_HEADER_LEN..])
                .await
                .context("reading append payload")?;

            let req = AppendRequest::decode(&full).context("decoding append request")?;
            let resp = self.handle_append(&req).await?;
            stream.write_all(&resp.encode()).await.context("writing append response")?;
            stream.flush().await.ok();
        }
    }
}

/// Run the ingest accept loop on `listener`.
pub async fn serve(sk: Arc<Safekeeper>, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "accept failed; continuing");
                continue;
            }
        };
        let _ = socket.set_nodelay(true);
        let sk = sk.clone();
        tokio::spawn(async move {
            debug!(%peer, "compute connected for WAL ingest");
            if let Err(err) = sk.serve_connection(socket).await {
                warn!(%peer, error = %format!("{err:#}"), "ingest connection error");
            }
        });
    }
}

/// Log a startup banner (used by the binary).
pub fn log_started(addr: std::net::SocketAddr, node_id: u64, quorum: usize) {
    info!(%addr, node_id, quorum, "safekeeper ready to ingest WAL");
}
