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

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use common::wal_service::{
    message_type, AppendRequest, AppendResponse, ReadRequest, ReadResponse, VoteRequest,
    VoteResponse, PREFIX_LEN, READ_REQUEST_LEN, REQUEST_HEADER_LEN, STATUS_OK, STATUS_STALE_TERM,
    TYPE_APPEND, TYPE_READ, TYPE_REPLICATE, TYPE_VOTE, VOTE_REQUEST_LEN, VOTE_RESPONSE_LEN,
};
use common::{TenantId, TimelineId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::consensus::{Consensus, NodeId};
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

        crate::metrics::APPENDS.inc();
        crate::metrics::WAL_BYTES.inc_by(req.payload.len() as u64);

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

        crate::metrics::COMMIT_LSN.set(commit_lsn.raw() as i64);
        debug!(%flush_lsn, %commit_lsn, peers = acks.len(), "append committed");
        Ok(AppendResponse { status: STATUS_OK, term, flush_lsn, commit_lsn })
    }

    /// Process a replication append from a leader safekeeper: durably store and
    /// flush the bytes and reply with our flush position. Acceptor role — it does
    /// **not** re-replicate, which is what keeps forwarding from looping.
    pub fn handle_replicate(&self, req: &AppendRequest) -> anyhow::Result<AppendResponse> {
        let (node_id, term) = {
            let c = self.consensus.lock().unwrap();
            if req.term < c.term() {
                return Ok(AppendResponse {
                    status: STATUS_STALE_TERM,
                    term: c.term(),
                    flush_lsn: self.storage.lock().unwrap().flush_lsn(),
                    commit_lsn: c.commit_lsn(),
                });
            }
            (c.node_id(), c.term())
        };

        let flush_lsn = {
            let mut s = self.storage.lock().unwrap();
            s.append(req.start_lsn, &req.payload).context("replicate: append to WAL store")?;
            s.flush().context("replicate: flush WAL store")?
        };

        let commit_lsn = {
            let mut c = self.consensus.lock().unwrap();
            c.record_flush(node_id, flush_lsn);
            c.commit_lsn()
        };
        crate::metrics::REPLICATED.inc();
        debug!(%flush_lsn, "replicated WAL run from leader");
        Ok(AppendResponse { status: STATUS_OK, term, flush_lsn, commit_lsn })
    }

    /// Serve a read request: return committed WAL from the requested cursor.
    ///
    /// Answers with `[from, commit_lsn)` capped at `max_bytes`, where `from` is
    /// the request's `start_lsn` clamped up to the retained floor. An empty
    /// payload means the reader is already caught up to the commit position.
    pub fn handle_read(&self, req: &ReadRequest) -> anyhow::Result<ReadResponse> {
        let commit_lsn = self.consensus.lock().unwrap().commit_lsn();
        let storage = self.storage.lock().unwrap();
        let from = req.start_lsn.max(storage.start_lsn());

        let available = commit_lsn
            .raw()
            .saturating_sub(from.raw())
            .min(req.max_bytes as u64) as usize;

        let mut payload = vec![0u8; available];
        if available > 0 {
            storage.read_at(from, &mut payload).context("reading committed WAL")?;
        }
        Ok(ReadResponse { status: STATUS_OK, commit_lsn, start_lsn: from, payload })
    }

    /// Handle a leadership vote request from a candidate. Grants at most one
    /// vote per term (the consensus state machine enforces this) and reports our
    /// current term and flush position.
    pub fn handle_vote(&self, req: &VoteRequest) -> VoteResponse {
        let (granted, term) = {
            let mut c = self.consensus.lock().unwrap();
            let granted = c.handle_vote_request(req.term, req.candidate);
            (granted, c.term())
        };
        if granted {
            crate::metrics::VOTES_GRANTED.inc();
        }
        let flush_lsn = self.storage.lock().unwrap().flush_lsn();
        debug!(term = req.term, candidate = req.candidate, granted, "handled vote request");
        VoteResponse { granted, term, flush_lsn }
    }

    /// Stand for election as leader of the group: bump the term and request
    /// votes from `peers` over the network. Returns whether we won (reached a
    /// quorum of grants, counting our own self-vote).
    pub async fn run_election(self: &Arc<Self>, peers: &[(NodeId, SocketAddr)]) -> anyhow::Result<bool> {
        let (term, candidate) = {
            let mut c = self.consensus.lock().unwrap();
            (c.start_election(), c.node_id())
        };
        let req = VoteRequest { tenant: TenantId::ZERO, timeline: TimelineId::ZERO, term, candidate };
        let bytes = req.encode();

        for (node, addr) in peers {
            match request_vote(*addr, &bytes).await {
                Ok(resp) if resp.granted => {
                    self.consensus.lock().unwrap().handle_vote_granted(*node, term);
                }
                Ok(resp) => debug!(node, peer_term = resp.term, "vote denied"),
                Err(e) => warn!(node, %addr, error = %format!("{e:#}"), "vote request failed"),
            }
        }

        let won = self.consensus.lock().unwrap().is_leader();
        info!(term, won, "election finished");
        Ok(won)
    }

    /// Serve one connection: dispatch framed appends and reads until EOF.
    pub async fn serve_connection(self: &Arc<Self>, mut stream: TcpStream) -> anyhow::Result<()> {
        let mut prefix = [0u8; PREFIX_LEN];
        loop {
            // Read the common prefix (clean EOF between messages ends the stream).
            match stream.read_exact(&mut prefix).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e).context("reading message prefix"),
            }
            match message_type(&prefix).context("parsing message prefix")? {
                TYPE_APPEND => self.serve_append(&mut stream, &prefix).await?,
                TYPE_REPLICATE => self.serve_replicate(&mut stream, &prefix).await?,
                TYPE_READ => self.serve_read(&mut stream, &prefix).await?,
                TYPE_VOTE => self.serve_vote(&mut stream, &prefix).await?,
                other => anyhow::bail!("unknown WAL message type {other}"),
            }
            stream.flush().await.ok();
        }
    }

    /// Read the rest of an append message, handle it, and reply.
    async fn serve_append(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        prefix: &[u8; PREFIX_LEN],
    ) -> anyhow::Result<()> {
        // Assemble the full fixed header (prefix + remainder), then the payload.
        let mut full = vec![0u8; REQUEST_HEADER_LEN];
        full[..PREFIX_LEN].copy_from_slice(prefix);
        stream
            .read_exact(&mut full[PREFIX_LEN..])
            .await
            .context("reading append header")?;
        let plen = AppendRequest::payload_len(&full).context("parsing append header")?;
        full.resize(REQUEST_HEADER_LEN + plen, 0);
        stream
            .read_exact(&mut full[REQUEST_HEADER_LEN..])
            .await
            .context("reading append payload")?;

        let req = AppendRequest::decode(&full).context("decoding append request")?;
        let resp = self.handle_append(&req).await?;
        stream.write_all(&resp.encode()).await.context("writing append response")?;
        Ok(())
    }

    /// Read the rest of a replication append, store it, and reply.
    async fn serve_replicate(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        prefix: &[u8; PREFIX_LEN],
    ) -> anyhow::Result<()> {
        let mut full = vec![0u8; REQUEST_HEADER_LEN];
        full[..PREFIX_LEN].copy_from_slice(prefix);
        stream
            .read_exact(&mut full[PREFIX_LEN..])
            .await
            .context("reading replicate header")?;
        let plen = AppendRequest::payload_len(&full).context("parsing replicate header")?;
        full.resize(REQUEST_HEADER_LEN + plen, 0);
        stream
            .read_exact(&mut full[REQUEST_HEADER_LEN..])
            .await
            .context("reading replicate payload")?;

        let req = AppendRequest::decode(&full).context("decoding replicate request")?;
        let resp = self.handle_replicate(&req)?;
        stream.write_all(&resp.encode()).await.context("writing replicate response")?;
        Ok(())
    }

    /// Read the rest of a read request, handle it, and reply.
    async fn serve_read(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        prefix: &[u8; PREFIX_LEN],
    ) -> anyhow::Result<()> {
        let mut full = vec![0u8; READ_REQUEST_LEN];
        full[..PREFIX_LEN].copy_from_slice(prefix);
        stream
            .read_exact(&mut full[PREFIX_LEN..])
            .await
            .context("reading read request")?;

        let req = ReadRequest::decode(&full).context("decoding read request")?;
        let resp = self.handle_read(&req)?;
        stream.write_all(&resp.encode()).await.context("writing read response")?;
        Ok(())
    }

    /// Read the rest of a vote request, handle it, and reply.
    async fn serve_vote(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        prefix: &[u8; PREFIX_LEN],
    ) -> anyhow::Result<()> {
        let mut full = vec![0u8; VOTE_REQUEST_LEN];
        full[..PREFIX_LEN].copy_from_slice(prefix);
        stream
            .read_exact(&mut full[PREFIX_LEN..])
            .await
            .context("reading vote request")?;

        let req = VoteRequest::decode(&full).context("decoding vote request")?;
        let resp = self.handle_vote(&req);
        stream.write_all(&resp.encode()).await.context("writing vote response")?;
        Ok(())
    }
}

/// Send a vote request to a peer and read its response.
async fn request_vote(addr: SocketAddr, bytes: &[u8]) -> anyhow::Result<VoteResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
    stream.write_all(bytes).await?;
    let mut resp = vec![0u8; VOTE_RESPONSE_LEN];
    stream.read_exact(&mut resp).await?;
    Ok(VoteResponse::decode(&resp)?)
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
