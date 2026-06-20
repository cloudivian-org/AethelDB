// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! [`PostgresRedoManager`] — a [`WalRedoManager`] backed by a child wal-redo
//! process (Phase 3).
//!
//! Real WAL records (anything that isn't a full-page image) can only be applied
//! by Postgres's per-resource-manager redo routines, so this backend ships the
//! work to a child process speaking the [`crate::walredo_proto`] pipe protocol —
//! the real one being the C wal-redo backend in `compute/walredo/`, and a Rust
//! reference (`aethel-walredo-mock`) used by the tests.
//!
//! Within a page's version history this backend behaves like the native one for
//! the parts it can do itself — install a full-page image, apply a `ByteEdit`
//! delta — and batches consecutive raw WAL records into a single process call,
//! sending the current page as the base and replacing it with the result.
//!
//! The child is spawned lazily and supervised: any pipe/protocol error tears it
//! down so the next reconstruction respawns a fresh process. A single
//! `Mutex` serializes requests, so one process serves the node.

use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;

use common::{Lsn, PageKey, RelTag, PAGE_SIZE};
use tracing::{debug, warn};

use crate::page::PageVersion;
use crate::walredo::{RedoError, WalRedoManager};
use crate::walredo_proto::{
    RedoRequest, RESP_HEADER_LEN, RESP_MAGIC, STATUS_ERR, STATUS_OK, VERSION,
};

/// A wal-redo backend that drives a child process over the pipe protocol.
pub struct PostgresRedoManager {
    program: PathBuf,
    args: Vec<String>,
    proc: Mutex<Option<RedoProc>>,
}

/// A running wal-redo child and its pipes.
struct RedoProc {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Drop for RedoProc {
    fn drop(&mut self) {
        // Best-effort: close stdin (EOF asks the child to exit) then reap it.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl PostgresRedoManager {
    /// Create a manager that launches `program` (with `args`) as its wal-redo
    /// process. The process is not started until the first record needs redo.
    pub fn new(program: impl Into<PathBuf>, args: Vec<String>) -> Self {
        PostgresRedoManager { program: program.into(), args, proc: Mutex::new(None) }
    }

    /// Apply a batch of WAL records to a page through the child process.
    fn redo_page(
        &self,
        rel: RelTag,
        blkno: u32,
        base: Option<&[u8]>,
        records: &[(Lsn, Vec<u8>)],
    ) -> Result<Vec<u8>, RedoError> {
        let req = RedoRequest {
            rel,
            blkno,
            base_image: base.map(|b| b.to_vec()),
            records: records.to_vec(),
        };

        let mut guard = self.proc.lock().unwrap();
        // One retry: if a stale process fails, respawn and try once more.
        for attempt in 0..2 {
            if guard.is_none() {
                *guard = Some(self.spawn()?);
            }
            match Self::exchange(guard.as_mut().unwrap(), &req) {
                Ok(page) => return Ok(page),
                Err(RedoError::RedoFailed(m)) => return Err(RedoError::RedoFailed(m)),
                Err(e) => {
                    // Pipe/protocol failure: drop the child so we respawn.
                    warn!(error = %e, attempt, "wal-redo process failed; restarting");
                    *guard = None;
                    if attempt == 1 {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!("loop returns on success or on the final attempt")
    }

    /// Spawn the wal-redo child and capture its pipes.
    fn spawn(&self) -> Result<RedoProc, RedoError> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| RedoError::Process(format!("spawn {}: {e}", self.program.display())))?;
        let stdin = child.stdin.take().ok_or_else(|| RedoError::Process("no stdin pipe".into()))?;
        let stdout = child.stdout.take().ok_or_else(|| RedoError::Process("no stdout pipe".into()))?;
        debug!(program = %self.program.display(), "spawned wal-redo process");
        Ok(RedoProc { child, stdin, stdout: BufReader::new(stdout) })
    }

    /// Write one request and read one response over the child's pipes.
    fn exchange(proc: &mut RedoProc, req: &RedoRequest) -> Result<Vec<u8>, RedoError> {
        let bytes = req.encode();
        proc.stdin.write_all(&bytes).map_err(|e| RedoError::Process(format!("write request: {e}")))?;
        proc.stdin.flush().map_err(|e| RedoError::Process(format!("flush request: {e}")))?;

        let mut header = [0u8; RESP_HEADER_LEN];
        proc.stdout.read_exact(&mut header).map_err(|e| RedoError::Process(format!("read response header: {e}")))?;
        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        if magic != RESP_MAGIC {
            return Err(RedoError::Process(format!("bad response magic {magic:#010x}")));
        }
        if header[4] != VERSION {
            return Err(RedoError::Process(format!("bad response version {}", header[4])));
        }
        match header[5] {
            STATUS_OK => {
                let mut page = vec![0u8; PAGE_SIZE];
                proc.stdout.read_exact(&mut page).map_err(|e| RedoError::Process(format!("read page: {e}")))?;
                Ok(page)
            }
            STATUS_ERR => {
                let mut len = [0u8; 4];
                proc.stdout.read_exact(&mut len).map_err(|e| RedoError::Process(format!("read error len: {e}")))?;
                let mut msg = vec![0u8; u32::from_be_bytes(len) as usize];
                proc.stdout.read_exact(&mut msg).map_err(|e| RedoError::Process(format!("read error msg: {e}")))?;
                Err(RedoError::RedoFailed(String::from_utf8_lossy(&msg).into_owned()))
            }
            other => Err(RedoError::Process(format!("unknown response status {other}"))),
        }
    }
}

impl WalRedoManager for PostgresRedoManager {
    fn reconstruct(
        &self,
        key: PageKey,
        _request_lsn: Lsn,
        versions: &[(Lsn, &PageVersion)],
    ) -> Result<Option<Vec<u8>>, RedoError> {
        let base = match versions.iter().rposition(|(_, v)| v.is_base()) {
            Some(i) => i,
            None => return Ok(None),
        };

        // Materialize the base, accumulating raw WAL records to flush through
        // the process whenever a natively-applied version interrupts the run.
        let mut page = vec![0u8; PAGE_SIZE];
        let mut have_base; // does `page` currently hold a real base image?
        let mut pending: Vec<(Lsn, Vec<u8>)> = Vec::new();

        match versions[base].1 {
            PageVersion::Image(_) => {
                versions[base].1.apply_to(&mut page)?;
                have_base = true;
            }
            // A will_init record reinitializes from zeros: it is itself the
            // first record to apply, with no base image.
            PageVersion::WalRecord(w) => {
                pending.push((versions[base].0, w.rec.clone()));
                have_base = false;
            }
            PageVersion::Delta(_) => unreachable!("delta cannot be a reconstruction base"),
        }

        for (lsn, v) in &versions[base + 1..] {
            match v {
                PageVersion::WalRecord(w) => pending.push((*lsn, w.rec.clone())),
                // A native version (image/delta) interrupts the record run:
                // flush what we have, then apply it directly.
                _ => {
                    self.flush(key.rel, key.block, &mut page, &mut have_base, &mut pending)?;
                    v.apply_to(&mut page)?;
                    have_base = true;
                }
            }
        }
        self.flush(key.rel, key.block, &mut page, &mut have_base, &mut pending)?;
        Ok(Some(page))
    }
}

impl PostgresRedoManager {
    /// Send any accumulated records to the process, replacing `page` with the
    /// result and clearing the batch.
    fn flush(
        &self,
        rel: RelTag,
        blkno: u32,
        page: &mut Vec<u8>,
        have_base: &mut bool,
        pending: &mut Vec<(Lsn, Vec<u8>)>,
    ) -> Result<(), RedoError> {
        if pending.is_empty() {
            return Ok(());
        }
        let base = if *have_base { Some(page.as_slice()) } else { None };
        let result = self.redo_page(rel, blkno, base, pending)?;
        page.copy_from_slice(&result);
        *have_base = true;
        pending.clear();
        Ok(())
    }
}
