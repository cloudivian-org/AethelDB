// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! `aethel-walredo-mock` — a reference wal-redo process for tests.
//!
//! It speaks the exact [`pageserver::walredo_proto`] pipe protocol the real C
//! wal-redo backend speaks (read a [`RedoRequest`] on stdin, write a
//! [`RedoResponse`] on stdout), but with **toy** apply semantics instead of
//! Postgres redo: each WAL record is a sequence of `[offset:u16][len:u16][data]`
//! big-endian byte edits applied onto the base page. That is enough to exercise
//! the page server's process plumbing (framing, batching, restart, errors)
//! end-to-end without a Postgres build; the real backend swaps the apply step
//! for `RmgrTable[rmid].rm_redo`.
//!
//! Flags (for tests): `--fail` always replies with an error; `--exit-after=N`
//! serves N requests then exits (to exercise crash/restart).

use std::io::{self, Read, Write};

use common::PAGE_SIZE;
use pageserver::walredo_proto::{RedoRequest, RedoResponse, FLAG_HAS_BASE};

fn main() -> io::Result<()> {
    let mut fail = false;
    let mut exit_after: Option<u64> = None;
    for arg in std::env::args().skip(1) {
        if arg == "--fail" {
            fail = true;
        } else if let Some(n) = arg.strip_prefix("--exit-after=") {
            exit_after = n.parse().ok();
        }
    }

    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    let mut served = 0u64;
    while let Some(req) = read_request(&mut reader)? {
        let resp = if fail {
            RedoResponse::Error("forced failure (mock --fail)".to_string())
        } else {
            let mut page = req.base_image.clone().unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
            for (_lsn, rec) in &req.records {
                apply_toy_edits(&mut page, rec);
            }
            RedoResponse::Page(page)
        };
        writer.write_all(&resp.encode())?;
        writer.flush()?;

        served += 1;
        if exit_after == Some(served) {
            break;
        }
    }
    Ok(())
}

/// Apply toy `[offset:u16][len:u16][data]` big-endian edits onto `page`.
fn apply_toy_edits(page: &mut [u8], rec: &[u8]) {
    let mut i = 0;
    while i + 4 <= rec.len() {
        let off = u16::from_be_bytes([rec[i], rec[i + 1]]) as usize;
        let len = u16::from_be_bytes([rec[i + 2], rec[i + 3]]) as usize;
        i += 4;
        if i + len > rec.len() {
            break;
        }
        if off + len <= page.len() {
            page[off..off + len].copy_from_slice(&rec[i..i + len]);
        }
        i += len;
    }
}

/// Read one framed [`RedoRequest`] from `r`, or `None` on clean EOF.
fn read_request<R: Read>(r: &mut R) -> io::Result<Option<RedoRequest>> {
    // Fixed 28-byte prefix: magic..blkno. A clean EOF here ends the stream.
    let mut buf = vec![0u8; 28];
    match r.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let flags = buf[5];
    if flags & FLAG_HAS_BASE != 0 {
        let mut base = vec![0u8; PAGE_SIZE];
        r.read_exact(&mut base)?;
        buf.extend_from_slice(&base);
    }
    // Record count, then each record's (lsn, len, bytes).
    let mut nbuf = [0u8; 4];
    r.read_exact(&mut nbuf)?;
    buf.extend_from_slice(&nbuf);
    let n = u32::from_be_bytes(nbuf);
    for _ in 0..n {
        let mut hdr = [0u8; 12]; // lsn(8) + len(4)
        r.read_exact(&mut hdr)?;
        let len = u32::from_be_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
        buf.extend_from_slice(&hdr);
        let mut rec = vec![0u8; len];
        r.read_exact(&mut rec)?;
        buf.extend_from_slice(&rec);
    }

    RedoRequest::decode(&buf).map(Some).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}
