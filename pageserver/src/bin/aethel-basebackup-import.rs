// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! `aethel-basebackup-import` — seed a page store from an `initdb`'d data dir.
//!
//! A patched-Postgres compute (`aethel_smgr`) serves every non-temp page from
//! the page server. A freshly provisioned timeline has **no** pages, so the
//! compute can't boot. This tool imports the base image: it walks a local
//! PostgreSQL data directory and pushes every relation block to the page
//! server's ingest port as a full-page-image [`Modification`], reusing the exact
//! ingest wire format. After importing, the real compute can boot and serve
//! reads from the page store.
//!
//! Pages are written at the requested LSN (default 0). `aethel_smgr` requests
//! pages at LSN 0 ("latest materialized"), and the page server reads a page
//! *as of* the requested LSN, so the base image must be at an LSN ≤ that — 0 is
//! the safe floor for the initial import.
//!
//! Usage:
//!   aethel-basebackup-import --pgdata <DIR> --ingest <HOST:PORT> [--lsn N]
//!
//! Notes / current limits (intentionally explicit):
//!   * Imports the `global/` (spc 1664) and `base/<db>/` (spc 1663) relation
//!     forks (main / fsm / vm). Segmented relations (>1 GiB, `<rel>.N`) and the
//!     init fork are skipped — a fresh initdb has none.
//!   * The ingest endpoint targets one timeline (the tenant root); run one
//!     import per timeline you want to seed.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;

use common::{ForkNumber, Lsn, RelTag, PAGE_SIZE};
use pageserver::{Modification, PageVersion};

/// PostgreSQL's `GLOBALTABLESPACE_OID` — shared catalogs under `global/`.
const GLOBAL_SPC: u32 = 1664;
/// PostgreSQL's `DEFAULTTABLESPACE_OID` — per-database relations under `base/`.
const DEFAULT_SPC: u32 = 1663;

fn main() -> anyhow::Result<()> {
    let (mut pgdata, mut ingest, mut lsn) = (None, None, 0u64);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--pgdata" => pgdata = args.next(),
            "--ingest" => ingest = args.next(),
            "--lsn" => {
                lsn = args
                    .next()
                    .unwrap_or_default()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--lsn must be a non-negative integer"))?
            }
            "-h" | "--help" => {
                println!(
                    "usage: aethel-basebackup-import --pgdata <DIR> --ingest <HOST:PORT> [--lsn N]"
                );
                return Ok(());
            }
            other => anyhow::bail!("unexpected argument `{other}` (try --help)"),
        }
    }
    let pgdata = pgdata.ok_or_else(|| anyhow::anyhow!("--pgdata is required"))?;
    let ingest = ingest.ok_or_else(|| anyhow::anyhow!("--ingest is required"))?;

    let mut sock = TcpStream::connect(&ingest)
        .map_err(|e| anyhow::anyhow!("connecting to page-server ingest {ingest}: {e}"))?;

    let mut total = 0usize;
    let global = Path::new(&pgdata).join("global");
    if global.is_dir() {
        total += import_dir(&mut sock, &global, GLOBAL_SPC, 0, lsn)?;
    }
    let base = Path::new(&pgdata).join("base");
    if base.is_dir() {
        let mut dbs: Vec<_> = fs::read_dir(&base)?.filter_map(|e| e.ok()).collect();
        dbs.sort_by_key(|e| e.file_name());
        for e in dbs {
            let path = e.path();
            if path.is_dir() {
                if let Some(db) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) {
                    total += import_dir(&mut sock, &path, DEFAULT_SPC, db, lsn)?;
                }
            }
        }
    }
    println!("imported {total} pages from {pgdata} -> {ingest} at lsn {lsn}");
    Ok(())
}

/// Parse a relation file name into `(relfilenode, fork)`, or `None` if it isn't
/// a relation file (e.g. `PG_VERSION`, `pg_filenode.map`, a `.N` segment).
fn parse_relfile(name: &str) -> Option<(u32, ForkNumber)> {
    let (num, fork) = match name.rsplit_once('_') {
        Some((n, "fsm")) => (n, ForkNumber::Fsm),
        Some((n, "vm")) => (n, ForkNumber::VisibilityMap),
        _ => (name, ForkNumber::Main),
    };
    // A bare relfilenode is all digits; anything else (segments, maps) is skipped.
    let rel: u32 = num.parse().ok()?;
    Some((rel, fork))
}

/// Import every relation block under `dir` for `(spc, db)`.
fn import_dir(
    sock: &mut TcpStream,
    dir: &Path,
    spc: u32,
    db: u32,
    lsn: u64,
) -> anyhow::Result<usize> {
    let mut entries: Vec<_> = fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    let mut n = 0;
    for e in entries {
        let fname = e.file_name();
        let Some(name) = fname.to_str() else { continue };
        let Some((rel, fork)) = parse_relfile(name) else { continue };
        let data = fs::read(e.path())?;
        for (blk, chunk) in data.chunks(PAGE_SIZE).enumerate() {
            let mut page = chunk.to_vec();
            page.resize(PAGE_SIZE, 0); // pad a short final block
            let m = Modification {
                rel: RelTag { spc_node: spc, db_node: db, rel_node: rel, fork },
                block: blk as u32,
                lsn: Lsn(lsn),
                version: PageVersion::Image(page),
            };
            let body = m.encode();
            sock.write_all(&(body.len() as u32).to_be_bytes())?;
            sock.write_all(&body)?;
            let mut ack = [0u8; 1];
            sock.read_exact(&mut ack)?;
            anyhow::ensure!(
                ack[0] == 0,
                "page server rejected {name} block {blk} (ack {})",
                ack[0]
            );
            n += 1;
        }
    }
    Ok(n)
}
