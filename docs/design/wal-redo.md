<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: WAL decode & redo

Status:
- **Phase 1 complete** — WAL stream framing + record decoder
  (`pageserver/src/waldecode.rs`, 10 unit tests).
- **Phase 2 complete (library)** — `PageVersion::WalRecord` storage variant, the
  `WalRedoManager` trait + `RustApplyRedoManager` (`pageserver/src/walredo.rs`),
  reconstruction routed through the redo backend, and `Repository::ingest_wal`
  feeding real WAL bytes through framing → decode → store → reconstruct.
- **Phase 4 complete** — the safekeeper→page-server link is live. A WAL-read
  protocol (`common::wal_service::ReadRequest`/`ReadResponse`), a safekeeper read
  endpoint (`Safekeeper::handle_read`), and a streaming `WalReceiver`
  (`pageserver/src/walreceiver.rs`) with a long-lived `WalStreamDecoder` pull
  committed WAL and ingest it via `Repository::ingest_record`. Wired into the
  `aethel-pageserver` binary behind `--safekeeper`. An end-to-end test drives a
  real safekeeper + receiver + repository over sockets and reconstructs a page.
- **Phase 3 next** — the Postgres wal-redo process, after which non-FPI WAL
  records (not just full-page images) materialize correctly.

## Why

The page server's job is to answer *"give me page `(rel, fork, blk)` exactly as
it stood at LSN `Y`."* It does that by storing a base image plus the stream of
changes after it and replaying the changes up to `Y`.

Today those changes arrive as abstract `Modification`s carrying `ByteEdit`s — a
*model* of WAL, hand-fed by tests. Two things are missing for this to be a real
serverless Postgres:

1. **Decode.** Nothing turns a real PostgreSQL **WAL stream** into per-page
   change records. Postgres emits WAL, not `ByteEdit`s.
2. **Redo.** Replaying a non-full-page WAL record onto an 8 KiB page requires
   Postgres's *own* per-resource-manager redo routines (heap, btree, gin, gist,
   spgist, sequences, …). Reimplementing them in Rust, bug-for-bug, is
   infeasible and unsafe — a single mismatch silently corrupts a page.

This subsystem supplies both.

## Background: what a WAL record actually contains

A PostgreSQL WAL record has a fixed `XLogRecord` header followed by a body that
is, deliberately, **two layers**:

- A *generic* layer the core WAL machinery (`xloginsert.c` / `xlogreader.c`)
  owns: a list of **registered block references**. Each says *which* block the
  record touches — `(RelFileLocator spc/db/rel, ForkNumber, BlockNumber)` — and
  optionally carries a **full-page image (FPI)** of that block and/or a chunk of
  per-block data.
- An *rmgr-specific* layer (the "main data" and the per-block data) that only
  the owning resource manager understands.

The crucial consequence: **identifying which pages a record modifies, and
extracting any FPIs, needs only the generic layer.** A decoder can route every
WAL record to the right page(s) *without* understanding heap or btree internals.
That is what makes Phase 1 a self-contained, pure-Rust, fully testable unit.

Applying a *non-FPI* change, however, needs the rmgr layer — hence redo
(Phase 3) defers to a real Postgres process.

## Architecture

```
 safekeeper ──committed WAL bytes──▶ WalReceiver (Phase 4)
                                          │
                                          ▼
                                  WalStreamDecoder        ── Phase 1 ──
                            (page framing + continuations)
                                          │  yields
                                          ▼
                                  DecodedWalRecord
                            { lsn, rmid, main_data, blocks[] }
                                          │  per block
                          ┌───────────────┴───────────────┐
                  has FPI │                                │ no FPI
                          ▼                                ▼
                 PageVersion::Image(page)         PageVersion::WalRecord(bytes)
                          │                                │   ── Phase 2 ──
                          └───────────────┬────────────────┘
                                          ▼
                                Repository (index by (PageKey, Lsn))
                                          │  GetPage @ Y
                                          ▼
                                  WalRedoManager  ── Phase 2/3 ──
                       base image + [WalRecord…]  ──▶  redone 8 KiB page
                          ┌───────────────┴───────────────┐
              Rust apply  │                                │  PostgresRedoManager
            (FPI + edits) ▼                                ▼   (child wal-redo PG,
            keeps tests green                       pipe protocol, real rmgr redo)
```

## Components & types

### Phase 1 — `pageserver/src/waldecode.rs`

- `WalStreamDecoder` — feed it WAL bytes starting at a known LSN; it strips the
  per-page `XLogPageHeader`s, stitches records that span page boundaries
  (`xlp_rem_len`), validates `xl_tot_len`, and yields whole record images with
  their start LSN.
- `decode_wal_record(lsn, &[u8]) -> DecodedWalRecord` — parses the record body's
  generic layer:
  - `DecodedWalRecord { lsn, end_lsn, rmid, info, xid, main_data, blocks }`
  - `DecodedBlock { rel: RelTag, blkno, flags, image: Option<DecodedImage>, data: Range }`
  - `DecodedImage` carries the stored bytes plus the **hole** (`hole_offset`,
    `hole_length`) and **compression** method, and can `restore()` the full
    8 KiB page (zero-filling the hole; decompressing pglz/lz4/zstd).
- Block-header flags honoured: `BKPBLOCK_FORK_MASK`, `HAS_IMAGE`, `HAS_DATA`,
  `WILL_INIT`, `SAME_REL`; image flags `BKPIMAGE_HAS_HOLE`, `APPLY`, and the
  compression flags.

No I/O, no Postgres — exhaustively unit-tested against format-accurate records
built in-test, and later against captured real WAL fixtures.

### Phase 2 — storage + redo seam

- New `PageVersion::WalRecord(Bytes)` variant: stores the **raw WAL record** for
  a block whose change isn't a full image. (FPIs still become
  `PageVersion::Image`.)
- `trait WalRedoManager { fn redo(&self, key, base: Option<&[u8]>, records: &[WalRecord], request_lsn) -> Result<Page> }`.
- `RustApplyRedoManager` — handles `Image` + `Delta(ByteEdit)` exactly as today,
  so all existing tests keep passing while the new path is introduced.
- Raw-WAL ingest path on the page server: `IngestWal { start_lsn, bytes }` →
  `WalStreamDecoder` → `decode_wal_record` → `repo.ingest_wal(...)`. The existing
  `Modification` ingest stays for back-compat/tests.

### Phase 3 — `PostgresRedoManager`

- Extend the compute patch (`0001-smgr-pluggable.patch`) with a **`--wal-redo`**
  single-backend mode: read `(BufferTag, base page, [WAL records])` over a pipe,
  call `RmgrTable[rmid].rm_redo`, write back the 8 KiB result. This is the only
  correct way to apply arbitrary rmgr changes.
- Pageserver spawns and supervises one wal-redo process per tenant (recycled on
  crash), with a timeout and a bounded request queue.

### Phase 4 — `WalReceiver`

- Pageserver connects to the safekeeper, streams committed WAL from its current
  ingest LSN, feeds the decoder, and advances a persisted `last_ingested_lsn`.
  Closes the safekeeper→pageserver gap; the e2e suite switches to driving real
  WAL end to end.

## Correctness notes

- **Compression.** PG15+ may store FPIs compressed (pglz / lz4 / zstd). The
  decoder records which method was used; Phase 1's `DecodedImage::restore`
  reconstructs **uncompressed** images (re-inserting the hole) and returns a
  typed `UnsupportedCompression` error for pglz/lz4/zstd until the matching
  decompressor is wired (Phase 2), so we never hand back a wrong page.
- **`WILL_INIT`.** A record that re-initializes a page needs no base image — redo
  starts from zeroes. The decoder surfaces the flag so the repository can drop
  the now-irrelevant history before it.
- **Alignment.** Records are `MAXALIGN`ed (8 bytes) in the stream; the stream
  decoder accounts for inter-record padding.
- **CRC.** `xl_crc` (CRC-32C) validation is wired in Phase 1 behind a flag and
  on by default for the receiver path; malformed records are rejected, not
  applied.

## Testing strategy

- Phase 1: byte-exact round-trips — build an `XLogRecord` with N block refs, an
  FPI with a hole, `SAME_REL` reuse, and continuation across a page boundary;
  assert the decoder recovers every field and restores the page.
- Phase 3: differential test — apply the same WAL via the wal-redo process and
  via a real Postgres recovering normally; assert identical page images.
- Phase 4: the existing lifecycle e2e, but the page server materializes from
  real streamed WAL rather than hand-fed `Modification`s.
