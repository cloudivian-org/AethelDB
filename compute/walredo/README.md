<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# wal-redo backend (compute side, Phase 3)

The page server materializes a page by replaying its WAL. Full-page images it
can install itself, but an ordinary WAL record (a heap insert, a btree split, …)
can only be applied by PostgreSQL's own resource-manager redo routines
(`RmgrTable[rmid].rm_redo`). Reimplementing those in Rust, bug-for-bug, is
infeasible and unsafe — a single mismatch silently corrupts a page.

So the page server ships the work to a child **wal-redo process**. This
directory is that process's compute-side source.

## How it fits together

```
 pageserver::walredo_process::PostgresRedoManager        compute/walredo/walredo.c
   gathers base image + the WAL records for one page  ──▶ reads RedoRequest (stdin)
                                                          applies via rm_redo
   receives the reconstructed 8 KiB page             ◀── writes RedoResponse (stdout)
```

The wire protocol is defined once, in [`pageserver/src/walredo_proto.rs`], and
spoken by both peers. [`walredo.c`](./walredo.c) implements the C side: the
framing, record decode, and the `rm_redo` dispatch.

## Status

- **Protocol + I/O loop + record decode:** complete in `walredo.c`.
- **The one Postgres-internal step** — running `rm_redo` with the target block
  faked into shared buffers — is isolated in `redo_apply_record()` and the
  `redo_*` helpers it calls. Those helpers are provided by a **`postgres
  --wal-redo` single-backend mode**: a small core patch (the same shape Neon
  uses) that brings up just enough of a backend for redo to run without a live
  cluster. Wiring that mode in is the remaining integration step; this file is
  structured to drop straight into it.

Until then, the page server is exercised against the Rust reference process
[`aethel-walredo-mock`](../../pageserver/src/bin/aethel-walredo-mock.rs), which
speaks the identical protocol with toy byte-edit semantics — enough to test all
of the page server's plumbing (framing, batching, restart, errors) end-to-end.
See `docs/design/wal-redo.md` for the full design.

## Build

`walredo.c` builds against an installed PostgreSQL 16 (`pg_config` on `PATH`)
once the `--wal-redo` core mode it links against is present:

```bash
make USE_PGXS=1
```
