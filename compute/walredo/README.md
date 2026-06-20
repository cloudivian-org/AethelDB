<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# wal-redo backend (compute side, Phase 3)

The page server materializes a page by replaying its WAL. Full-page images it
installs itself, but an ordinary WAL record (a heap insert, a btree split, …)
can only be applied by PostgreSQL's own resource-manager redo routines
(`RmgrTable[rmid].rm_redo`). Reimplementing those in Rust, bug-for-bug, is
infeasible and unsafe — a single mismatch silently corrupts a page.

So the page server ships the work to a child **`postgres --wal-redo`** process.

## How it fits together

```
 pageserver::walredo_process::PostgresRedoManager        postgres --wal-redo
   gathers base image + the WAL records for one page  ──▶ reads RedoRequest (stdin)
                                                          applies via rm_redo
   receives the reconstructed 8 KiB page             ◀── writes RedoResponse (stdout)
```

The wire protocol is defined once, in [`pageserver/src/walredo_proto.rs`], and
spoken by both peers (and by the Rust reference process `aethel-walredo-mock`).

## Implementation

The backend is a **core patch**, not a standalone program, because it needs the
full backend environment (shared buffers, smgr) to run `rm_redo`:

- **`compute/patches/0002-wal-redo-mode.patch`** adds
  `src/backend/access/transam/walredo.c` (+ `access/walredo.h`) and small hooks
  in `main.c` / `postgres.c`. `postgres --wal-redo` reuses single-user
  initialization, then runs the redo protocol loop instead of the SQL loop.
- The target relation's storage is routed to memory via the `smgr_hook` from
  `0001-smgr-pluggable.patch`: redo's buffer reads/writes resolve to the base
  image the page server handed in, not a disk file. Any other block a record
  happens to touch reads back as zeros and is harmless — only the target block's
  result is returned.

## Build

`make -C compute all` clones PostgreSQL 16 (`REL_16_STABLE`), applies `0001` and
`0002`, and builds `compute/install/bin/postgres` with the `--wal-redo` mode.
The page server is pointed at that binary:

```bash
aethel-pageserver ... # PostgresRedoManager::new("compute/install/bin/postgres",
                      #   ["--wal-redo", "-D", "<datadir>", "postgres"])
```

## Verification

`compute/walredo/verify.sh` reproduces the end-to-end proof against a built
install: it `initdb`s a throwaway cluster, has a real server emit a heap-insert
WAL record (with `full_page_writes=off`, so a true delta — not a full-page
image), then drives `postgres --wal-redo` with that record and compares the
reconstructed page to the page the live server wrote.

The reconstructed page is **byte-identical** to the server's, except the
`pd_lsn` header stamp (which differs only by WAL framing — the page server
assigns authoritative LSNs anyway). `test_real_redo.py` does the extraction and
comparison.

```bash
make -C compute all                 # build compute/install/bin/postgres
compute/walredo/verify.sh           # initdb + generate + redo + compare
```

See `docs/design/wal-redo.md` for the full design.
