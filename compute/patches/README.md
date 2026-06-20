# Compute patches

PostgreSQL source patches that turn a stock PostgreSQL 16 server into a
AethelDB compute node. They are applied in lexical filename order by both
`compute/Makefile` (`make patch`) and `compute/Dockerfile`.

## Patches

- **`0001-smgr-pluggable.patch`** *(Step 3)* — makes the storage manager
  pluggable. It publishes the `f_smgr` function table in `storage/smgr.h`,
  replaces the per-relation `smgr_which` index with a `const f_smgr *smgr`
  pointer, and adds an `smgr_hook` / `smgr_init_hook` plus `smgr_standard()` so
  an extension can serve a relation's pages from somewhere other than local
  disk. Verified to apply cleanly to `REL_16_STABLE` (`git apply --check`) and
  to compile under PostgreSQL's strict warning flags.

- **`0002-wal-redo-mode.patch`** *(WAL decode/redo, Phase 3)* — adds a
  `postgres --wal-redo` single-backend mode: an out-of-process WAL-redo server
  for the page server. It reuses single-user initialization, then runs a
  request/response loop (the protocol in `pageserver/src/walredo_proto.rs`)
  instead of the SQL loop, applying WAL records to one page via the resource
  managers' `rm_redo` routines. The target relation's storage is routed to
  memory through the `smgr_hook` from 0001, so redo reads/writes hit the
  page server's base image. New file `src/backend/access/transam/walredo.c`
  (+ `access/walredo.h`); small hooks in `main.c` and `postgres.c`. Depends on
  0001. Applies cleanly to `REL_16_STABLE`, compiles from scratch, and is
  verified to reconstruct a page byte-identically from a real heap-insert WAL
  record. See `compute/walredo/README.md`.

The network storage manager itself ships as a normal extension under
`compute/extension/aethel_smgr/` (built by `make extension` / the Dockerfile) rather
than as a core patch, so it can be developed and tested independently.

## Authoring a patch

```bash
cd compute
make fetch                 # clean source tree in ./postgres-src
cd postgres-src
git checkout -b my-change
# ...edit C sources...
git diff > ../patches/0002-my-change.patch
```

Keep each patch focused and reversible (`patch -p1 -R` must cleanly revert it).
