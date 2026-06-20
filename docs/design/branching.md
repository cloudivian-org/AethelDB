<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: branching & point-in-time

Status: **complete & wired** — `Tenant` + `Timeline` with copy-on-write
reconstruction across the ancestor chain (`pageserver/src/{tenant,timeline}.rs`),
wired into the `aethel-pageserver` binary: the page service routes reads by the
request's timeline, the offload worker covers every timeline, the WAL receiver
targets a timeline, and a control endpoint (`pageserver/src/control.rs`) creates
branches at runtime. An end-to-end test branches over real sockets and asserts
isolation.

## Why

Two of the headline serverless features — **instant branching** and
**point-in-time** reads — fall out of the page server already storing every
page version keyed by LSN:

- **Point-in-time (PITR)** is just `get_page(key, lsn)` at a past `lsn`: the
  reconstruction engine already replays history up to any LSN.
- **Branching** is creating a new *timeline* that shares its parent's history up
  to a chosen LSN and diverges after it — copying nothing.

## Model

- A **tenant** is one isolated database instance; it owns a set of timelines.
- A **timeline** is one branch of history. The root has no ancestor; a branch
  records its **parent** and the **branch-point LSN** (`ancestor_lsn`).
- Each timeline owns a `Repository` holding only the changes written *on that
  branch*. Branching allocates a new `TimelineId` and an empty store — O(1), no
  data copied.

```
        main ──●────●────●────●────●──▶   (LSN increases →)
               10   20   30   40   50
                     │
                     └── branch @20:  ●────●──▶
                                      30'   40'
```

## Copy-on-write reconstruction

To read page `K` at LSN `Y` on a branch:

1. If the branch has its own self-sufficient history for `K` — a full image or a
   `will_init` record at or before `Y` — reconstruct from it. (This is the case
   when the branch rewrote the whole page.)
2. Otherwise **inherit**: read `K` from the parent **at `ancestor_lsn`**, use it
   as the base image, and replay this branch's deltas (LSN in
   `(ancestor_lsn, Y]`) on top.

Step 2 recurses up the ancestor chain, so a grandchild inherits from its
grandparent transparently. Because a branch stores only its own writes, writing
to a branch never touches its parent — isolation is automatic, and the parent
can keep advancing without the branch seeing it.

This composes with the WAL-redo subsystem: an inherited base plus the branch's
real WAL records are handed to the same `WalRedoManager`, so branches redo real
Postgres WAL exactly as the root does.

`get_rel_size` follows the same rule: a branch's fork size is the larger of its
own and the size inherited from the ancestor at the branch point (a branch may
extend a relation independently).

## API

```rust
let tenant = Tenant::new(freeze_threshold);
let main   = tenant.create_timeline(main_id)?;           // root
main.ingest_wal(start_lsn, &wal)?;                       // stream WAL in
let dev    = tenant.branch_timeline(dev_id, main_id, lsn)?;  // instant branch @lsn
dev.ingest(/* writes diverge here */);
dev.get_page(key, at_lsn)?;                              // CoW read
```

## Control endpoint

Branching is a control-plane action, so it gets a small line-oriented endpoint
(`pageserver/src/control.rs`) rather than space on the hot path:

```
create <timeline-hex>                  -> ok created <id>
branch <new-hex> <parent-hex> <lsn>    -> ok branched <new> from <parent> @ <lsn>
list                                   -> ok <id> <id> ...
```

## Next

- **Per-branch network ingest** — today the WAL receiver and ingest endpoint
  target the root timeline; a branch is written via its `Timeline` handle. A
  per-branch receiver (one WAL stream per timeline) lands with the control plane.
- **Retention / GC** — bound how far back PITR reaches, and keep a branch's
  `ancestor_lsn` pinned so its base history is never collected.
