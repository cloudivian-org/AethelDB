<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: compaction & garbage collection

Status: **core complete** — `Repository::compact(gc_horizon)` plus branch-aware
`Tenant::gc(horizon)`, reachable over the control endpoint (`gc <lsn>`).

## Why

The page store appends a new version for every page change and freezes them into
immutable layers. Without maintenance two things grow without bound:

- **Read amplification** — a read scans the memtable plus *every* frozen layer.
- **Space** — every historical version is retained forever.

Compaction (merge the layer stack) and GC (drop versions outside the retention
window) bound both.

## Retention horizon

A single LSN, the **`gc_horizon`**, defines the point-in-time floor: the store
must remain able to reconstruct any page at any LSN ≥ `gc_horizon`, and is free
to forget everything below it. (A real deployment computes the horizon as
"current LSN − retention window".)

## Compaction + GC, in one pass

`Repository::compact(gc_horizon)`:

1. **Merge** every frozen layer's entries into one sorted map.
2. **GC-prune**: for each page, find the latest **base** (full image or
   `will_init` record) at or before `gc_horizon`; drop every version of that
   page strictly older than it. Reconstruction at any LSN ≥ `gc_horizon` starts
   from a base ≥ that one, so the dropped versions are unreachable.
   - A page with **no** base ≤ the horizon is left untouched — important on a
     branch, whose deltas sit on top of an *inherited* base and have no local
     base to prune against.
3. **Replace** the layer stack with the single compacted layer. The replaced
   layer ids are returned so their object-store files can be deleted.

## Branch-aware GC

A branch reads its parent **at the branch point** (`ancestor_lsn`), so the
parent must not collect history below it. `Tenant::gc(requested_horizon)` lowers
each timeline's effective horizon to

```
min(requested_horizon, min ancestor_lsn over its direct children)
```

Deeper descendants are covered transitively: every level pins its own parent at
its own branch point, so a chain `main ← dev ← feature` keeps `main` down to
`dev`'s branch point and `dev` down to `feature`'s. This guarantees GC can never
break a branch's inherited reads.

## Next

- **Object-store deletion** — delete the layer files named in
  `removed_layer_ids` from S3 after a successful compaction (today they are
  orphaned; correctness is unaffected, but space leaks).
- **Automatic policy** — a background worker that derives the horizon from a
  retention window and triggers compaction by layer count / size, rather than
  the explicit `gc` control command.
- **Layered compaction** — level the store (L0→L1…) instead of collapsing to a
  single layer, to keep compaction incremental as data grows.
