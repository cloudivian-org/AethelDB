<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

## What & why

<!-- What does this change, and why? Link any related issue. -->

## How it was verified

<!-- Tests added/updated; what's proven and what isn't. Prefer real services /
     real bytes over mocks where practical. -->

## Checklist

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` is clean
- [ ] `cargo test --workspace` passes (zero warnings)
- [ ] Tests cover the new behavior
- [ ] A design doc under `docs/design/` was added/updated if this is a subsystem change
- [ ] New source files carry the `SPDX-License-Identifier: Apache-2.0` header
