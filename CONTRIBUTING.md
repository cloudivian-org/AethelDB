<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Contributing to AethelDB

Thanks for your interest in AethelDB. This guide covers the dev setup, how to
build and test, and the conventions the project follows.

## Prerequisites

- **Rust** (stable; MSRV 1.75) — `rustup` recommended.
- **Docker** — for the local stack, MinIO (S3 tests), and the compute image.
- **A C toolchain + PostgreSQL build deps** — only if you build the patched
  PostgreSQL compute engine (`compute/`): a C compiler, `make`, and the usual PG
  build dependencies. On macOS, the Xcode command-line tools suffice.

## Build & test

The Rust services are a Cargo workspace:

```bash
make build           # cargo build --workspace
make test            # cargo test --workspace
cargo build --workspace --all-targets   # should produce ZERO warnings
```

**Every change must build and test green with zero warnings** before it is
merged. Treat warnings as errors.

### Cloud-integration tests (opt-in)

Some tests need external services and are skipped unless an env var is set:

```bash
# Real S3 against MinIO:
docker run -d -p 9100:9000 -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
# create the bucket "aethel", then:
AETHEL_S3_ENDPOINT=http://localhost:9100 cargo test -p pageserver --test s3
```

### Building the patched PostgreSQL

```bash
cd compute
make fetch           # shallow-clone PostgreSQL REL_16_STABLE into ./postgres-src
make patch           # apply patches/*.patch in order
make build           # configure + build into ./install
compute/walredo/verify.sh   # verify the --wal-redo backend against a real WAL record
```

`postgres-src/` and `install/` are gitignored; only the patches are committed.

## Project conventions

These are the practices the codebase already follows — please match them.

### One focused change per PR

Keep each PR scoped to a single subsystem or feature. The history is a sequence
of small, reviewable, independently-green commits. Branch off `main`
(`feat/<short-name>`), open a PR, and squash-or-merge once it's green and reviewed.

### A design doc per subsystem

Non-trivial subsystems get a short design doc under [`docs/design/`](docs/design/)
explaining *why* and *how*, with honest "what's not done yet" notes. Update the
relevant doc in the same PR that changes the behaviour.

### Tests prove the behaviour

Add tests for new behaviour — unit tests for pure logic, integration tests over
real sockets for the wire protocols and end-to-end paths. Prefer faithful tests
(real services, real bytes) over mocks where practical. State honestly in the PR
what is verified and what isn't.

### Code style

- Run `cargo fmt`; the repo ships `rustfmt.toml`.
- Match the surrounding code's comment density and idiom. Comments explain
  *why*, not *what*; module-level docs orient the reader and call out limitations.
- Errors: `thiserror` for typed library errors that cross crate boundaries,
  `anyhow` for application binaries. Don't `unwrap()` on fallible I/O in
  non-test code.
- Every new source file carries an `SPDX-License-Identifier: Apache-2.0` header
  and the copyright line.

### Wire protocols

The services exchange fixed, explicitly-encoded binary messages (see
`common/`). When you touch a protocol: keep the encoding explicit and
round-trip-tested, bump/define message types rather than overloading fields, and
update both peers in the same change.

### Compute (PostgreSQL) patches

Core PostgreSQL changes live as patches in `compute/patches/`, applied in lexical
order. Keep each patch focused and reversible (`patch -p1 -R` must cleanly
revert it), and verify it applies cleanly to `REL_16_STABLE` and compiles. See
`compute/patches/README.md`.

### Commit messages

- A concise subject line (what changed), then a body explaining the why and the
  notable details.
- End commits with the trailer used across the project's history, e.g.:
  ```
  Co-Authored-By: <name> <email>
  ```

## Security

AethelDB is database infrastructure. Please report security-sensitive issues
privately rather than in a public issue. Dual-use components (auth, TLS, WAL
handling) should be changed with care and clear tests.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, consistent with the rest of the project.
