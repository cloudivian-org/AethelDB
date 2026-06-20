<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: proxy TLS termination (and the pooling decision)

Status: **TLS termination complete** — the activation proxy negotiates and
terminates TLS for client connections (`proxy/src/tls.rs`, rustls). Auth
passthrough and connection pooling are addressed below.

## TLS termination

PostgreSQL clients negotiate encryption *before* the startup packet: the client
sends an `SSLRequest`, and the server answers a single byte — `S` (proceed with
TLS) or `N` (continue in clear text).

When the proxy is configured with a certificate (`--tls-cert` / `--tls-key`,
PEM) it answers `S`, performs a rustls handshake, and then speaks the rest of the
protocol over the encrypted stream. TLS is **terminated at the proxy**; the hop
to the backend stays on the trusted local network (the proxy already lives in
front of compute). Without a certificate it answers `N` as before.

The data path was made generic over the client stream
(`AsyncRead + AsyncWrite`), so the same splice logic handles a plaintext
`TcpStream` or a `TlsStream` uniformly. The crypto backend is pinned to `ring`
to keep the build dependency-light.

```
client ──SSLRequest──▶ proxy ──'S'──▶ client
client ◀═══ TLS handshake (rustls) ═══▶ proxy
client ══ StartupMessage + session (encrypted) ══▶ proxy ──plaintext──▶ backend
```

Verified end to end: a real rustls client over a self-signed certificate
handshakes through the proxy and a payload round-trips through to the backend;
and with no certificate the proxy declines and the plaintext session still works.

## Connection pooling — compose, don't rebuild

Connection pooling (transaction/session pooling, server-connection reuse,
prepared-statement handling) is a deep, solved problem. AethelDB's proxy earns
its keep on what PgBouncer **doesn't** do — scale-to-zero activation, tenant
routing, and TLS termination — so pooling is **composed, not reimplemented**:

```
client → aethel-proxy (wake / route / TLS) → PgBouncer (pooling) → Postgres
```

Chaining PgBouncer behind the activation proxy keeps each layer focused; a Rust
pool crate (`deadpool` / `bb8`) is an option for in-process reuse later. This is
a deliberate decision not to build a PgBouncer equivalent from scratch.

## Next

- **Auth passthrough** — forward SCRAM-SHA-256 between client and backend (or
  terminate auth at the proxy against a tenant credential store).
- **CancelRequest routing** — track per-session backend key data so cancels
  reach the right backend.
- **mTLS to the backend** — if compute ever runs off the trusted local network.
