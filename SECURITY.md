<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Security Policy

AethelDB is database infrastructure — it handles WAL, page data, authentication,
and TLS. We take security reports seriously.

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately via GitHub's **[Private Vulnerability Reporting](https://github.com/cloudivian-org/AethelDB/security/advisories/new)**
(Security → Advisories → "Report a vulnerability"). This keeps the details
private while we investigate and prepare a fix.

Please include:

- a description of the vulnerability and its impact,
- steps to reproduce (a minimal proof-of-concept if possible),
- affected component(s) and version/commit.

We aim to acknowledge a report within a few business days and to keep you updated
as we work on a fix. We'll credit you in the advisory unless you prefer to remain
anonymous.

## Supported versions

AethelDB is pre-1.0; security fixes land on `main` and in the latest release.
Until a `1.0` release line exists, only the latest tagged release and `main` are
supported.

## Scope

In scope: the proxy (TLS, SCRAM, connection handling), the safekeeper (WAL
durability, replication, voting), the page server (WAL decode/redo, storage,
control plane), and the compute patches. Dual-use components (auth, TLS, WAL
handling) should be changed with care and clear tests.

Out of scope: vulnerabilities in third-party dependencies (report those
upstream), and issues that require a misconfiguration explicitly warned against
in the docs (e.g. mounting the host Docker socket, static dev credentials).
