# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Compute image: a PostgreSQL 16 server built from source so that Step 3 can
# patch its C storage manager (smgr) to fetch pages over the network and stream
# WAL externally. The build is a two-stage image: a fat `builder` that compiles
# Postgres, and a slim `runtime` that carries only the install tree.
#
# Source is cloned from the official GitHub mirror so the build works in
# network-restricted CI (no FTP). Override PG_REF to build a different tag.

# ---------------------------------------------------------------------------
# Stage 1: builder
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS builder

# REL_16_STABLE tracks the latest 16.x point release; pin to a tag for repro.
ARG PG_REF=REL_16_STABLE
ARG PG_REPO=https://github.com/postgres/postgres.git

# Build toolchain + the libraries Postgres links against. Kept minimal but
# sufficient for a realistic server (SSL, ICU, readline, zlib, libxml).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        ca-certificates \
        bison \
        flex \
        libreadline-dev \
        zlib1g-dev \
        libssl-dev \
        libicu-dev \
        libxml2-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
# Shallow-clone just the requested ref to keep the layer small and fast.
RUN git clone --depth 1 --branch "${PG_REF}" "${PG_REPO}" postgres

# Apply the AethelDB storage-manager patches (Step 3). Patches are applied
# in lexical order; 0001-smgr-pluggable.patch makes the smgr selectable.
COPY patches/ /patches/
# The network storage-manager extension source (built after the server below).
COPY extension/ /extension/
RUN set -eux; \
    cd /src/postgres; \
    shopt -s nullglob; \
    for p in $(ls /patches/*.patch 2>/dev/null | sort); do \
        echo "Applying $p"; \
        patch -p1 < "$p"; \
    done; \
    true

# Configure for the serverless compute role:
#   --with-openssl/icu/libxml : realistic feature parity
#   CFLAGS -O2                 : optimized but debuggable
# Step 3 will extend the configure flags to strip local-disk WAL writing and
# wire in the network storage manager.
RUN cd /src/postgres \
    && ./configure \
        --prefix=/usr/local/pgsql \
        --with-openssl \
        --with-icu \
        --with-libxml \
        CFLAGS="-O2 -pipe" \
    && make -j"$(nproc)" \
    && make install

# Build and install the network storage manager (aethel_smgr) into the server.
RUN cp -r /extension/aethel_smgr /src/postgres/contrib/aethel_smgr \
    && make -C /src/postgres/contrib/aethel_smgr \
    && make -C /src/postgres/contrib/aethel_smgr install

# ---------------------------------------------------------------------------
# Stage 2: runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Only the shared libraries the server loads at runtime — no compilers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libreadline8 \
        zlib1g \
        libssl3 \
        libicu72 \
        libxml2 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/postgresql postgres

COPY --from=builder /usr/local/pgsql /usr/local/pgsql
# Default compute-node configuration (network smgr + WAL streaming) + the
# environment-driven bootstrap entrypoint.
COPY postgresql.compute.conf /usr/local/pgsql/share/postgresql.compute.conf
COPY entrypoint.sh /usr/local/bin/aethel-compute-entrypoint
ENV PATH="/usr/local/pgsql/bin:${PATH}"
ENV PGDATA="/var/lib/postgresql/data"

USER postgres
WORKDIR /var/lib/postgresql

# The activator starts/stops these containers. The entrypoint renders the compute
# config from the environment (tenant / timeline / page server / safekeepers) and
# execs postgres — reads are served by the page server via aethel_smgr.
EXPOSE 5432
ENTRYPOINT ["/usr/local/bin/aethel-compute-entrypoint"]
