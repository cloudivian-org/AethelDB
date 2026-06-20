# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Local (host) build of the patchable PostgreSQL compute engine. This mirrors
# what the Dockerfile does, but checks the source out into ./postgres-src so a
# developer can iterate on the Step 3 smgr patches without rebuilding an image.
#
# Usage:
#   make fetch     # shallow-clone the Postgres source at $(PG_REF)
#   make patch     # apply every patches/*.patch in order
#   make build     # configure + make + make install into ./install
#   make all       # fetch + patch + build
#   make clean      # remove the source checkout and install tree

PG_REF   ?= REL_16_STABLE
PG_REPO  ?= https://github.com/postgres/postgres.git
SRC_DIR  := postgres-src
PREFIX   := $(abspath install)
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)

.PHONY: all fetch patch build extension clean

all: fetch patch build extension

fetch:
	@if [ ! -d "$(SRC_DIR)/.git" ]; then \
		echo ">> cloning $(PG_REPO) @ $(PG_REF)"; \
		git clone --depth 1 --branch "$(PG_REF)" "$(PG_REPO)" "$(SRC_DIR)"; \
	else \
		echo ">> $(SRC_DIR) already present, skipping clone"; \
	fi

patch:
	@echo ">> applying patches (if any)"
	@for p in $$(ls patches/*.patch 2>/dev/null | sort); do \
		echo "   applying $$p"; \
		( cd "$(SRC_DIR)" && patch -p1 < "../$$p" ); \
	done; \
	true

build:
	@echo ">> configuring + building into $(PREFIX)"
	cd "$(SRC_DIR)" && ./configure \
		--prefix="$(PREFIX)" \
		--with-openssl \
		--with-icu \
		--with-libxml \
		CFLAGS="-O2 -pipe"
	cd "$(SRC_DIR)" && make -j"$(JOBS)" && make install
	@echo ">> built: $(PREFIX)/bin/postgres"

extension:
	@echo ">> building aethel_smgr extension"
	cp -r extension/aethel_smgr "$(SRC_DIR)/contrib/aethel_smgr"
	$(MAKE) -C "$(SRC_DIR)/contrib/aethel_smgr"
	$(MAKE) -C "$(SRC_DIR)/contrib/aethel_smgr" install
	@echo ">> installed aethel_smgr into $(PREFIX)/lib"

clean:
	rm -rf "$(SRC_DIR)" "$(PREFIX)"
