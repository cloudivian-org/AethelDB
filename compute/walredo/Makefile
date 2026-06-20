# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Builds the wal-redo backend against an installed PostgreSQL 16.
# Requires the `postgres --wal-redo` core mode this program links against
# (see ./README.md). Build with: make USE_PGXS=1

PROGRAM = aethel-walredo
OBJS = walredo.o

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

# Compile/link flags pulled from the target server's configuration.
PG_CPPFLAGS := -I$(shell $(PG_CONFIG) --includedir-server)

include $(PGXS)
