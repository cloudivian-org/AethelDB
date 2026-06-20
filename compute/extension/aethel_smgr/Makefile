# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors

MODULE_big = aethel_smgr
OBJS = aethel_smgr.o
PGFILEDESC = "aethel_smgr - network storage manager for AethelDB"

EXTENSION = aethel_smgr
DATA = aethel_smgr--1.0.sql

# Build either against an installed server (USE_PGXS=1 + pg_config on PATH) or
# in-tree under contrib/.
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/aethel_smgr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
