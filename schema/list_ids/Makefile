MODULES = list_ids
EXTENSION = list_ids
DATA = list_ids--1.0.sql
PGFILEDESC = "list_ids - Id generation functions for lists"
REGRESS = list_ids

PG_CPPFLAGS = -std=c99 -Wno-declaration-after-statement -Wno-missing-prototypes
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
