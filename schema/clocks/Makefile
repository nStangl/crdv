MODULES = clocks
EXTENSION = clocks
DATA = clocks--1.0.sql
PGFILEDESC = "clocks - Operations over vector clocks and hybrid logical clocks"
REGRESS = clocks

PG_CPPFLAGS = -std=c99 -Wno-declaration-after-statement -Wno-missing-prototypes
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
