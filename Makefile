# hello_fdw/Makefile

MODULE_big = hello_fdw
OBJS = hello_fdw.o

EXTENSION = hello_fdw
DATA = hello_fdw--1.0.sql

REGRESS = hello_fdw

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
