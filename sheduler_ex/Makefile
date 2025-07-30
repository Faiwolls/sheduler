# contrib/sheduler_ex/Makefile

MODULE_big = sheduler_ex
OBJS = sheduler_ex.o $(WIN32RES)
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = sheduler_ex
DATA = sheduler_ex--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/sheduler_ex
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
