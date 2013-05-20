
MODULE_big = libmongres
OBJS = mongres.o

override CPPFLAGS += -Imongo-c-driver/src
SHLIB_LINK += mongo-c-driver/libmongoc.a

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
override CC += -std=c99

