HOSTNAME = $(shell hostname)

CC=g++ #-4.8

ifeq ($(HOSTNAME), yx.csail.mit.edu)
CC=g++
endif

CFLAGS=-Wall -g -std=c++0x # -rdynamic

ifeq ($(shell echo $(HOSTNAME) | head -c 2), ip)
CC=g++ # EC2
endif

MASSTREE_DIR =./storage/silo/masstree
SILO_DIR = ./storage/silo

MASSTREE ?= 1
MASSTREE_S=$(strip $(MASSTREE))
MASSTREE_CONFIG:=--enable-max-key-len=1024
MASSTREE_CONFIG+=--enable-assertions # --disable-assertions
MASSTREE_CONFIG+=--disable-invariants --disable-preconditions

ifeq ($(MASSTREE_S),1)
MASSTREE_SRCFILES = $(MASSTREE_DIR)/compiler.cc \
	$(MASSTREE_DIR)/str.cc \
	$(MASSTREE_DIR)/string.cc \
	$(MASSTREE_DIR)/straccum.cc \
	$(MASSTREE_DIR)/json.cc
endif

MASSTREE_OBJFILES := $(patsubst $(MASSTREE_DIR)/%.cc, %.o, $(MASSTREE_SRCFILES))

.SUFFIXES: .o .cpp .h .cc

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./include

CFLAGS += $(INCLUDE) -lrt -lpthread -msse4.2 -march=native -ffast-math -Wno-unused-variable -Werror -O3 -D_GNU_SOURCE -fopenmp # -D_FORTIFY_SOURCE=2 -fsanitize=address # -std=c++11 -fsanitize=address -fno-omit-frame-pointer

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
RELEASE_NAME := $(shell uname -r | cut -d '-' -f 3)
	ifeq ($(RELEASE_NAME),generic)
        LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt -ljemalloc -lnuma
	endif
	ifeq ($(RELEASE_NAME),aws)
        LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt -ljemalloc -lnuma # -lasan
	endif
	ifeq ($(RELEASE_NAME),Microsoft) # if Windows Subsystem
		LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt -lnuma # -ljemalloc
	endif
endif
ifeq ($(UNAME_S),Darwin)
        LDFLAGS = -Wall -L. -g -ggdb -std=c++0x -O3 -pthread  -lSystem.B -lnuma # -ljemalloc 
endif

MASSCFLAGS=$(CFLAGS)
ifeq ($(MASSTREE_S),1)
	MASSCFLAGS += -DNDB_MASSTREE -include $(MASSTREE_DIR)/config.h
	OBJDEP += $(MASSTREE_DIR)/config.h
endif

LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

CCS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cc))
CCOBJS = $(CCS:.cc=.o)
CCDEPS = $(CCS:.cc=.d)

SILO_OBJS = $(SILO_DIR)/out-perf.masstree/compiler.o \
	$(SILO_DIR)/out-perf.masstree/core.o \
	$(SILO_DIR)/out-perf.masstree/counter.o \
	$(SILO_DIR)/out-perf.masstree/json.o \
	$(SILO_DIR)/out-perf.masstree/straccum.o \
	$(SILO_DIR)/out-perf.masstree/string.o \
	$(SILO_DIR)/out-perf.masstree/ticker.o \
	$(SILO_DIR)/out-perf.masstree/rcu.o \
	$(SILO_DIR)/out-perf.masstree/allocator.o

all:rundb

rundb : $(OBJS) $(CCOBJS) $(SILO_OBJS)
	$(CC) $(ARCH) -o $@ $^ $(LDFLAGS)

#We don't need to clean up when we're making these targets
NODEPS:=clean
ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
    -include $(OBJS:%.o=%.d)
endif

$(MASSTREE_OBJFILES) : %.o: $(MASSTREE_DIR)/%.cc $(MASSTREE_DIR)/config.h
	$(CC) $(MASSCFLAGS) -include $(MASSTREE_DIR)/config.h -c $< -o $@

$(MASSTREE_DIR)/config.h: buildstamp.masstree $(MASSTREE_DIR)/configure $(MASSTREE_DIR)/config.h.in
	rm -f $@
	cd $(MASSTREE_DIR); ./configure $(MASSTREE_CONFIG)
	if test -f $@; then touch $@; fi

$(MASSTREE_DIR)/configure $(MASSTREE_DIR)/config.h.in: $(MASSTREE_DIR)/configure.ac
	cd $(MASSTREE_DIR) && autoreconf -i && touch configure config.h.in


%.d: %.cc
	$(CC) $(ARCH) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cc %.d
	$(CC) $(ARCH) -c $(CFLAGS) -o $@ $<

%.d: %.cpp
	$(CC) $(ARCH) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp %.d
	$(CC) $(ARCH) -c $(CFLAGS) -o $@ $<


./buildstamp ./buildstamp.bench ./buildstamp.masstree:
	@mkdir -p $(@D)
	@echo >$@

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS) $(CCOBJS) $(CCDEPS)


