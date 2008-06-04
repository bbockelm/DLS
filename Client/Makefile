#!/usr/bin/make

PREFIX=$(shell pwd)
#abs_prefix=$(shell cd ${PREFIX}; pwd)
# Sources
PYTHON_SRC=$(shell pwd)

# Targets
LIB_DIR=$(PREFIX)/lib
BIN_DIR=$(PREFIX)/bin
SETUP_DIR=$(PREFIX)/etc

install: default 
default: dbs
all: lfc

setup:
	/bin/mkdir -p $(LIB_DIR)
	/bin/mkdir -p $(BIN_DIR)
	/bin/mkdir -p $(SETUP_DIR)

lfc: WHAT=bare_lfc
lfc: dli api_subdir

dli: abs_prefix=$(shell cd ${PREFIX}; pwd)
dli: setup
	cd DliClient && ${MAKE} PREFIX=${abs_prefix} dli
	cd LFCClient && ${MAKE} PREFIX=${abs_prefix} bare_dli
	cd LFCClient  && ${MAKE} PREFIX=${abs_prefix} message
   
mysql: WHAT=mysql
mysql: api_subdir

dbs: WHAT=dbs
dbs: api_subdir

phedex: WHAT=phedex
phedex: api_subdir

api_subdir: abs_prefix=$(shell cd ${PREFIX}; pwd)
api_subdir:
	cd LFCClient  && ${MAKE} PREFIX=${abs_prefix} ${WHAT}

clean:
	/bin/rm -rf $(LIB_DIR)/*
	/bin/rm -rf $(BIN_DIR)/*
	/bin/rm -rf $(SETUP_DIR)/*
	/bin/rm -rf $(PREFIX)/README

