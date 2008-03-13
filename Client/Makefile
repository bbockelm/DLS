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

lfc: WHAT=lfc
lfc: dli api_subdir end

dli: abs_prefix=$(shell cd ${PREFIX}; pwd)
dli: setup
	cd DliClient && ${MAKE} PREFIX=${abs_prefix} dli
	cd LFCClient && ${MAKE} PREFIX=${abs_prefix} dli
	@echo 
	@echo "NOTE: To set proper environment you need to source $(SETUP_DIR)/setup.(c)sh"
	@echo "You may want to add that to one of your start-up scripts"
	@echo 

mysql: WHAT=mysql
mysql: setup api_subdir end

dbs: WHAT=dbs
dbs: setup api_subdir end

phedex: WHAT=phedex
phedex: setup api_subdir end

api_subdir: abs_prefix=$(shell cd ${PREFIX}; pwd)
api_subdir:
	cd LFCClient  && ${MAKE} PREFIX=${abs_prefix} ${WHAT}

end:
	@echo 
	@echo "NOTE: To set proper environment you need to source $(SETUP_DIR)/setup.(c)sh"
	@echo "You may want to add that to one of your start-up scripts"
	@echo 

clean:
	/bin/rm -rf $(LIB_DIR)/*
	/bin/rm -rf $(BIN_DIR)/*
	/bin/rm -rf $(SETUP_DIR)/*

