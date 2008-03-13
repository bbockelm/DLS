#!/usr/bin/make

PREFIX=$(shell pwd)
# Sources
LFCClient=$(shell pwd)

# Targets
LIB_DIR=$(PREFIX)/lib
BIN_DIR=$(PREFIX)/bin
SETUP_DIR=$(PREFIX)/etc

install: default
default: setup dbs
all: setup lfc

setup:
	/bin/mkdir -p $(LIB_DIR)
	/bin/mkdir -p $(BIN_DIR)
	/bin/mkdir -p $(SETUP_DIR)

lfc: common
dli: common
mysql: common
dbs: common
phedex: common

common:
	/bin/cp $(LFCClient)/dls*py $(LIB_DIR)/.
	/bin/cp $(LFCClient)/dls-* $(BIN_DIR)/.
	/bin/rm $(BIN_DIR)/dls-dli-get-se
	@if ! [ -e  $(SETUP_DIR)/setup.sh ]; then \
      echo "export PYTHONPATH=${LIB_DIR}:\$$PYTHONPATH"   > $(SETUP_DIR)/setup.sh; \
      echo "export PATH=${BIN_DIR}:\$$PATH"              >> $(SETUP_DIR)/setup.sh; \
      echo "setenv PYTHONPATH  ${LIB_DIR}:\$$PYTHONPATH"  > $(SETUP_DIR)/setup.csh; \
      echo "setenv PATH  ${BIN_DIR}:\$$PATH"             >> $(SETUP_DIR)/setup.csh; \
   fi

clean:
	/bin/rm -rf $(LIB_DIR)/*
	/bin/rm -rf $(BIN_DIR)/*
	/bin/rm -rf $(SETUP_DIR)/*

