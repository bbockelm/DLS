#!/usr/bin/make

PREFIX=$(shell pwd)
# Sources
LFCClient=$(shell pwd)

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

lfc: setup bare_lfc message
dli: setup bare_dli message
mysql: setup bare_mysql message
dbs: setup bare_dbs message
phedex: setup bare_phedex message

bare_lfc: common
bare_dli: common
bare_mysql: common
bare_dbs: common
bare_phedex: common

common:
	/bin/cp $(LFCClient)/README $(PREFIX)/.
	/bin/cp $(LFCClient)/dls*py $(LIB_DIR)/.
	/bin/cp $(LFCClient)/dls-* $(BIN_DIR)/.
	/bin/rm $(BIN_DIR)/dls-dli-get-se
	@if ! [ -e  $(SETUP_DIR)/setup.sh ]; then \
      echo "export PYTHONPATH=${LIB_DIR}:\$$PYTHONPATH"   > $(SETUP_DIR)/setup.sh; \
      echo "export PATH=${BIN_DIR}:\$$PATH"              >> $(SETUP_DIR)/setup.sh; \
      echo "setenv PYTHONPATH  ${LIB_DIR}:\$$PYTHONPATH"  > $(SETUP_DIR)/setup.csh; \
      echo "setenv PATH  ${BIN_DIR}:\$$PATH"             >> $(SETUP_DIR)/setup.csh; \
   fi

message:
	@echo 
	@echo "***********************************************************"
	@echo " NOTE: To set proper environment you need to do:"
	@echo "   source $(SETUP_DIR)/setup.(c)sh"
	@echo " You may want to add that to one of your start-up scripts."
	@echo "***********************************************************"
	@echo 

clean:
	/bin/rm -rf $(LIB_DIR)/*
	/bin/rm -rf $(BIN_DIR)/*
	/bin/rm -rf $(SETUP_DIR)/*
	/bin/rm -rf $(PREFIX)/README

