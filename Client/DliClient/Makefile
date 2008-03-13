#!/usr/bin/make

PREFIX=$(shell pwd)
PYXML_VER=0.8.4
# Sources
DliClient=$(shell pwd)

# Targets
LIB_DIR=$(PREFIX)/lib
BIN_DIR=$(PREFIX)/bin
SETUP_DIR=$(PREFIX)/etc


install: default
default: setup dli
all: setup dli

setup:
	/bin/mkdir -p $(LIB_DIR)
	/bin/mkdir -p $(BIN_DIR)
	/bin/mkdir -p $(SETUP_DIR)

dli:
	/bin/cp ${DliClient}/dli*py $(LIB_DIR)/.
	/bin/cp -r ${DliClient}/ZSI.tar.gz $(LIB_DIR)/.
	/bin/cp -r ${DliClient}/PyXML-$(PYXML_VER).tar.gz $(LIB_DIR)/.
	/bin/cp ${DliClient}/pyexpat* $(LIB_DIR)/.
	/bin/cp -r ${DliClient}/InstallPyXML.sh $(LIB_DIR)/.
	/bin/chmod a+x $(LIB_DIR)/InstallPyXML.sh
	cd $(LIB_DIR) && tar xvzf ZSI.tar.gz > /dev/null && /bin/rm $(LIB_DIR)/ZSI.tar.gz
	@echo "export PYTHONPATH=${LIB_DIR}:\$$PYTHONPATH"            > $(SETUP_DIR)/setup.sh
	@echo "export PATH=${BIN_DIR}:\$$PATH"                       >> $(SETUP_DIR)/setup.sh
	@echo "export LD_LIBRARY_PATH=${LIB_DIR}:\$$LD_LIBRARY_PATH" >> $(SETUP_DIR)/setup.sh
	@echo "setenv PYTHONPATH  ${LIB_DIR}:\$$PYTHONPATH"           > $(SETUP_DIR)/setup.csh
	@echo "setenv PATH  ${BIN_DIR}:\$$PATH"                      >> $(SETUP_DIR)/setup.csh
	@echo "setenv LD_LIBRARY_PATH ${LIB_DIR}:\$$LD_LIBRARY_PATH" >> $(SETUP_DIR)/setup.csh

clean:
	/bin/rm -rf $(LIB_DIR)/*
	/bin/rm -rf $(BIN_DIR)/*
	/bin/rm -rf $(SETUP_DIR)/*

