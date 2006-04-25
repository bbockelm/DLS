#!/usr/bin/make

PREFIX=$(shell pwd)

# Sources
PYTHON_SRC=$(shell pwd)

# Targets
LIB_DIR=$(PREFIX)/lib

all: setup lfc-mysql

install : setup lfc-mysql

setup:
	/bin/mkdir -p $(LIB_DIR)

lfc-mysql:
	/bin/cp $(PYTHON_SRC)/LFCClient/dls* $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/dli* $(LIB_DIR)/.

lfc:
	/bin/cp $(PYTHON_SRC)/LFCClient/dls* $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/dli* $(LIB_DIR)/.
	/bin/rm $(LIB_DIR)/dlsMySQLApi.py

mysql:
	/bin/cp $(PYTHON_SRC)/LFCClient/dls* $(LIB_DIR)/.
	/bin/rm $(LIB_DIR)/dlsLfcApi.py
	/bin/rm $(LIB_DIR)/dlsDliClient.py

clean:
	/bin/rm -rf $(LIB_DIR)/*

