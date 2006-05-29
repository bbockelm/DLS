#!/usr/bin/make

PREFIX=$(shell pwd)
PYXML_VER=0.8.4
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
	/bin/cp -r $(PYTHON_SRC)/DliClient/ZSI.tar.gz $(LIB_DIR)/.
	/bin/cp -r $(PYTHON_SRC)/DliClient/PyXML-$(PYXML_VER).tar.gz $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/pyexpat* $(LIB_DIR)/.
	/bin/cp -r $(PYTHON_SRC)/DliClient/InstallPyXML.sh $(LIB_DIR)/.
	/bin/chmod a+x $(LIB_DIR)/InstallPyXML.sh
	cd $(LIB_DIR) && tar xvzf ZSI.tar.gz > /dev/null && /bin/rm $(LIB_DIR)/ZSI.tar.gz

lfc:
	/bin/cp $(PYTHON_SRC)/LFCClient/dls* $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/dli* $(LIB_DIR)/.
	/bin/rm $(LIB_DIR)/dlsMySQLApi.py
	/bin/cp -r $(PYTHON_SRC)/DliClient/ZSI.tar.gz $(LIB_DIR)/.
	/bin/cp -r $(PYTHON_SRC)/DliClient/PyXML-$(PYXML_VER).tar.gz $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/pyexpat* $(LIB_DIR)/.
	/bin/cp -r $(PYTHON_SRC)/DliClient/InstallPyXML.sh $(LIB_DIR)/.
	/bin/chmod a+x $(LIB_DIR)/InstallPyXML.sh
	cd $(LIB_DIR) && tar xvzf ZSI.tar.gz > /dev/null && /bin/rm $(LIB_DIR)/ZSI.tar.gz

dli:
	/bin/cp $(PYTHON_SRC)/LFCClient/dls* $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/dli* $(LIB_DIR)/.
	/bin/rm $(LIB_DIR)/dls-add
	/bin/rm $(LIB_DIR)/dls-delete
	/bin/rm $(LIB_DIR)/dls-update
	/bin/rm $(LIB_DIR)/dlsMySQLApi.py
	/bin/rm $(LIB_DIR)/dlsLfcApi.py
	/bin/cp -r $(PYTHON_SRC)/DliClient/ZSI.tar.gz $(LIB_DIR)/.
	/bin/cp -r $(PYTHON_SRC)/DliClient/PyXML-$(PYXML_VER).tar.gz $(LIB_DIR)/.
	/bin/cp $(PYTHON_SRC)/DliClient/pyexpat* $(LIB_DIR)/.	
	/bin/cp -r $(PYTHON_SRC)/DliClient/InstallPyXML.sh $(LIB_DIR)/.
	/bin/chmod a+x $(LIB_DIR)/InstallPyXML.sh
	cd $(LIB_DIR) && tar xvzf ZSI.tar.gz > /dev/null && /bin/rm $(LIB_DIR)/ZSI.tar.gz

mysql:
	/bin/cp $(PYTHON_SRC)/LFCClient/dls* $(LIB_DIR)/.
	/bin/rm $(LIB_DIR)/dlsLfcApi.py
	/bin/rm $(LIB_DIR)/dlsDliClient.py


clean:
	/bin/rm -rf $(LIB_DIR)/*

