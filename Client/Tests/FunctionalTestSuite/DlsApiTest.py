#!/usr/bin/env python
 
#
# $Id$
#
# DLS Client Functional Test Suite. $Name$.
# Antonio Delgado Peris. CIEMAT. CMS.
#

import unittest

import anto_utils

from commands import getstatusoutput
run = getstatusoutput
from os import putenv, unsetenv, chdir, getcwd, environ
from time import strftime
import sys
from stat import *

# Need a global variable here
conf = {}
name = "DlsApiTest.py"


##############################################################################
# Parent Class for DLS API testing
##############################################################################

# TODO: Include things within sessions as much as possible...

class TestDlsApi(unittest.TestCase):
 
  def setUp(self):
      # Check that all the env vars are there
      self.conf = conf
      msg = "Required variable has not been defined!"
  
      msg1 = msg + " (DLS_TEST_DIR)"
      self.testdir = self.conf.get("DLS_TEST_DIR")
      self.assert_(self.testdir, msg1)
  
      msg1 = msg + " (DLS_TEST_SERVER)"
      self.host = self.conf.get("DLS_TEST_SERVER")
      self.assert_(self.host, msg1)
  
      msg1 = msg + " (DLS_CODE_PATH)"
      self.path = self.conf.get("DLS_CODE_PATH")
      self.assert_(self.path, msg1)
  
      msg1 = msg + " (DLS_TYPE)"
      self.type = self.conf.get("DLS_TYPE")
      self.assert_(self.type, msg1)

      # If there is a verbosity, use it
      verb = self.conf.get("DLS_VERBOSITY")

      # Create the interface
      self.session = False
      try:
        self.api = dlsClient.getDlsApi(self.type, self.host+self.testdir)
        if(verb): self.api.setVerbosity(int(verb))
      except DlsApiError, inst:
        msg = "Error in interface creation: %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)


  def tearDown(self):
     # Common clean up (just in case)
     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     fB3 = DlsFileBlock("f3")
     entry = DlsEntry(fB, [])
     entry2 = DlsEntry(fB2, [])
     entry3 = DlsEntry(fB3, [])
     try:
       self.api.delete([entry, entry2, entry3], all = True)
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
     
     # End session
     if(self.session):
         self.api.endSession()


###########################################
# Classes for basic general DLS API testing
###########################################

# Parent class general DLS API tests
# ==================================

class TestDlsApi_General(TestDlsApi):
  def setUp(self):
     # Invoke parent
     TestDlsApi.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi.tearDown(self)


# Class for multiple arguments testing
# ====================================

class TestDlsApi_General_Basic(TestDlsApi_General):
  def setUp(self):
     # Invoke parent
     TestDlsApi_General.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_General.tearDown(self)


  # Test endpoint and interface binding
  def testEndpointAndInterface(self):

     fB = DlsFileBlock("f1")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     entry = DlsEntry(fB, [loc1, loc2])
      
     # First, wrong interface selection
     try:
       api2 = dlsClient.getDlsApi("SOMETHING")
       msg = "Unexpected success binding interface of wrong type SOMETHING"
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       expected = "['DLS_TYPE_LFC', 'DLS_TYPE_DLI', 'DLS_TYPE_MYSQL']"
       msg = "Results (%s) are not those expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)

     # Next the DLS endpoint
     try:
       api2 = dlsClient.getDlsApi(self.type)
     except DlsApiError, inst:
       expected = "Could not set the DLS server to use"
       msg = "Results (%s) are not those expected (%s)" % (inst, expected)
       self.assertEqual(str(inst), expected, msg)


  # Test basic addition, getLocations and listFileBlocks
  def testAddListGetSE(self):

     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     loc3 = DlsLocation("DlsApiTest_se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     try:
       self.api.add(entry)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
       
     entry2 = DlsEntry(fB, [loc2, loc3])
     try:
       self.api.add(entry2)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     entry3 = DlsEntry(fB2, [])
     try:
       self.api.add(entry3)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry3, inst)
       self.assertEqual(0, 1, msg)
     
     # Now get locations
     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest_se1") != None)
     correct *= (res[0].getLocation("DlsApiTest_se2") != None)
     correct *= (res[0].getLocation("DlsApiTest_se3") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Now list FileBlocks passing name
     try:
       res = self.api.listFileBlocks("/")
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % ("/", inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].name, res[1].name]
     correct = ("f1" in fb_got)
     correct *= ("f2" in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Now list FileBlocks passing object 
     try:
       res = self.api.listFileBlocks(fB2)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB2, inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].name]
     correct = ("f2" in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
    
     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2], all = True)
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s]): %s" % (entry, entry2, inst)
       self.assertEqual(0, 1, msg)


  # Test basic deletion
  def testDeletion(self):

     fB = DlsFileBlock("f1")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     loc3 = DlsLocation("DlsApiTest_se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2, loc3])
     try:
       self.api.add(entry)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     # Delete some replicas
     entry2 = DlsEntry(fB, [loc1, loc2])
     try:
       self.api.delete(entry2)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest_se3") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Delete the entry ("all" flag)
     try:
       self.api.delete(entry2, all = True)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     # Check the entry is gone
     try:
       res = self.api.listFileBlocks(entry.fileBlock)
       msg = "Unexpected success in listFileBlocks(%s)" % (entry.fileBlock)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       pass


  # Test addition with non-existing parent directories
  def testAdditionWithParent(self):
     fB = DlsFileBlock("dir1/dir2/f1")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     try:
       self.api.add(entry)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     # Now get locations
     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest_se1") != None)
     correct *= (res[0].getLocation("DlsApiTest_se2") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Clean: Delete the entry (and -LFC- the dirs)
     try:
       self.api.delete(entry, all = True)
       if(self.type == "DLS_TYPE_LFC"):
          # TODO: If empty dirs were automatically removed, this would go away...
          entry2 = DlsEntry(DlsFileBlock("dir1/dir2"), [])
          self.api.delete(entry2, all = True)
          entry3 = DlsEntry(DlsFileBlock("dir1"), [])
          self.api.delete(entry3, all = True)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


# Class for multiple arguments testing
# ====================================

class TestDlsApi_General_MultipleArgs(TestDlsApi_General):
  def setUp(self):
     # Invoke parent
     TestDlsApi_General.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_General.tearDown(self)

  # Test basic addition, getLocations and listFileBlocks with multiple args
  def testAddListGetSE(self):
  
     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     fB3 = DlsFileBlock("f3")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     loc3 = DlsLocation("DlsApiTest_se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2, loc3])
     entry3 = DlsEntry(fB3, [loc3])
     try:
       self.api.add([entry, entry2, entry3])
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
       
     entry2 = DlsEntry(fB, [loc2, loc3])
     try:
       self.api.add(entry)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     # Now get locations
     try:
       res = self.api.getLocations([fB, fB2])
     except DlsApiError, inst:
       msg = "Error in getLocations([%s, %s]): %s" % (fB, fB2, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest_se1") != None)
     correct *= (res[0].getLocation("DlsApiTest_se2") != None)
     correct *= (res[1].getLocation("DlsApiTest_se2") != None)
     correct *= (res[1].getLocation("DlsApiTest_se3") != None)
     msg = "Locations were not correctly retrieved (entry1: %s, entry2: %s)" % (res[0], res[1])
     self.assert_(correct, msg)

     # Now list FileBlocks passing name
     try:
       res = self.api.listFileBlocks(["f1", "f2"])
     except DlsApiError, inst:
       msg = "Error in listFileBlocks([%s, %s]): %s" % ("f1", "f2", inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].name, res[1].name]
     correct = ("f1" in fb_got)
     correct *= ("f2" in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Now list FileBlocks passing object 
     try:
       res = self.api.listFileBlocks([fB2, fB3])
     except DlsApiError, inst:
       msg = "Error in listFileBlocks([%s, %s]): %s" % (fB2, fB3, inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].name, res[1].name]
     correct = ("f2" in fb_got)
     correct *= ("f3" in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
    
     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2, entry3], all = True)
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)


  # Test deletion using multiple arguments
  def testDeletion(self):
  
     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     fB3 = DlsFileBlock("f3")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     loc3 = DlsLocation("DlsApiTest_se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2, loc3])
     entry3 = DlsEntry(fB3, [loc3])
     try:
       self.api.add([entry, entry2, entry3])
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
       
     # Delete some replicas (but not all)
     entry = DlsEntry(fB, [loc1])
     entry2 = DlsEntry(fB2, [loc3])
     try:
       self.api.delete([entry, entry2])
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s]): %s" % (entry, entry2, inst)
       self.assertEqual(0, 1, msg)

     # Check with getLocations
     try:
       res = self.api.getLocations([fB, fB2])
     except DlsApiError, inst:
       msg = "Error in getLocations([%s, %s]): %s" % (fB, fB2, inst)
       self.assertEqual(0, 1, msg)
     correct = len(res[0].locations) == 1
     correct *= len(res[1].locations) == 1
     correct *= (res[0].getLocation("DlsApiTest_se2") != None)
     correct *= (res[1].getLocation("DlsApiTest_se2") != None)
     msg = "FileBlocks were not removed correctly (res[0]: %s, res[1]: %s)" % (res[0], res[1])
     self.assert_(correct, msg)

     # Now delete all replicas and FileBlock
     try:
       self.api.delete(entry, all = True)
     except DlsApiError, inst:
       msg = "Error in delete(%s, all = True): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
     entry2 = DlsEntry(fB2, [loc2, loc3])
     try:
       self.api.delete([entry2])
     except DlsApiError, inst:
       msg = "Error in delete([%s]): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     # See they are gone
     try:
       res = self.api.listFileBlocks("/")
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % ("/", inst)
       self.assertEqual(0, 1, msg)
     correct = len(res) == 1
     correct *= res[0].name == "f3"
     msg = "FileBlocks were not removed correctly (res: %s)" % (res)
     self.assert_(correct, msg)

     # Delete the last entry
     try:
       self.api.delete([entry3], all = True)
     except DlsApiError, inst:
       msg = "Error in delete(%s, all = True): %s" % (entry3, inst)
       self.assertEqual(0, 1, msg)



# Class for (slower) getFileBlocks testing 
# ========================================

class TestDlsApi_General_GetFileBlocks(TestDlsApi_General):
  def setUp(self):
     # Invoke parent
     TestDlsApi_General.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_General.tearDown(self)

  # Test get-fileblock simple arg
  def testGetFileBlocks(self):

     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     fB3 = DlsFileBlock("f3")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     loc3 = DlsLocation("DlsApiTest_se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2, loc3])
     entry3 = DlsEntry(fB3, [loc3])
     try:
       self.api.add([entry, entry2, entry3])
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
       
    # Now retrieve them
     try:
       res = self.api.getFileBlocks("DlsApiTest_se2")
     except DlsApiError, inst:
       msg = "Error in getFileBlocks(%s): %s" % (loc2, inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].fileBlock.name, res[1].fileBlock.name]
     correct = ("f1" in fb_got)
     correct *= ("f2" in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry1: %s, entry2: %s)" % (res[0], res[1])
     self.assert_(correct, msg)

     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2, entry3], all = True)
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)


  # Test getFileblocks with multiple arguments
  def testGetFileBlocksMulti(self):
  
     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     fB3 = DlsFileBlock("f3")
     loc1 = DlsLocation("DlsApiTest_se1")
     loc2 = DlsLocation("DlsApiTest_se2")
     loc3 = DlsLocation("DlsApiTest_se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2, loc3])
     entry3 = DlsEntry(fB3, [loc3])
     try:
       self.api.add([entry, entry2, entry3])
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
       
    # Now retrieve them
     try:
       res = self.api.getFileBlocks([loc1, loc2, loc3])
     except DlsApiError, inst:
       msg = "Error in getFileBlocks(%s): %s" % (loc2, inst)
       self.assertEqual(0, 1, msg)
     fb_got = []
     for i in res:
        fb_got.append(i.fileBlock.name)
     correct = (len(fb_got) == 5)
     correct *= ("f1" in fb_got)
     correct *= ("f2" in fb_got)
     correct *= ("f3" in fb_got)
     msg = "FileBlocks were not correctly retrieved (got: %s)" % fb_got
     self.assert_(correct, msg)

     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2, entry3], all = True)
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)




##############################################################################
# Module's methods to return the suites
##############################################################################

def getSuite():
  suite = []
  suite.append(getSuite_General_Basic())
  suite.append(getSuite_General_Multi())
  suite.append(getSuite_General_GetFileBlocks())
  return unittest.TestSuite(suite)
  
def getSuite_General_Basic():
  return unittest.makeSuite(TestDlsApi_General_Basic)
   
def getSuite_General_Multi():
  return unittest.makeSuite(TestDlsApi_General_MultipleArgs)
   
def getSuite_General_GetFileBlocks():
  return unittest.makeSuite(TestDlsApi_General_GetFileBlocks)
   

##############################################################################
# Other module methods
##############################################################################

# Module's methods to set the conf file
def setConf(pConf):
  global conf
  conf = pConf

# Read the conf file, etc.
def loadVars(conf_file):
  # "Source" the configuration file
  conf = anto_utils.SimpleConfigParser()
  try:
     conf.read(conf_file)
  except Exception:
     msg = "There were errors parsing the configuration file ("+conf_file+"). Please, review it"
     sys.stderr.write(msg+"\n")
  
  # Add the code path so that python can find it
  path = conf.get("DLS_CODE_PATH")
  if(not path):
     msg = "The code path was not correctly specified in the config file!"
     sys.stderr.write(msg+"\n")
     return None
  sys.path.append(path)

  return conf


def help():
  print "This script runs tests on the DLS API."
  print "It requires a configuration file with some variables (refer to the example)"
  print
  print "An optional additional argument may be used to execute a desired subset"
  print "of the tests. Default is to execute all of them."
  print "   \"basic\" ==> Tests on general basic functionality"
  print "   \"multi\" ==> Basic tests but using multiple arguments (lists)"
  print "   \"getfb\" ==> Tests on getFileBlocks functions (maybe slower)" 
  
def usage():
  print "Usage:  %s <conf_file> [<subset_of_tests>]" % (name+".py")


##############################################################################
# Main method
##############################################################################

# Make this runnable
if __name__ == '__main__':
  if(len(sys.argv) < 2):
     msg = "Not enought input arguments!\n\n"
     msg += "You need to specify the configuration file as argument.\n"
     msg += "Probably you can use %s.conf, but please check it out first.\n" % (name)
     sys.stderr.write(msg+"\n")
     help()
     print
     usage()
     sys.exit(-1)

  the_conf = loadVars(sys.argv[1])
  if(not the_conf):
     sys.stderr.write("Incorrecect conf file, leaving...\n")
  from dlsDataObjects import *
  from dlsApi import *
  import dlsClient
  setConf(the_conf)

  suite = None
  if(len(sys.argv) > 2):
     if(sys.argv[2] == "basic"):
        suite = getSuite_General_Basic()
     if(sys.argv[2] == "multi"):
        suite = getSuite_General_Multi()
     if(sys.argv[2] == "getfb"):
        suite = getSuite_General_GetFileBlocks()

     if(not suite):
           msg = "Error: The optional second argument is not one of the supported ones!\n"
           sys.stderr.write("\n"+msg)
           help()
           print
           usage()
           sys.exit(-1)
  else:
     suite = getSuite()

  unittest.TextTestRunner(verbosity=4).run(suite)