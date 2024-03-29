#!/usr/bin/env python
 
#
# $Id: DlsDbsApiTest.py,v 1.1 2007/03/23 10:29:01 delgadop Exp $
#
# DLS Client Functional Test Suite. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

import unittest

import anto_utils

from commands import getstatusoutput
run = getstatusoutput
from os import putenv, unsetenv, chdir, getcwd, environ
import sys
from stat import *

# Need a global variable here
conf = {}
name = "DlsDbsApiTest.py"


##############################################################################
# Parent Class for DLS API testing
##############################################################################

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

      msg1 = msg + " (DBS_f1)"
      self.f1 = self.conf.get("DBS_f1")
      self.assert_(self.f1, msg1)

      msg1 = msg + " (DBS_f2)"
      self.f2 = self.conf.get("DBS_f2")
      self.assert_(self.f2, msg1)

      msg1 = msg + " (DBS_f3)"
      self.f3 = self.conf.get("DBS_f3")
      self.assert_(self.f3, msg1)

      # If there is a verbosity, use it
      verb = self.conf.get("DLS_VERBOSITY")

      # Create the interface
      self.clean = True
      self.session = False
      try:
        self.api = dlsClient.getDlsApi(self.type, self.host+self.testdir)
        if(verb): self.api.setVerbosity(int(verb))
      except DlsApiError, inst:
        msg = "Error in interface creation: %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)


  def tearDown(self):
     # Common clean up (just in case)
     if(self.clean):
        fBList = map(DlsFileBlock, [self.f1, self.f2, self.f3])
        entryList = map(DlsEntry, fBList, [])
        try:
          self.api.delete(entryList, all = True, force = True)
        except DlsApiError, inst:
          msg = "Error in delete(%s): %s" % (map(str, entryList), inst)
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


# Class for single argument testing
# =================================

class TestDlsApi_General_Basic(TestDlsApi_General):
  def setUp(self):
     # Invoke parent
     TestDlsApi_General.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_General.tearDown(self)


  # Test endpoint and interface binding
  def testEndpointAndInterface(self):

     unsetenv("DLS_ENDPOINT")
     unsetenv("DBS_CLIENT_CONFIG")

     fB = DlsFileBlock("self.f1")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     entry = DlsEntry(fB, [loc1, loc2])
      
     # First, wrong interface selection
     try:
       api2 = dlsClient.getDlsApi("SOMETHING")
       msg = "Unexpected success binding interface of wrong type SOMETHING"
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       expected = "['DLS_TYPE_LFC', 'DLS_TYPE_DLI', 'DLS_TYPE_MYSQL', 'DLS_TYPE_DBS', 'DLS_TYPE_PHEDEX']"
       msg = "Results (%s) don't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)

     # Next the DLS endpoint
     try:
       api2 = dlsClient.getDlsApi(self.type)
     except DlsApiError, inst:
       expected = "Could not set the interface to the DLS server"
       msg = "Results (%s) don't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)

  # Test wrong hostname detection
  def testCheckHostname(self):
     try:
        se = "asdaf"
        loc = DlsLocation(se, checkHost = True)
        msg = "Unexpected success in DlsLocation(%s, checkHost = True) creation" % (se)
        self.assertEqual(0, 1, msg)
     except DlsObjValueError, inst:
        pass
        
     try:
        se = "asf&asdf.es"
        loc = DlsLocation(se, checkHost = True)
        msg = "Unexpected success in DlsLocation(%s, checkHost = True) creation" % (se)
        self.assertEqual(0, 1, msg)
     except DlsObjValueError, inst:
        pass 
        
     try:
        se = "dpmsrmX.ciemat.es"
        loc = DlsLocation(se, checkHost = True)
        msg = "Unexpected success in DlsLocation(%s, checkHost = True) creation" % (se)
        self.assertEqual(0, 1, msg)
     except DlsObjValueError, inst:
        pass
        
     try:
        se = "www.google.es"
        loc = DlsLocation(se, checkHost = True)
     except DlsObjValueError, inst:
        msg = "Unexpected error in DlsLocation(%s, checkHost = True) creation" % (se)
        self.assertEqual(0, 1, msg)

     try:
        se = "sdaf-"
        loc = DlsLocation(se)
     except DlsObjValueError, inst:
        msg = "Unexpected error in DlsLocation(%s) creation" % (se)
        self.assertEqual(0, 1, msg)





  # Test basic addition, getLocations and listFileBlocks
  def testAddListGetSE(self):

     fB = DlsFileBlock(self.f1)
     fB2 = DlsFileBlock(self.f2)
     fBXX = DlsFileBlock("fXX")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("DlsApiTest-se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
       
     entry2 = DlsEntry(fB, [loc2, loc3])
     try:
       self.api.add(entry2, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     entry3 = DlsEntry(fB2, [])
     try:
       self.api.add(entry3, checkLocations = False, allowEmptyBlocks = True)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry3, inst)
       self.assertEqual(0, 1, msg)
     
     # Now get locations
     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (self.f1, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest-se1") != None)
     correct *= (res[0].getLocation("DlsApiTest-se2") != None)
     correct *= (res[0].getLocation("DlsApiTest-se3") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Now get wrong location (w/ and w/out errorTolerant)
     try:
       res = self.api.getLocations(fBXX)
       msg = "Unexpected success in getLocations(%s): %s" % ("fBXX", inst)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       pass
     try:
       res = self.api.getLocations(fBXX, errorTolerant = True)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % ("fBXX, errorTolerant=True", inst)
       self.assertEqual(0, 1, msg)
     correct = (res == [])
     msg = "Empty list should have been retrieved (entry list: %s)" % (res)
     self.assert_(correct, msg)

     # Now list FileBlocks passing name
     try:
       res = self.api.listFileBlocks("/")
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % ("/", inst)
       self.assertEqual(0, 1, msg)
     fb_got = []
     for i in res:
        fb_got.append(i.name)
     correct = (self.f1 in fb_got)
     correct *= (self.f2 in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (fb_got)
     self.assert_(correct, msg)

     # Now list FileBlocks passing object 
     try:
       res = self.api.listFileBlocks(fB2)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB2, inst)
       self.assertEqual(0, 1, msg)
     fb_got.append(res[0].name)
     correct = (self.f2 in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
    
     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2], all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s]): %s" % (entry, entry2, inst)
       self.assertEqual(0, 1, msg)


  # Test basic deletion
  def testDeletion(self):

     fB = DlsFileBlock(self.f1)
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("DlsApiTest-se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2, loc3])
     try:
       self.api.add(entry, checkLocations = False)
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
     correct = (res[0].getLocation("DlsApiTest-se3") != None)
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
       res = self.api.getLocations(entry.fileBlock)
       msg = "Not all locations were correctly removed (%s)" % (res[0].simpleStr())
       self.assertEqual(0, len(res[0].locations), msg)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (entry.fileblock.name, inst)
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
  
     fB = DlsFileBlock(self.f1)
     fB2 = DlsFileBlock(self.f2)
     fB3 = DlsFileBlock(self.f3)
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("DlsApiTest-se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2, loc3])
     entry3 = DlsEntry(fB3, [loc3])
     try:
       self.api.add([entry, entry2, entry3], checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
       
     entry4 = DlsEntry(fB, [loc2, loc3])
     try:
       self.api.add(entry4, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry4, inst)
       self.assertEqual(0, 1, msg)

     # Now get locations
     try:
       res = self.api.getLocations([fB, fB2])
     except DlsApiError, inst:
       msg = "Error in getLocations([%s, %s]): %s" % (fB, fB2, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest-se1") != None)
     correct *= (res[0].getLocation("DlsApiTest-se2") != None)
     correct *= (res[1].getLocation("DlsApiTest-se2") != None)
     correct *= (res[1].getLocation("DlsApiTest-se3") != None)
     msg = "Locations were not correctly retrieved (entry1: %s, entry2: %s)" % (res[0], res[1])
     self.assert_(correct, msg)

     # Now list FileBlocks passing name
     try:
       res = self.api.listFileBlocks([self.f1, self.f2])
     except DlsApiError, inst:
       msg = "Error in listFileBlocks([%s, %s]): %s" % (self.f1, self.f2, inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].name, res[1].name]
     correct = (self.f1 in fb_got)
     correct *= (self.f2 in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Now list FileBlocks passing object 
     try:
       res = self.api.listFileBlocks([fB2, fB3])
     except DlsApiError, inst:
       msg = "Error in listFileBlocks([%s, %s]): %s" % (fB2, fB3, inst)
       self.assertEqual(0, 1, msg)
     fb_got = [res[0].name, res[1].name]
     correct = (self.f2 in fb_got)
     correct *= (self.f3 in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
    
     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2, entry3], all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)


  # Test deletion using multiple arguments
  def testDeletion(self):
  
     fB = DlsFileBlock(self.f1)
     fB2 = DlsFileBlock(self.f2)
     fB3 = DlsFileBlock(self.f3)
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("DlsApiTest-se3")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2, loc3])
     entry3 = DlsEntry(fB3, [loc3])
     try:
       self.api.add([entry, entry2, entry3], checkLocations = False)
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
     correct *= (res[0].getLocation("DlsApiTest-se2") != None)
     correct *= (res[1].getLocation("DlsApiTest-se2") != None)
     msg = "Locations were not removed correctly "
     msg += "(res[0]: %s, res[1]: %s)"%(res[0].simpleStr(), res[1].simpleStr())
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
     res = self.api.getLocations(entry2.fileBlock)
     msg = "Not all locations were correctly removed (%s)" % (res)
     self.assertEqual(0, len(res[0].locations), msg)
     res = self.api.getLocations(entry.fileBlock)
     msg = "Not all locations were correctly removed (%s)" % (res)
     self.assertEqual(0, len(res[0].locations), msg)

     # Delete the last entry
     try:
       self.api.delete([entry3], all = True)
       self.clean = False
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
     fBList = map(DlsFileBlock, [self.f1, self.f2, self.f3])
     locList = map(lambda x: [DlsLocation(x)], ["DlsApiTest-se1","DlsApiTest-se2","DlsApiTest-se3"])
     entryList = map(DlsEntry, fBList, locList)
     entryList[0].locations.append(locList[1][0])
     entryList[1].locations.append(locList[2][0])

     # Session
     self.session = True
     self.api.startSession()

     # First add
     try:
       self.api.add(entryList, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)
       
    # Now retrieve them
     try:
       res = self.api.getFileBlocks("DlsApiTest-se2")
     except DlsApiError, inst:
       msg = "Error in getFileBlocks(%s): %s" % (locList[1][0], inst)
       self.assertEqual(0, 1, msg)
     fb_got = []
     for entry in res:
       fb_got.append(entry.fileBlock.name)
     correct = (self.f1 in fb_got)
     correct *= (self.f2 in fb_got)
     msg = "FileBlocks were not correctly retrieved (entry[1]: %s, entry[2]: %s)"%(res[0].simpleStr(), res[1].simpleStr())
     self.assert_(correct, msg)

     # Clean: Delete the entries
     try:
       self.api.delete(entryList, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)


  # Test getFileblocks with multiple arguments
  def testGetFileBlocksMulti(self):
  
     fBList = map(DlsFileBlock, [self.f1, self.f2, self.f3])
     locList = map(lambda x: [DlsLocation(x)], ["DlsApiTest-se1","DlsApiTest-se2","DlsApiTest-se3"])
     entryList = map(DlsEntry, fBList, locList)
     entryList[0].locations.append(locList[1][0])
     entryList[1].locations.append(locList[2][0])

     # Session
     self.session = True
     self.api.startSession()

     # First add
     try:
       self.api.add(entryList, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)
       
    # Now retrieve them
     try:
       res = self.api.getFileBlocks([locList[0][0], locList[1][0], locList[2][0]])
     except DlsApiError, inst:
       msg = "Error in getFileBlocks(%s): %s" % (loc2, inst)
       self.assertEqual(0, 1, msg)
     fb_got = []
     for i in res:
        fb_got.append(i.fileBlock.name)
     correct = (self.f1 in fb_got)
     correct *= (self.f2 in fb_got)
     correct *= (self.f3 in fb_got)
     msg = "FileBlocks were not correctly retrieved (got: %s)" % fb_got
     self.assert_(correct, msg)

     # Clean: Delete the entries
     try:
       self.api.delete(entryList, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)



# Class for dump + getAllLocations testing 
# =================================================

class TestDlsApi_General_Dump(TestDlsApi_General):
  def setUp(self):
     # Invoke parent
     TestDlsApi_General.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_General.tearDown(self)

  # Test dump entries
  def testDumpEntries(self):

     fBList = map(DlsFileBlock, [self.f1, self.f2, self.f3])
     locList = map(lambda x: [DlsLocation(x)], ["DlsApiTest-se1","DlsApiTest-se2","DlsApiTest-se3"])
     entryList = map(DlsEntry, fBList, locList)
     entryList[0].locations.append(locList[1][0])
     entryList[1].locations.append(locList[2][0])

     # Session
     self.session = True
     self.api.startSession()

     # First add
     try:
       self.api.add(entryList, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)
       
    # Now dump them and check
     try:
       res = self.api.dumpEntries("/", recursive=True)
     except DlsApiError, inst:
       msg = "Error in dumpEntries(\"/\"): %s" % (inst)
       self.assertEqual(0, 1, msg)
     correct = (len(res)>=3)
     msg = "Incorrectal dump (number of entries smaller than three: %s)"%(res)
     self.assert_(correct, msg)
     correct = True
     dump_str= ""
     fbs = 0
     for i in res:
        dump_str += i.simpleStr() + '\n'
        locs_str = []
        for j in i.locations:
           locs_str.append(j.host)
        if (i.fileBlock.name == self.f1):
            fbs += 1
            correct *= "DlsApiTest-se1" in locs_str
            correct *= "DlsApiTest-se2" in locs_str
        if (i.fileBlock.name == self.f2):
            fbs += 1
            correct *= "DlsApiTest-se2" in locs_str
            correct *= "DlsApiTest-se3" in locs_str
        if (i.fileBlock.name == self.f3):
            fbs += 1
            correct *= "DlsApiTest-se3" in locs_str
     
     if(fbs<3): correct = False
     msg = "Incorrectal dump (fbs: %d, \n entries: %s)" % (fbs, dump_str)
     self.assert_(correct, msg)

     # Clean: Delete the entry (and -LFC- the dirs)
     try:
       self.api.delete(entryList, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)


  # Test get all locations
  def testGetAllLocations(self):
  
     fBList = map(DlsFileBlock, [self.f1, self.f2, self.f3])
     locList = map(lambda x: [DlsLocation(x)], ["DlsApiTest-se1","DlsApiTest-se2","DlsApiTest-se3"])
     entryList = map(DlsEntry, fBList, locList)
     entryList[0].locations.append(locList[1][0])
     entryList[1].locations.append(locList[2][0])

     # Session
     self.session = True
     self.api.startSession()

     # First add
     try:
       self.api.add(entryList, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)
       
    # Now get all and check
     try:
       res = self.api.getAllLocations()
     except DlsApiError, inst:
       msg = "Error in getAllLocations(): %s" % (inst)
       self.assertEqual(0, 1, msg)
     correct = (len(res)>=3)
     msg = "Incorrectal retrieval of all locations (number less than three: %s)"%(res)
     self.assert_(correct, msg)
     test_locs = ["DlsApiTest-se1", "DlsApiTest-se2", "DlsApiTest-se3"]
     all_locs = []
     for i in res:
           all_locs.append(i.host)
     correct = True
     for i in test_locs:
        correct *= i in all_locs
     msg = "Incorrect retrieval of all locations. (locations: %s)" % (all_locs)
     self.assert_(correct, msg)

     # Clean: Delete the entry (and -LFC- the dirs)
     try:
       self.api.delete(entryList, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)



##############################################################################
# Module's methods to return the suites
##############################################################################

def getSuite():
  suite = []
  suite.append(getSuite_General_Basic())
  suite.append(getSuite_General_Multi())
  suite.append(getSuite_General_GetFileBlocks())
  suite.append(getSuite_General_Dump())
  return unittest.TestSuite(suite)
  
def getSuite_General_Basic():
  return unittest.makeSuite(TestDlsApi_General_Basic)
   
def getSuite_General_Multi():
  return unittest.makeSuite(TestDlsApi_General_MultipleArgs)
   
def getSuite_General_GetFileBlocks():
  return unittest.makeSuite(TestDlsApi_General_GetFileBlocks)
   
def getSuite_General_Dump():
  return unittest.makeSuite(TestDlsApi_General_Dump)
   

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
  print "   \"getfb\" ==> Tests on getFileBlocks functions (maybe slow)" 
  print "   \"dump\" ==> Tests on dump and getAllLocations functions (maybe slow)" 
  
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
     sys.stderr.write("Incorrect conf file, leaving...\n")
     sys.exit(-1)
  from dlsDataObjects import *
  from dlsDataObjects import ValueError as DlsObjValueError
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
     if(sys.argv[2] == "dump"):
        suite = getSuite_General_Dump()

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
