#!/usr/bin/env python
 
#
# $Id: DlsLfcApiTest.py,v 1.4 2006/10/19 10:22:36 delgadop Exp $
#
# DLS Client Functional Test Suite. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

import unittest

import anto_utils

from commands import getstatusoutput
run = getstatusoutput
from os import putenv, unsetenv, chdir, getcwd, environ
from time import time
import sys
from stat import *

# Need a global variable here
conf = {}
name = "DlsApiTest.py"


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

      # If there is a verbosity, use it
      verb = self.conf.get("DLS_VERBOSITY")

      # Create the interface
      self.session = False
      self.clean   = True
      self.cleanDirs = False
      try:
        self.api = dlsClient.getDlsApi(self.type, self.host+self.testdir)
        if(verb): self.api.setVerbosity(int(verb))
      except DlsApiError, inst:
        msg = "Error in interface creation: %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)


  def tearDown(self):
        
     # Common clean up (if not already done)
     if(self.clean):
        fBList = map(DlsFileBlock, ["f1", "f2", "f3"])
        entryList = map(DlsEntry, fBList, [])
        try:
          self.api.delete(entryList, all = True, force = True)
        except DlsApiError, inst:
          msg = "Error in delete(%s): %s" % (map(str, entryList), inst)
          self.assertEqual(0, 1, msg)

     if(self.cleanDirs):
        fBList2 = []
        fBList2.append(map(DlsFileBlock, ["dir1/f2", "dir1/dir2/f2", "dir1/dir2/f3"]))
        fBList2.append(map(DlsFileBlock, ["dir1/f1", "dir1/dir2", "dir1/dir3", "dir1"]))
        fBList2.append(map(DlsFileBlock, ["dir2/dir3/f1", "dir2/dir3", "dir2"]))
        
        for i in fBList2:
           entryList = map(DlsEntry, i, [])
           try:
             self.api.delete(entryList, all = True, force = True)
           except DlsApiError, inst:
             msg = "Error in delete(%s): %s" % (map(str, entryList), inst)
             self.assertEqual(0, 1, msg)

     # This should be the proper way to clean, but it is risky
     # (Imagine somebody sets test root to "/grid/cms"!)
     # TODO: Create a testing subdir, just as the CLI suite does
#     try:
#        fBList = self.api.listFileBlocks("/", recursive = True)
#        while(fBList):
#           self.api.delete(map(DlsEntry, fBList, []), all = True, force = True)
#           fBList = self.api.listFileBlocks("/", recursive = True)
#     except DlsApiError, inst:
#       msg = "Error in delete(%s): %s" % (map(str, fbList), inst)
#       self.assertEqual(0, 1, msg)

     # End session
     if(self.session):
         self.api.endSession()



###########################################
# Classes for LFC-specific DLS API testing
###########################################

# Parent Class for LFC tests
# ==========================

class TestDlsApi_LFC(TestDlsApi):
  def setUp(self):
     # Invoke parent
     TestDlsApi.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi.tearDown(self)



## Class for LFC general options and setup tests
## =============================================
#
#class TestDlsApi_LFC_general(TestDlsApi_LFC):
#  def setUp(self):
#     # Invoke parent
#     TestDlsApi_LFC.setUp(self)
#     
#  def tearDown(self):
#     # Invoke parent
#     TestDlsApi_LFC.tearDown(self)


# Class for LFC attributes tests
# ==============================

class TestDlsApi_LFC_Attrs(TestDlsApi_LFC):
  def setUp(self):
     # Invoke parent
     TestDlsApi_LFC.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_LFC.tearDown(self)


  # Test addition and get-se, for attributes
  def testAttrsAdditionListGetSE(self):
  
     fB = DlsFileBlock("f1")
     loc1 = DlsLocation("DlsApiTest-se1")
     
     # Session
     self.session = True
     self.api.startSession()
  
     # Generate GUID 
     cmd = "uuidgen"
     st, guid = run(cmd)
     msg = "Error generating the GUID",guid
     self.assertEqual(st, 0, msg)
     
     # Define the attributes
     fB.attribs["filesize"] = 400
     fB.attribs["guid"] = guid
     fB.attribs["filemode"] = 0711
     surl = "srm://my_sfn/%s" % guid
     loc1.attribs["sfn"] = surl
     loc1.attribs["f_type"] = "P"
     loc1.attribs["ptime"] = "45"
     loc1.attribs["atime"] = "xxx"

     # Addition with attributes 
     entry = DlsEntry(fB, [loc1])
     prevtime = int(time())
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     # Test the fileblock attributes listing
     try:
       res = self.api.listFileBlocks("f1")
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % ("f1", inst)
       self.assertEqual(0, 1, msg)
     msg = "The listing of attributes is not as expected: %s" % res[0]
     self.assertEqual(res[0].attribs["filesize"], 400, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IRUSR, 256, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IWUSR, 128, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IXUSR, 64, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IRGRP, 0, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IWGRP, 0, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IXGRP, 8, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IROTH, 0, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IWOTH, 0, msg)
     self.assertEqual(res[0].attribs["filemode"] & S_IXOTH, 1, msg)

     fB2 = DlsFileBlock("/")
     try:
       res2 = self.api.listFileBlocks(fB2, longList = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB2, inst)
       self.assertEqual(0, 1, msg)
     msg = "The listing of attributes from dir is not as from FileBlock: %s" % res2[0]
     self.assertEqual(res2[0].attribs, res[0].attribs, msg)
    
     # NOTE: This is even more purely LFC 
     # Test the guid retrieval
     try:
        retrievedGuid = self.api.getGUID("f1")
     except DlsApiError, inst:
        msg = "Error in getGUID(f1): %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)
     msg = "The guid retrieval was not correct"
     self.assertEqual(retrievedGuid, guid, msg)
     
     # Same with FileBlock as argument
     try:
        retrievedGuid = (self.api.getGUID(fB)).getGuid()
     except DlsApiError, inst:
        msg = "Error in getGUID(%s): %s" % (fB, inst) 
        self.assertEqual(-1, 0, msg)
     msg = "The guid retrieval was not correct"
     self.assertEqual(retrievedGuid, guid, msg)

     
     # Test the replica attributes
     posttime = int(time())
     try:
       res = self.api.getLocations(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     msg = "Attributes of locations (%s) are not those expected" % (res[0].locations[0].attribs)
     self.assert_(prevtime <= res[0].locations[0].attribs["atime"], msg)
     self.assert_(posttime >= res[0].locations[0].attribs["atime"], msg)
     self.assertEqual(res[0].locations[0].attribs["sfn"], "srm://my_sfn/"+guid, msg)
     self.assertEqual(res[0].locations[0].attribs["f_type"], "P", msg)
     self.assertEqual(res[0].locations[0].attribs["ptime"], 45, msg)
    
     # NOTE: This is even more purely LFC 
     # Test the SURL retrieval
     try:        
        entry = self.api.getSURL(entry)
        retrievedSurl = (entry.getLocation("DlsApiTest-se1")).getSurl()
     except DlsApiError, inst:
        msg = "Error in getSURL(%s): %s" % (entry, inst) 
        self.assertEqual(-1, 0, msg)
     msg = "The retrieved surl (%s) is not that expected (%s)" % (retrievedSurl, surl)
     self.assertEqual(retrievedSurl, surl, msg)



  # Test basic update
  def testUpdate(self):
  
     fB = DlsFileBlock("f1")
     fB.attribs["filesize"] = "123"
     fB.attribs["csumtype"] = "CS"
     fB.attribs["csumvalue"] = "4321"
     loc1 = DlsLocation("DlsApiTest-se1")
     loc1.attribs["ptime"] = 111
     
     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


     # Now update 
     prevtime = int(time())
     entry.fileBlock.attribs["filesize"] = "222"
     entry.fileBlock.attribs["csumtype"] = "AD"
     entry.fileBlock.attribs["csumvalue"] = "444"
     entry.locations[0].attribs = {"ptime": "333", "atime": "xxx"}
     try:
       self.api.update(entry)
     except DlsApiError, inst:
       msg = "Error in update([%s, %s], trans = False)" % (entry, entry2)
       self.assertEqual(0, 1, msg)

     # Check upate was performed
     try:
       res = self.api.listFileBlocks(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = res[0].attribs["filesize"] == 222
     correct *= res[0].attribs["csumtype"] == "AD"
     correct *= res[0].attribs["csumvalue"] == "444"
     msg = "FileBlocks were not updated correctly (res[0]: %s)" % (res[0])
     self.assert_(correct, msg)

     posttime = int(time())
     try:
       res = self.api.getLocations(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = res[0].locations[0].attribs["ptime"] == 333
     correct *= prevtime <= res[0].locations[0].attribs["atime"]
     correct *= posttime >= res[0].locations[0].attribs["atime"]
     msg = "Locations were not updated correctly (res[0]: %s)" % (res[0])
     self.assert_(correct, msg)

     # Clean: Delete the entry
     try:
       self.api.delete(entry, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)



  # Test update multiple
  def testUpdateMulti(self):
  
     fB = DlsFileBlock("f1")
     fB.attribs["filesize"] = "123"
     fB2 = DlsFileBlock("f2")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc1.attribs["ptime"] = 111
     loc2 = DlsLocation("DlsApiTest-se2")
     loc2.attribs = {"ptime": 999, "aaa": 888}
     
     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


     # Now update (correct part should be performed)
     entry.fileBlock.attribs = {"filesize": 222, "asdf": 777}
     entry.locations[0].attribs = {"ptime": 333, "jjj": 666}
     entry2 = DlsEntry(fB2, [loc2])
     try:
       self.api.update([entry, entry2])
     except DlsApiError, inst:
       msg = "Error in update([%s, %s], trans = False)" % (entry, entry2)
       self.assertEqual(0, 1, msg)

     # Check upate was performed
     try:
       res = self.api.listFileBlocks(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = res[0].attribs["filesize"] == 222
     correct *= (not res[0].attribs.has_key("asdf"))
     msg = "FileBlocks were not updated correctly (res[0]: %s)" % (res[0])
     self.assert_(correct, msg)

     try:
       res = self.api.getLocations(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = res[0].locations[0].attribs["ptime"] == 333
     correct *= not res[0].locations[0].attribs.has_key("jjj")
     msg = "Locations were not updated correctly (res[0]: %s)" % (res[0])
     self.assert_(correct, msg)

     # Clean: Delete the entry
     try:
       self.api.delete(entry, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


  # Test deletion with attributes
  def testDeletion(self):
  
     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     fB3 = DlsFileBlock("f3")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("DlsApiTest-se3")
     loc3.attribs = {"f_type": "P"}
     
     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB, [loc1, loc2])
     entry2 = DlsEntry(fB2, [loc2])
     entry3 = DlsEntry(fB3, [loc1, loc3])
     try:
       self.api.add([entry, entry2, entry3], checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s]): %s" % (entry, entry2, entry3, inst)
       self.assertEqual(0, 1, msg)
       
     # Delete some replicas (but keep FileBlock)
     try:
       self.api.delete([entry, entry2], keepFileBlock = True)
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s]): %s" % (entry, entry2, inst)
       self.assertEqual(0, 1, msg)

     # Now get locations
     try:
       res = self.api.getLocations([fB, fB2])
     except DlsApiError, inst:
       msg = "Error in getLocations([%s, %s]): %s" % (fB, fB2, inst)
       self.assertEqual(0, 1, msg)
     correct = len(res[0].locations) == 0
     correct *= len(res[1].locations) == 0
     msg = "FileBlocks were not removed correctly (res[0]: %s, res[1]: %s)" % (res[0], res[1])
     self.assert_(correct, msg)

     # Next, fail to delete permanent replicas
     try:
       self.api.delete(entry3, all = True)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry3, inst)
       self.assertEqual(0, 1, msg)
       
     # Check permanent location is still there
     try:
       res = self.api.getLocations(fB3)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = len(res[0].locations) == 1
     correct *= (res[0].getLocation("DlsApiTest-se3") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Now, really delete the entry ("force" flag)
     try:
       self.api.delete(entry3, force = True)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry3, inst)
       self.assertEqual(0, 1, msg)

     # Check the entry is gone
     try:
       res = self.api.listFileBlocks(entry3.fileBlock)
       msg = "Unexpected success in listFileBlocks(%s)" % (entry3.fileBlock)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       pass

     # Clean: Delete the entries
     try:
       self.api.delete([entry, entry2], all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s]): %s" % (entry, entry2, inst)
       self.assertEqual(0, 1, msg)



  
# Class for LFC transactions tests
# ================================

class TestDlsApi_LFC_Trans(TestDlsApi_LFC):
  def setUp(self):
     # Invoke parent
     TestDlsApi_LFC.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_LFC.tearDown(self)


  # Test transactions on addition 
  def testAddTrans(self):
  
     fB = DlsFileBlock("f1")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("DlsApiTest-se3")
     
     entry = DlsEntry(fB, [loc1, loc2])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     entry2 = DlsEntry(fB, [loc3, loc2])
     try:
       self.api.add(entry, trans = True, checkLocations = False)
     except DlsApiError, inst:
       expected = "Transaction operations rolled back"
       msg = "Results (%s) of add(trans) are not those expected (%s)" % (inst, expected)
       contains = str(inst).find(expected) != -1
       self.assert_(contains, msg)

     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest-se1") != None)
     correct *= (res[0].getLocation("DlsApiTest-se2") != None)
     correct *= (res[0].getLocation("DlsApiTest-se3") == None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
       
     try:
       self.api.add(entry2, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest-se1") != None)
     correct *= (res[0].getLocation("DlsApiTest-se2") != None)
     correct *= (res[0].getLocation("DlsApiTest-se3") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)

     # Clean: Delete the entries
     try:
       self.api.delete(entry, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


  # Test transactions on update 
  def testUpdateTrans(self):
  
     fB = DlsFileBlock("f1")
     fB.attribs["filesize"] = 123
     fB.attribs["csumtype"] = "CS"
     fB.attribs["csumvalue"] = "4321"
     fB2 = DlsFileBlock("f2")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc1.attribs["ptime"] = "111"
     loc2 = DlsLocation("DlsApiTest-se2")
     loc2.attribs = {"ptime": 999, "aaa": 888}
     
     # First add
     entry = DlsEntry(fB, [loc1])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     # Now, erroneous update on transactions (should be rolled back)
     entry.fileBlock.attribs = {"filesize": 222, "asdf": 777}
     entry.locations[0].attribs = {"ptime": 333, "jjj": 666}
     entry2 = DlsEntry(fB2, [loc2])
     try:
       self.api.update([entry, entry2], trans = True)
       msg = "Unexpected success in update([%s, %s], trans = True)" % (entry, entry2)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       expected = "Transaction operations rolled back"
       contains = str(inst).find(expected) != -1
       msg = "Results with update(trans)(%s, %s) are not those expected(%s)"%(entry, entry2, inst)
       self.assert_(contains, msg)
 
     # Check that roll-back
     try:
       res = self.api.listFileBlocks(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = res[0].attribs["filesize"] == 123
     msg = "FileBlocks wrong update was not rolled back (res[0]: %s)" % (res[0])
     self.assert_(correct, msg)

     try:
       res = self.api.getLocations(fB, longList = True)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = res[0].locations[0].attribs["ptime"] == 111
     msg = "Locations wrong update was not rolled back (res[0]: %s)" % (res[0])
     self.assert_(correct, msg)

     # Clean: Delete the entry
     try:
       self.api.delete(entry, all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)




# Class for LFC "other" tests
# ===========================

class TestDlsApi_LFC_Other(TestDlsApi_LFC):
  def setUp(self):
     # Invoke parent
     TestDlsApi_LFC.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsApi_LFC.tearDown(self)

  def testEndpointAndInterface(self):
     fB = DlsFileBlock("f1")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")

     # Test the root path
     entry = DlsEntry(fB, [loc1, loc2])
     try:
       api2 = dlsClient.getDlsApi("DLS_TYPE_LFC", self.host)
       api2.setVerbosity(self.api.verb)
     except DlsApiError, inst:
       expected = "No LFC's root directory specified for DLS use"
       msg = "Results (%s) don't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)
     
     # Now correct endpoint but incorrect type (DLI not supporting addition)
     try:
       api2 = dlsClient.getDlsApi("DLS_TYPE_DLI", self.host+self.testdir)
       api2.setVerbosity(self.api.verb)
       api2.add(entry, checkLocations = False)
     except DlsApiError, inst:
       expected = "This is just a base class! This method should "
       expected += "be implemented in an instantiable DLS API class"
       msg = "Results (%s) don't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)


  # Test erroneous addition in non-accessible location
  def testErroneousAddition(self):
   
     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("f2")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     loc3 = DlsLocation("www.google.com")
     loc4 = DlsLocation("www.cern.ch")

     # Erroneous root directory (not permitted)
     try:
       api2 = dlsClient.getDlsApi("DLS_TYPE_LFC", self.host+'/')
       api2.setVerbosity(self.api.verb)
     except DlsApiError, inst:
       msg = "Error in api binding): %s" % (inst)
       self.assertEqual(0, 1, msg)

     entry = DlsEntry(fB, [loc1, loc2])
     try:
       api2.add(entry, checkLocations = False)
     except DlsApiError, inst:
       expected = "Warning: Error creating the FileBlock %s: Permission denied" % (f1)
       msg = "Results (%s) are not those expected (%s)" % (inst, expected)
       self.assertEqual(str(inst), expected, msg)


     # Wrong location with other proper ones (errorTolerant)
     entry = DlsEntry(fB, [loc1, loc3])
     try:
       self.api.add(entry)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("www.google.com") != None)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
    
     # Wrong location with other proper ones (non errorTolerant)
     entry = DlsEntry(fB, [loc2, loc4])
     try:
       self.api.add(entry, errorTolerant = False)
       msg = "Unexpected success in add(%s)" % (entry)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       pass
     try:
       res = self.api.getLocations(fB)
     except DlsApiError, inst:
       pass 
     correct = (res[0].getLocation("www.google.com") != None)
     correct *= (len(res[0].locations) == 1)
     msg = "Locations were not correctly retrieved (entry: %s)" % (res[0])
     self.assert_(correct, msg)
    

     # Wrong location alone (though errorTolerant)
     entry = DlsEntry(fB2, [loc2])
     try:
       self.api.add(entry)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
     try:
       self.api.listFileBlocks(fB2)
       msg = "Unexpected success in listFileBlock(%s)" % (fB2)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB2, inst)
       expected = "Error accessing FileBlock %s: No such file or directory" % (fB2.name)
       msg = "Results (%s) don't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)
     
     # Clean: Delete the entries
     try:
       self.api.delete(entry, all = True)
       self.api.delete(DlsEntry(fB2,[]), all = True)
       self.clean = False
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


  # Test deletion of a directory using CL args
  def testDeletionDir(self):
     self.cleanDirs = True

     fB = DlsFileBlock("dir1/f2")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")
     
     # Session
     self.session = True
     self.api.startSession()

     # Create a non-empty dir
     entry = DlsEntry(fB, [loc1, loc2])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     # Test wrong arguments (without -a)
     fB2 = DlsFileBlock("dir1")
     entry2 = DlsEntry(fB2, [])
     try:
       self.api.delete(entry2)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)

     # Test trying to remove non-empty 
     try:
       self.api.delete(entry2, all = True, errorTolerant = False)
       msg = "Unexpected success in delete(%s)" % (entry2)
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       expected = "Error deleting FileBlock directory dir1: File exists"
       msg = "Results (%s) of delete non empty don't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)

     # Now delete the file and remove the dir correctly
     try:
       self.api.delete(entry, all = True, errorTolerant = False)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)
     try:
       self.api.delete(entry2, all = True, errorTolerant = False)
     except DlsApiError, inst:
       msg = "Error in delete(empty %s): %s" % (entry2, inst)
       self.assertEqual(0, 1, msg)


     # Test the dir went away
     try:
       self.api.listFileBlocks(DlsFileBlock("dir1"))
       msg = "Unexpected success in listFileBlock(%s)" % ("dir1")
       self.assertEqual(0, 1, msg)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % ("dir1", inst)
       expected = "Error accessing FileBlock %s: No such file or directory" % ("dir1")
       msg = "Error msg of listFileBlock (%s) doesn't contain expected (%s)" % (inst, expected)
       contains = (str(inst)).find(expected) != -1
       self.assert_(contains, msg)
       self.clean = False
       self.cleanDirs = False


  # Test recursive listing
  def testRecursiveListing(self):
     self.cleanDirs = True

     fB = DlsFileBlock("f1")
     fB2 = DlsFileBlock("dir1/f2")
     fB3 = DlsFileBlock("dir1/dir2/f3")
     fB4 = DlsFileBlock("dir1/dir3/fXX")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")

     # Session
     self.session = True
     self.api.startSession()

     # First create a dir hierarchy
     entry = DlsEntry(fB, [loc1])
     entry2 = DlsEntry(fB2, [loc1, loc2])
     entry3 = DlsEntry(fB3)
     entry4 = DlsEntry(fB4, [loc2])
     try:
       self.api.add([entry,entry2,entry3,entry4], checkLocations=False, allowEmptyBlocks=True)
     except DlsApiError, inst:
       msg = "Error in add([%s, %s, %s, %s]): %s" % (entry, entry2, entry3, entry4, inst)
       self.assertEqual(0, 1, msg)
     try:
       # Remove the fXX file (leaving an empty dir1/dir3)
       self.api.delete(entry4)
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry4, inst)
       self.assertEqual(0, 1, msg)

     # Now list recursively, for several cases...
       #... Just one file (just like normal list)
     try:
       res = self.api.listFileBlocks(fB, recursive = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB, inst)
       self.assertEqual(0, 1, msg)
     correct = len(res) == 1
     correct *= res[0].name == "f1"
     msg = "FileBlocks were not correctly listed (fB: %s)" % (res[0])
     self.assert_(correct, msg)
       #... empty dir (should return nothing)
     fB4.name = "dir1/dir3"
     try:
       res = self.api.listFileBlocks(fB4, recursive = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB4, inst)
       self.assertEqual(0, 1, msg)
     correct = len(res) == 0
     msg = "FileBlocks were not correctly listed ([fB]: %s)" % (res)
     self.assert_(correct, msg)
       #... root dir (should return whole hierarchy)
     fB4.name = "/"
     try:
       res = self.api.listFileBlocks(fB4, recursive = True)
     except DlsApiError, inst:
       msg = "Error in listFileBlocks(%s): %s" % (fB4, inst)
       self.assertEqual(0, 1, msg)
     nameList = []
     for i in res:
        nameList.append(i.name)
     correct = len(res) == 4
     correct *= ("f1" in nameList)
     correct *= ("dir1/f2" in nameList)
     correct *= ("dir1/dir2/f3" in nameList)
     correct *= ("dir1/dir3/" in nameList)
     msg = "FileBlocks were not correctly listed ([hosts]: %s)" % (nameList)
     self.assert_(correct, msg)


     # Clean: Delete all the dirs and entries
     try:
       self.api.delete(entry, all = True)
       self.api.delete(entry2, all = True)
       self.api.delete(entry3, all = True)
       entry4 = DlsEntry(DlsFileBlock("dir1/dir2"), [])
       self.api.delete(entry4, all = True)
       entry4 = DlsEntry(DlsFileBlock("dir1/dir3"), [])
       self.api.delete(entry4, all = True)
       entry4 = DlsEntry(DlsFileBlock("dir1"), [])
       self.api.delete(entry4, all = True)
       self.clean = False
       self.cleanDirs = False
     except DlsApiError, inst:
       msg = "Error in delete dirs hierarchy: %s" % (inst)
       self.assertEqual(0, 1, msg)


  # Test dump entries of a single dir (non recursive)
  def testDumpEntries(self):
     self.cleanDirs = True

     fBList = map(DlsFileBlock, ["f1", "dir1/dir2/f2", "dir1/dir2/f3"])
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
       res = self.api.dumpEntries("dir1/dir2")
     except DlsApiError, inst:
       msg = "Error in dumpEntries(\"/\"): %s" % (inst)
       self.assertEqual(0, 1, msg)
     correct = (len(res)==2)
     msg = "Incorrectal dump (number of entries not two: %s)"%(res)
     self.assert_(correct, msg)
     correct = (res[0].fileBlock.name=="f2") and (res[1].fileBlock.name=="f3")
     correct *= (len(res[0].locations)==2) and (len(res[1].locations)==1)
     correct *= (res[0].locations[0].host=="DlsApiTest-se2")
     correct *= (res[0].locations[1].host=="DlsApiTest-se3")
     correct *= (res[1].locations[0].host=="DlsApiTest-se3")
     msg = "Incorrectal dump (entries: %s, %s)"%(res[0], res[1])
     self.assert_(correct, msg)

     # Clean: Delete the entries and dirs
     try:
       self.api.delete(entryList, all = True)
       entry2 = DlsEntry(DlsFileBlock("dir1/dir2"), [])
       self.api.delete(entry2, all = True)
       entry3 = DlsEntry(DlsFileBlock("dir1"), [])
       self.api.delete(entry3, all = True)
       self.clean = False
       self.cleanDirs = False
     except DlsApiError, inst:
       msg = "Error in delete([%s, %s, %s]): %s" % (entryList[0], entryList[1], entryList[2], inst)
       self.assertEqual(0, 1, msg)


  # Test renaming of a dir and createParents flag
  def testRenaming(self):
     self.cleanDirs = True

     fB1 = DlsFileBlock("dir1/f1")
     dir1 = "dir1"
     dir2 = DlsFileBlock("dir2/dir3")
     fB2 = DlsFileBlock("dir2/dir3/f1")
     loc1 = DlsLocation("DlsApiTest-se1")
     loc2 = DlsLocation("DlsApiTest-se2")

     # Session
     self.session = True
     self.api.startSession()

     # First add
     entry = DlsEntry(fB1, [loc1, loc2])
     try:
       self.api.add(entry, checkLocations = False)
     except DlsApiError, inst:
       msg = "Error in add(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)

     # Now rename it
     try:
       self.api.renameFileBlock(dir1, dir2, createParent=True)
     except DlsApiError, inst:
       msg = "Error in renameFileBlock(%s, %s): %s" % (dir1, dir2, inst)
       self.assertEqual(0, 1, msg)
     # Check new entry
     try:
       res = self.api.getLocations(fB2)
     except DlsApiError, inst:
       msg = "Error in getLocations(%s): %s" % (fB2, inst)
       self.assertEqual(0, 1, msg)
     correct = (res[0].getLocation("DlsApiTest-se1") != None)
     correct *= (res[0].getLocation("DlsApiTest-se2") != None)
     msg = "Rename was not correct (retrieving new entry: %s)" % (res[0])
     self.assert_(correct, msg)
     # Check old entry and dir are gone
     correct = False
     try:
       res = self.api.listFileBlocks(fB1)
     except DlsApiError, inst:
       correct = True
     msg = "Rename was not correct (old entry still there: %s)" % (res[0])
     self.assert_(correct, msg)
     correct = False
     try:
       res = self.api.listFileBlocks(dir1)
     except DlsApiError, inst:
       correct = True
     msg = "Rename was not correct (old entry still there: %s)" % (res[0])
     self.assert_(correct, msg)

     # Clean: Delete the entry (and -LFC- the dirs)
     try:
       entry = DlsEntry(DlsFileBlock("dir1/f1"), [])
       self.api.delete(entry, all = True)
       entry = DlsEntry(DlsFileBlock("dir2/dir3/f1"), [])
       self.api.delete(entry, all = True)
       entry2 = DlsEntry(DlsFileBlock("dir2/dir3"), [])
       self.api.delete(entry2, all = True)
       entry3 = DlsEntry(DlsFileBlock("dir2"), [])
       self.api.delete(entry3, all = True)
       entry3 = DlsEntry(DlsFileBlock("dir1"), [])
       self.api.delete(entry3, all = True)
       self.clean = False
       self.cleanDirs = False
     except DlsApiError, inst:
       msg = "Error in delete(%s): %s" % (entry, inst)
       self.assertEqual(0, 1, msg)


##############################################################################
# Module's methods to return the suites
##############################################################################

def getSuite():
  suite = []
  suite.append(getSuite_LFC_Attrs())
  suite.append(getSuite_LFC_Trans())
  suite.append(getSuite_LFC_Other())
  return unittest.TestSuite(suite)
  
def getSuite_LFC_Attrs():
  return unittest.makeSuite(TestDlsApi_LFC_Attrs)
   
def getSuite_LFC_Trans():
  return unittest.makeSuite(TestDlsApi_LFC_Trans)
   
def getSuite_LFC_Other():
  return unittest.makeSuite(TestDlsApi_LFC_Other)
   

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
  print "This script runs LFC-specific tests on the DLS API."
  print "It requires a configuration file with some variables (refer to the example)"
  print
  print "An optional additional argument may be used to execute a desired subset"
  print "of the tests. Default is to execute all of them."
  print "   \"attrs\" ==> Tests on attributes support"
  print "   \"trans\" ==> Test transactions in add and update"
  print "   \"other\" ==> Other tests (setup, dir deletion/dump/rename, recursive list)" 
  
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
  from dlsDataObjects import *
  from dlsApi import *
  import dlsClient
  setConf(the_conf)

  suite = None
  if(len(sys.argv) > 2):
     if(sys.argv[2] == "attrs"):
        suite = getSuite_LFC_Attrs()
     if(sys.argv[2] == "trans"):
        suite = getSuite_LFC_Trans()
     if(sys.argv[2] == "other"):
        suite = getSuite_LFC_Other()

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
