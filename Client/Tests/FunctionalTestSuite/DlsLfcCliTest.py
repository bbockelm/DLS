#!/usr/bin/env python
 
#
# $Id: DlsLfcCliTest.py,v 1.13 2006/09/21 15:24:50 delgadop Exp $
#
# DLS Client Functional Test Suite. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

import unittest

import anto_utils

from commands import getstatusoutput
from os import putenv, unsetenv, chdir, getcwd, environ
from time import strftime
import sys 

run = getstatusoutput

# Need a global variable here
conf_file = ""


# TODO: Basic tests for rename, dump, get-all-locations... but already tested in the API


##############################################################################
# Parent Class for DLS CLI testing
##############################################################################

class TestDlsCli(unittest.TestCase):
 
  def setUp(self):
      # "Source" the configuration file
      self.conf = anto_utils.SimpleConfigParser()
      try:
         self.conf.read(conf_file)
      except Exception:
         msg = "There were errors parsing the configuration file ("+conf_file+"). Please, review it"
         self.assertEqual(1, 0, msg)

      # Check that all the env vars are there
      msg = "Required variable has not been defined!"

      msg1 = msg + " (DLS_TEST_DIR)"
      self.testdir = self.conf.get("DLS_TEST_DIR")
      self.assert_(self.testdir, msg1)
      self.testdir = self.testdir + "/TESTDIR"

      msg1 = msg + " (DLS_TEST_SERVER)"
      self.host = self.conf.get("DLS_TEST_SERVER")
      self.assert_(self.host, msg1)

      msg1 = msg + " (DLS_CODE_PATH)"
      self.path = self.conf.get("DLS_CODE_PATH")
      self.assert_(self.path, msg1)

      msg1 = msg + " (LFC_DEL_PATH )"
      self.lfcdel = self.conf.get("LFC_DEL_PATH")
      self.assert_(self.lfcdel, msg1)

      # Set necessary environmental variables
      putenv("DLS_ENDPOINT",  self.host + self.testdir)
      putenv("DLS_TYPE", "DLS_TYPE_LFC")
      # DLS code for direct api calls
      sys.path.append(self.path)

      # Inside the given directory create a subdir where to work (and remove)
      putenv("LFC_HOST", self.host)
      cmd = "lfc-mkdir " + self.testdir
      st, out = run(cmd)
      msg = "Error creating the LFC dir!",out 
      self.assertEqual(st, 0, msg)

      # Unset LFC_HOST, as it has to be managed by the commands
      unsetenv("LFC_HOST")

  def tearDown(self):
     # Common clean up
     putenv("LFC_HOST", self.host)
     st, out = run(self.lfcdel+"/lfc-del-dir -r "+self.testdir)
     unsetenv("LFC_HOST")


##############################################################################
# Classes for DLS CLI testing from command line arguments
##############################################################################


######################################################
# Class for DLS CLI testing: General options and setup
######################################################

class TestDlsCli_FromArgs_General(TestDlsCli):
  def setUp(self):
     # Invoke parent
     TestDlsCli.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli.tearDown(self)

  # Test endpoint and interface binding
  def testEndpointAndInterface(self):
     # Unset DLS_ENPOINT, will try the options 
     unsetenv("DLS_ENDPOINT")
      
     # First, wrong interface selection
     cmd = self.path + "/dls-add -i SOMETHING c1"
     st, out = run(cmd)
     expected = "Unsupported interface type: SOMETHING\nSuppported values: ['DLS_TYPE_LFC', 'DLS_TYPE_DLI', 'DLS_TYPE_MYSQL']"
     msg = "Results (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)

     # Next the LFC host
     cmd = self.path + "/dls-add c1"
     st, out = run(cmd)
     expected = "Error when binding the DLS interface: Could not set the DLS server to use"
     msg = "Results (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
     # Now the LFC root path
     cmd = self.path + "/dls-add -e %s c1" % (self.host)
     st, out = run(cmd)
     expected = "Error when binding the DLS interface: "
     expected += "No LFC's root directory specified for DLS use"
     msg = "Results (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
     # Now correct endpoint but incorrect type
     cmd = self.path + "/dls-add -e %s%s -i DLS_TYPE_DLI c1" % (self.host, self.testdir)
     st, out = run(cmd)
     expected = "Error in the entry(ies) insertion: This is just a base class! "
     expected += "This method should be implemented in an instantiable DLS API class."
     msg = "Results (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
   
     # With everything right, the thing should work 
     cmd = self.path + "/dls-add -e %s%s c1" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-add -e %s%s c1: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add -e %s%s -i DLS_TYPE_LFC c2" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-add -e %s%s -i DLS_TYPE_LFC c2: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-update -e %s%s c1 filesize=100" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-update -e %s%s c1 filesize=100: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se -e %s%s c1" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-get-se -e %s%s c1: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se -e %s%s -i DLS_TYPE_DLI c1" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-get-se -e %s%s -i DLS_TYPE_DLI c1: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-fileblock -e %s%s CliTest-NOTHING" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-get-fileblock -e %s%s CliTest-NOTHING: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-delete -e %s%s -a c1" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-delete -e %s%s -a c1: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)
  

  # Test verbosity (on update after addition)
  def testVerbosity(self):
     cmd = self.path + "/dls-add c1 CliTest-se1 CliTest-se2"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest-se1 CliTest-se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-update -v 0 c1 CliTest-se5"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = ""
     msg = "The results obtained (%s) with dls-update -v are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-update -v 1 c1 CliTest-se5"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "Warning: FileBlock c1 - Not all locations could be found and updated"
     msg = "The results obtained (%s) with dls-update -v are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-update -v 2 c1 CliTest-se5"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     for i in ["--DlsApi.update","--Starting session","--updateFileBlock","--lfc.lfc_getreplica",\
               "Warning: FileBlock c1 - Not all locations", "--Ending session"]:
       expected = i
       msg = "The results obtained (%s) with dls-update -v are not those expected  (%s)"\
           % (out, expected)
       contains = out.find(expected) != -1
       self.assert_(contains, msg)


  # Test sessions (on dls-get-se for -l only) 
  def testSession(self):
     cmd = self.path + "/dls-add c1 CliTest-se4 CliTest-se5 CliTest-se6"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest-se4 CliTest-se5 CliTest-se6",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se -v 2 -l c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se -v 2 -l c1",out 
     self.assertEqual(st, 0, msg)
     for i in ["--Starting session", "--Ending session"]:
       expected = i
       contains = out.find(expected) != -1
       msg = "The results obtained (%s) with dls-get-se -v 2 -l are not those expected  (%s)"\
           % (out, expected)
       self.assert_(contains, msg)

     cmd = self.path + "/dls-get-se -v 2 c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     for i in ["--Starting session", "--Ending session"]:
       not_expected = i
       not_contains = out.find(not_expected) == -1
       msg = "The results obtained (%s) with dls-get-se should not contain: %s"\
           % (out, not_expected)
       self.assert_(not_contains, msg)


  # Test transactions on addition (from CL arguments) 
  def testAddTrans(self):
     cmd = self.path + "/dls-add c1 CliTest-se1 CliTest-se2"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest-se1 CliTest-se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-add -v 2 -t c1 CliTest-se3 CliTest-se1"
     st, out = run(cmd)
     expected = "Transaction operations rolled back"
     contains = out.find(expected) != -1
     msg = "The results obtained (%s) with dls-add -t are not those expected  (%s)"\
         % (out, expected)
     self.assert_(contains, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest-se1\nCliTest-se2"
     msg = "The results obtained (%s) with dls-get-se are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-add -v 2 c1 CliTest-se3 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in dls-add -v 2 c1 CliTest-se3 CliTest-se1",out 
     self.assertEqual(st, 0, msg)
     expected = "--Ending session"
     contains = out.find(expected) != -1
     msg = "The results obtained (%s) with dls-add (without -t) are not those expected  (%s)"\
         % (out, expected)
     self.assert_(contains, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest-se1\nCliTest-se2\nCliTest-se3"
     msg = "The results obtained (%s) with dls-get-se are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)


#####################################
# Class for DLS CLI testing: Deletion
#####################################

class TestDlsCli_FromArgs_Deletion(TestDlsCli):
  def setUp(self):
     # Invoke parent
     TestDlsCli.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli.tearDown(self)
    
  # Test deletion using command line arguments
  def testDeletion(self):
     # First add 
     cmd = self.path + "/dls-add c1 CliTest-se4 CliTest-se5 CliTest-se6"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest-se4 CliTest-se5 CliTest-se6",out 
     self.assertEqual(st, 0, msg)

     # Delete some replicas
     cmd = self.path + "/dls-delete c1 CliTest-se4 CliTest-se5"
     st, out = run(cmd)
     msg = "Error in dls-delete c1 CliTest-se4 CliTest-se5",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     msg = "The results obtained with dls-get-se are not those expected",out
     self.assertEqual(out, "CliTest-se6", msg)

     # Delete the entry by removing last location
     cmd = self.path + "/dls-delete c1 CliTest-se6"
     st, out = run(cmd)
     msg = "Error in dls-delete -a c1",out 
     self.assertEqual(st, 0, msg)

     # Check the FileBlock is also gone
     cmd = self.path + "/dls-list c1"
     st, out = run(cmd)
     msg = "Unexpected success in %s: %s" % (cmd, out)
     self.assert_(st != 0, msg)


  # Test deletion with attrs (force...) using CLI
  def testDeletionAttrs(self):
     # First add with a permanent replica
     cmd = self.path + "/dls-add f1 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add f2  CliTest-se2 CliTest-se3 f_type=P"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     # Delete some replicas (but keep FileBlock)
     cmd = self.path + "/dls-delete -k f1 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     # Now get locations
     cmd = self.path + "/dls-get-se f1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     expected = ""
     msg = "The results obtained with %s (%s) are not those expected (%s)" % (cmd, out, expected)
     self.assertEqual(out, expected, msg)

     # Next, fail to delete permanent replicas
     cmd = self.path + "/dls-delete -a f2"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     expected = "Warning: Not deleting custodial replica in CliTest-se3 of f2"
     msg = "The results obtained with %s (%s) are not those expected (%s)" % (cmd, out, expected)
     self.assertEqual(out, expected, msg)

     # Check permanent location is still there
     cmd = self.path + "/dls-get-se f2"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     expected = "CliTest-se3"
     msg = "The results obtained with %s (%s) are not those expected (%s)" % (cmd, out, expected)
     self.assertEqual(out, expected, msg)

     # Now, really delete the entry ("force" flag)
     cmd = self.path + "/dls-delete -x f2 CliTest-se3"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     # Check the entry is gone
     cmd = self.path + "/dls-list f2"
     st, out = run(cmd)
     msg = "Unexpected success in %s: %s" % (cmd, out)
     self.assert_(st != 0, msg)


  # Test deletion of a directory using CL args
  def testDeletionDir(self):
     # Create a non-empty dir
     cmd = self.path + "/dls-add dir1/f1 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in dls-add dir1/f1 CliTest-se1",out 
     self.assertEqual(st, 0, msg)

     # Test wrong arguments
     cmd = self.path + "/dls-delete dir1 CliTest-se-XXX"
     st, out = run(cmd)
     msg = "Error in dls-delete dir1 CliTest-se-XXX",out 
     self.assertEqual(st, 0, msg)
     expected = "Warning: Without \"all\" option, skipping directory dir1"
     msg = "The results obtained with dls-delete (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
     # Test trying to remove non-empty 
     cmd = self.path + "/dls-delete -a dir1"
     st, out = run(cmd)
     msg = "Error in dls-delete -a dir1",out 
     self.assertEqual(st, 0, msg)
     expected = "Warning: Error deleting FileBlock directory dir1: File exists"
     msg = "The results obtained with dls-delete (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)

     # Now delete the file and remove the dir correctly
     cmd = self.path + "/dls-delete -a dir1/f1"
     st, out = run(cmd)
     msg = "Error in dls-delete -a dir1/f1",out 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-delete -a dir1"
     st, out = run(cmd)
     msg = "Error in dls-delete -a dir1 (empty)",out 
     self.assertEqual(st, 0, msg)

     # Test the dir went away
     cmd = self.path + "/dls-get-se -l dir1"
     st, out = run(cmd)
     expected = "Error in the DLS query: Error retrieving locations for dir1: "
     expected += "No such file or directory."
     msg = "The results obtained with dls-delete (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)


##################################################
# Class for DLS CLI testing: Addition, GetSE, List
##################################################

class TestDlsCli_FromArgs_AddGetSEList(TestDlsCli):
  def setUp(self):
     # Invoke parent
     TestDlsCli.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli.tearDown(self)

  # Test addition and get-se using command line arguments
  def testAdditionListGetSE(self):
     cmd = self.path + "/dls-add c1 CliTest-se4 CliTest-se5 CliTest-se6"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest-se4 CliTest-se5 CliTest-se6",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest-se4\nCliTest-se5\nCliTest-se6"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
     cmd = self.path + "/dls-add c2"
     st, out = run(cmd)
     msg = "Error in dls-add c2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-list /"
     st, out = run(cmd)
     msg = "Error in dls-list /",out 
     self.assertEqual(st, 0, msg)
     expected = "c1\nc2"
     msg = "The results obtained with dls-list (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-list c2"
     st, out = run(cmd)
     msg = "Error in dls-list c2",out 
     self.assertEqual(st, 0, msg)
     expected = "c2"
     msg = "The results obtained with dls-list (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
  # Test addition with non-existing parent directories using CLI args 
  def testAdditionWithParent(self):
     cmd = self.path + "/dls-add dir1/dir2/c1 CliTest-se1 CliTest-se2"
     st, out = run(cmd)
     msg = "Error in dls-add dir1/dir2/c1 CliTest-se1 CliTest-se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se dir1/dir2/c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se dir1/dir2/c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest-se1\nCliTest-se2"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
  # Test erroneous addition for FileBlock and locations using CLI args 
  def testErroneousAddition(self):
   
     # Erroneous root directory (not permitted)
     cmd = self.path + "/dls-add -e %s/ c1 CliTest-se4 CliTest-se5" % (self.host)
     st, out = run(cmd)
     msg = "Error in dls-add /c1 CliTest-se4 CliTest-se5",out 
     self.assertEqual(st, 0, msg)
     expected= "Warning: Error creating the FileBlock c1: Permission denied"
     msg = "Output obtained with dls-add /c1 (%s) is not that expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)

     # Erroneous location (wrong hostname)
     cmd = self.path + "/dls-add /c1 CliTest_#@_WRONG"
     st, out = run(cmd)
     msg = "Unexpected success in %s: %s" % (cmd, out) 
     wasError = (st!=0)
     self.assert_(wasError, msg)
     expected = "Wrong specified host (CliTest_#@_WRONG) of DlsLocation object:"
     contains = out.find(expected) != -1
     msg = "Output obtained with %s (%s) is not that expected (%s)" % (cmd, out, expected)
     self.assert_(contains, msg)
     cmd = self.path + "/dls-list /"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (msg,out)
     self.assertEqual(st, 0, msg)
     expected = ""
     msg = "The results obtained with %s (%s) are not those expected (%s)"%(cmd,out,expected)
     self.assertEqual(out, expected, msg)
     
     # Erroneous location (existing)
     cmd = self.path + "/dls-add /c1 CliTest-se4 CliTest-se5"
     st, out = run(cmd)
     msg = "Error in dls-add /c1 CliTest-se4 CliTest-se5",out 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add /c1 CliTest-se4 CliTest-se6"
     st, out = run(cmd)
     msg = "Error in dls-add /c1 CliTest-se4 CliTest-se6",out 
     self.assertEqual(st, 0, msg)
     expected = "Warning: Error adding location CliTest-se4 for FileBlock"
     contains = out.find(expected) != -1
     msg = "Output obtained with dls-add /c1 (%s) is not that expected (%s)" % (out, expected)
     self.assert_(contains, msg)
     cmd = self.path + "/dls-get-se /c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se /c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest-se4\nCliTest-se5\nCliTest-se6"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     

  # Test addition and get-se, for attributes using command line arguments
  def testAttrsAdditionListGetSE(self):
     # DLS code for direct api calls
     import dlsLfcApi
     import dlsDataObjects as obj

     # Generate GUID 
     cmd = "uuidgen"
     st, guid = run(cmd)
     msg = "Error generating the GUID",guid
     self.assertEqual(st, 0, msg)
     
     # Define some vars
     fattrs="filesize=400 guid=%s filemode=0711" % (guid)
     surl = "sfn://my_sfn/%s" % (guid)
     rattrs="sfn=%s f_type=P ptime=45" % (surl)
     # Addition with attributes 
     cmd = self.path + "/dls-add c2 "+fattrs+" CliTest-se1 "+rattrs
     st, out = run(cmd)
     msg = "Error in dls-add c2 "+fattrs+" CliTest-se1 "+rattrs,out 
     self.assertEqual(st, 0, msg)

     # Test the fileblock attributes listing
     cmd = self.path + "/dls-list -l c2"
     st, out = run(cmd)
     msg = "Error in dls-list -l c2",out 
     self.assertEqual(st, 0, msg)
     contains = (out.find("rwx--x--x") != -1) and (out.find("400") != -1)
     msg = "The listing of attributes is not as expected",out 
     self.assert_(contains, msg)
     
     cmd = self.path + "/dls-list -l /"
     st, out = run(cmd)
     msg = "Error in dls-list -l /",out 
     self.assertEqual(st, 0, msg)
     contains = (out.find("c2")!=-1) and (out.find("rwx--x--x")!=-1) and (out.find("400")!=-1) 
     msg = "The listing of attributes is not as expected",out 
     self.assert_(contains, msg)
     
     # Test the GUID listing
     cmd = self.path + "/dls-list -g c2"
     st, out = run(cmd)
     msg = "Error in dls-list -g c2",out 
     self.assertEqual(st, 0, msg)
     out_tokens = out.split()
     retrievedGuid = out_tokens[1]
     msg = "The normal guid listing was not correct: %s (guid: %s)" % (out, guid)
     self.assertEqual(retrievedGuid, guid, msg)
     
     cmd = self.path + "/dls-list -lg c2"
     st, out = run(cmd)
     msg = "Error in dls-list -lg c2",out 
     self.assertEqual(st, 0, msg)
     out_tokens = out.split()
     retrievedGuid = out_tokens[9]
     msg = "The long guid listing was not correct: %s (guid: %s)" % (out, guid)
     self.assertEqual(retrievedGuid, guid, msg)
     
     # Test the replica attributes
     the_date=strftime("%b %d %H:%M")
     cmd = self.path + "/dls-get-se -l c2"
     st, out = run(cmd)
     expected = "CliTest-se1 \t"+the_date+" \t45 \tP \tsfn://my_sfn/"+guid
     msg = "Results from dls-get-se -l c2 (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
    
     # Test the guid retrieval
     iface = dlsLfcApi.DlsLfcApi(self.host + self.testdir)
     try:
        retrievedGuid = iface.getGUID("c2")
     except dlsLfcApi.DlsLfcApiError, inst:
        msg = "Error in iface.getGUID(\"c2\"): %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)
     msg = "The guid retrieval was not correct"
     self.assertEqual(retrievedGuid, guid, msg)
     
     # Same with FileBlock as argument
     fB = obj.DlsFileBlock("c2")
     try:
        retrievedGuid = (iface.getGUID(fB)).getGuid()
     except dlsLfcApi.DlsLfcApiError, inst:
        msg = "Error in iface.getGUID(\"fB\"): %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)
     msg = "The guid listing was not correct: %s and %s" % (retrievedGuid, guid)
     self.assertEqual(retrievedGuid, guid, msg)

     # Test the SURL retrieval
     fB = obj.DlsFileBlock("c2")
     loc = obj.DlsLocation("CliTest-se1")
     entry = obj.DlsEntry(fB, [loc])
     try:        
        entry = iface.getSURL(entry)
        retrievedSurl = (entry.getLocation("CliTest-se1")).getSurl()
     except dlsLfcApi.DlsLfcApiError, inst:
        msg = "Error in iface.getSURL(entry): %s" % (inst.msg) 
        self.assertEqual(-1, 0, msg)
     msg = "The surl retrieval was not correct"
     self.assertEqual(retrievedSurl, surl, msg)
    

  # Test recursive listing using CLI args 
  def testRecursiveListing(self):
   
     # First create a dir hierarchy
     cmd = self.path + "/dls-add f1 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add dir1/f2 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add dir1/dir2/f3 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add dir1/dir3/fXX CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
       # Remove the fXX file (leaving an empty dir1/dir3)
     cmd = self.path + "/dls-delete -a dir1/dir3/fXX"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     # Now list recursively, for several cases...
       #... Just one file (just like normal list)
     cmd = self.path + "/dls-list -r f1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     expected = "f1"
     msg = "The results obtained with %s (%s) are not those expected (%s)"%(cmd, out, expected)
     self.assertEqual(out, expected, msg)
       #... empty dir (should return nothing)
     cmd = self.path + "/dls-list -r dir1/dir3"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     expected = ""
     msg = "The results obtained with %s (%s) are not those expected (%s)"%(cmd, out, expected)
     self.assertEqual(out, expected, msg)
       #... root dir (should return whole hierarchy)
     cmd = self.path + "/dls-list -r /"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     expected = "f1\ndir1/f2\ndir1/dir2/f3\ndir1/dir3/"
     msg = "The results obtained with %s (%s) are not those expected (%s)"%(cmd, out, expected)
     self.assertEqual(out, expected, msg)


###########################################
# Class for DLS CLI testing: replace-location
###########################################

class TestDlsCli_FromArgs_ReplaceLoc(TestDlsCli):
  def setUp(self):
     # Invoke parent
     TestDlsCli.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli.tearDown(self)

  # Test replace-location works using command line arguments
  def testReplaceLoc(self):
     # First add
     cmd = self.path + "/dls-add f1 CliTest-se1 CliTest-se2"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add f2 CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)

    # Now replace one location
     cmd = self.path + "/dls-replace-location -o CliTest-se1 -n CliTest-NEW"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)

    # Now check what happened
     cmd = self.path + "/dls-get-fileblock CliTest-NEW"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     contains = out.find("f1") != -1
     contains *= out.find("f2") != -1
     msg = "The results obtained with dls-get-fileblock are not those expected",out
     self.assert_(contains, msg)
     cmd = self.path + "/dls-get-fileblock CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     msg = "The results obtained with dls-get-fileblock are not those expected",out
     self.assertEqual(out, '', msg)


  # Test replace-location roll backs correctly on failure
  def testReplaceLocError(self):
     # First add
     cmd = self.path + "/dls-add f1 CliTest-se1 CliTest-se2"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add f2 CliTest-se1 f_type=P"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)

     # Now try to replace (and fail)
     cmd = self.path + "/dls-replace-location -o CliTest-se1 -n CliTest-NEW"
     st, out = run(cmd)
     msg = "Unexpected success in %s: %s" % (cmd, out) 
     wasError = (st!=0)
     self.assert_(wasError, msg)

    # Now check what happened
     cmd = self.path + "/dls-get-fileblock CliTest-NEW"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     msg = "The results obtained with dls-get-fileblock are not those expected",out
     self.assertEqual(out, '', msg)
     cmd = self.path + "/dls-get-fileblock CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     contains = out.find("f1") != -1
     contains *= out.find("f2") != -1
     msg = "The results obtained with dls-get-fileblock are not those expected",out
     self.assert_(contains, msg)


##############################################################################
# Parent class for DLS CLI testing using arguments from file (-f option)
##############################################################################

class TestDlsCli_FromFile(TestDlsCli):
  def setUp(self):
     # Invoke parent
     TestDlsCli.setUp(self)

     # Create temp directory for all the files to live in
     cmd = "mktemp -d"
     st, out = run(cmd)
     msg = "Error when creating temporary directory",out 
     self.assertEqual(st, 0, msg)
     self.tempdir = out
     self.orgdir = getcwd()
     chdir(self.tempdir)

     # Create files for the commands to read from
     f_10_LFNs = open('10_LFNs', 'w')
     f_8_LFNs = open('8_LFNs', 'w')
     f_10_LFNs_with_SURLs = open('10_LFNs_with_SURLs', 'w')
     f_2_SEs = open('2_SEs', 'w')
     f_2_LFNs = open('2_LFNs', 'w')
     f_2_LFNs_with_SURLs = open('2_LFNs_with_SURLs', 'w')
     f_2_LFNs_with_SURLS_attrs = open('f_2_LFNs_with_SURLS_attrs', 'w')
     f_rename = open('f_rename', 'w')

     f_2_SEs.write("CliTest-se1\nCliTest-se3\n")
     for i in xrange(10):       
        f_10_LFNs.write("f%d\n" %i)
        if ((i != 2) and (i!= 6)): 
           f_8_LFNs.write("f%d\n" %i)
        if(i < 5):
            f_10_LFNs_with_SURLs.write("f%d CliTest-se1 CliTest-se2\n" % i)
        else:
            f_10_LFNs_with_SURLs.write("f%d CliTest-se3\n" %i )
     
     f_2_LFNs.write("f2\nf6\n")
     f_2_LFNs_with_SURLs.write("f2 CliTest-se1 CliTest-se2\n"+"f6 CliTest-se3\n")
     f_2_LFNs_with_SURLS_attrs.write("c1 filesize=777 adsf=324 CliTest-se1 ptime=444 jjj=999\n")
     f_2_LFNs_with_SURLS_attrs.write("f99 CliTest-se2 ptime=444 a=0\n")
     f_rename.write("f1 f1_NEW\nfXXX fXXX_NEW\nf2 f2_NEW\n")
     
     f_10_LFNs.close()
     f_8_LFNs.close()
     f_10_LFNs_with_SURLs.close()
     f_2_SEs.close()
     f_2_LFNs.close()
     f_2_LFNs_with_SURLs.close()
     f_2_LFNs_with_SURLS_attrs.close()
     f_rename.close()


  def tearDown(self):
     # Invoke parent
     TestDlsCli.tearDown(self)

     # Remove the temp directory
     chdir(self.orgdir)
     cmd = "rm -r "+self.tempdir
     st, out = run(cmd)

##############################################################################
# Classes for DLS CLI testing using arguments from file (-f option)
##############################################################################


##########################################################
# Class for DLS CLI testing: Addition, getSE, list, Rename
##########################################################

class TestDlsCli_FromFile_AddGetSEList(TestDlsCli_FromFile):
  def setUp(self):
     # Invoke parent
     TestDlsCli_FromFile.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli_FromFile.tearDown(self)

  # Test addition using list files
  def testAddition(self):
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se f1"
     st, buffer = run(cmd)
     msg = "Error in dls-get-se f1",buffer
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-get-se f6"
     st, out = run(cmd)
     msg = "Error in dls-get-se f6",out 
     self.assertEqual(st, 0, msg)

     out = buffer + "\n" + out
     msg = "The results obtained with dls-add -f are not those expected",out
     self.assertEqual(out, "CliTest-se1\nCliTest-se2\nCliTest-se3", msg)


  # Test dls-list using list files
  def testList(self):
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-list -f 2_LFNs"
     st, buffer = run(cmd)
     msg = "Error in dls-list f1",buffer
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-list f1"
     st, out = run(cmd)
     msg = "Error in dls-list f6",out 
     self.assertEqual(st, 0, msg)
     out = buffer + "\n" + out

     expected = "f2\nf6\nf1"
     msg = "The results obtained with dls-list -f (%s) are not those expected (%s)" %(out, expected)
     self.assertEqual(out, expected, msg)

  # Test get-se using list files
  def testGetSE(self):
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se -f 2_LFNs"
     st, buffer = run(cmd)
     msg = "Error in dls-get-se -f 2_LFNs",buffer
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-get-se f1"
     st, out = run(cmd)
     msg = "Error in dls-get-se f6",out 
     self.assertEqual(st, 0, msg)

     out = buffer + "\n" + out
     expected="  FileBlock: f2\nCliTest-se1\nCliTest-se2\n"
     expected += "  FileBlock: f6\nCliTest-se3\n"
     expected += "CliTest-se1\nCliTest-se2"
     msg = "Results obtained with dls-get-se -f (%s) are not those expected (%s)"%(out, expected)
     self.assertEqual(out, expected, msg)


  # Test dls-rename using list files
  def testRename(self):
     cmd = self.path + "/dls-add f1 se1"
     st, out = run(cmd)
     msg = "Error in dls-add f1 se1",out 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add f2 se2"
     st, out = run(cmd)
     msg = "Error in dls-add f2 se2",out 
     self.assertEqual(st, 0, msg)

     # First try in transaction mode
     cmd = self.path + "/dls-rename -t -f f_rename"
     st, out = run(cmd)

     # Check nothing was changed
     cmd = self.path + "/dls-list /"
     st, out = run(cmd)
     msg = "Error in dls-list /",out
     self.assertEqual(st, 0, msg)
     expected = "f1\nf2"
     msg = "The results obtained with dls-list / (%s) are not those expected (%s)" %(out, expected)
     self.assertEqual(out, expected, msg)

     # Now try without transactions
     cmd = self.path + "/dls-rename -f f_rename"
     st, out = run(cmd)
     msg = "Error in dls-rename -f f_rename",out
     self.assertEqual(st, 0, msg)

     # Check the renamings were correctly made
     cmd = self.path + "/dls-get-se f1_NEW"
     st, out = run(cmd)
     msg = "Error in dls-get-se f1_NEW",out
     self.assertEqual(st, 0, msg)
     expected = "se1"
     msg = "Results for dls-get-se f1_NEW (%s) are not those expected (%s)" %(out, expected)
     self.assertEqual(out, expected, msg)
     cmd = self.path + "/dls-get-se f2_NEW"
     st, out = run(cmd)
     msg = "Error in dls-get-se f2_NEW",out
     self.assertEqual(st, 0, msg)
     expected = "se2"
     msg = "Results for dls-get-se f2_NEW (%s) are not those expected (%s)" %(out, expected)
     self.assertEqual(out, expected, msg)

     # Check also that the old files are not there anymore
     cmd = self.path + "/dls-list /"
     st, out = run(cmd)
     msg = "Error in dls-list /",out
     self.assertEqual(st, 0, msg)
     expected = "f1_NEW\nf2_NEW"
     msg = "The results obtained with dls-list / (%s) are not those expected (%s)" %(out, expected)
     self.assertEqual(out, expected, msg)



###########################################
# Class for DLS CLI testing: get-fileblocks
###########################################

class TestDlsCli_FromFile_GetBlock(TestDlsCli_FromFile):
  def setUp(self):
     # Invoke parent
     TestDlsCli_FromFile.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli_FromFile.tearDown(self)

  # Test get-fileblock using command line arguments
  def testGetBlockArgs(self):
     # First add -f
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

    # Now retrieve them
     cmd = self.path + "/dls-get-fileblock CliTest-se1"
     st, out = run(cmd)
     msg = "Error in /dls-get-fileblock CliTest-se1",out
     self.assertEqual(st, 0, msg)

     for i in xrange(5):
       contains = out.find("f%d" % i) != -1
       msg = "The results obtained with dls-get-dabablock are not those expected",out
       self.assert_(contains, msg)


  # Test get-fileblock using list files
  def testGetBlock(self):
     # First add -f
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     # Now retrieve them
     cmd = self.path + "/dls-get-fileblock -f 2_SEs"
     st, out = run(cmd)
     msg = "Error in /dls-get-fileblock -f 2_SEs",out 
     self.assertEqual(st, 0, msg)

     outlist = out.split("\n")
     firsthalf = ""
     secondhalf = ""
     for i in xrange(6):
        firsthalf += outlist[i] + "\n"
        
     for i in xrange(6,12):
        secondhalf += outlist[i] + "\n"
    
     for i in xrange(5):
       contains = firsthalf.find("f%d" % i) != -1
       msg = "The results obtained (%s) with dls-get-fileblock -f are not those \
              expected (f%d)"  % (firsthalf, i)
       self.assert_(contains, msg)
           
     for i in xrange(5,10):
       contains = secondhalf.find("f%d" % i) != -1
       msg = "The results obtained (%s) with dls-get-fileblock -f are not those \
              expected (f%d)"  % (secondhalf, i)
       self.assert_(contains, msg)


################################################
# Class for DLS CLI testing: Deletion and update
################################################

class TestDlsCli_FromFile_DelUpdate(TestDlsCli_FromFile):
  def setUp(self):
     # Invoke parent
     TestDlsCli_FromFile.setUp(self)
     
  def tearDown(self):
     # Invoke parent
     TestDlsCli_FromFile.tearDown(self)


  # Test deletion using listing file
  def testDeletion(self):
     # First add (already tested) 
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     # Delete some replicas (but keep FileBlock)
     cmd = self.path + "/dls-delete -k -f 2_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-delete -k -f 2_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     expected = "  FileBlock: "+"f2\n"
     expected += "  FileBlock: "+"f6"

     cmd = self.path + "/dls-get-se -f 2_LFNs"
     st, out = run(cmd)
     msg = "Error in dls-get-se -f 2_LFNs",out 
     self.assertEqual(st, 0, msg)

     msg = "The results obtained with dls-get-se -f (%s) are not those \
            expected (%s)" % (out,expected)
     self.assertEqual(out, expected, msg)
     
     # Now delete also FileBlock
     cmd = self.path + "/dls-delete -f 2_LFNs"
     st, out = run(cmd)
     msg = "Error in dls-delete -f 2_LFNs",out 
     self.assertEqual(st, 0, msg)

     expected = "Error in the DLS query: Error querying for f2: Error accessing DLI " 
     expected += "%s for %s/f2 of type lfn. SOAP-ENV:Client, InputData." %(self.host, self.testdir)

     cmd = self.path + "/dls-get-se -f 2_LFNs"
     st, out = run(cmd)

     msg = "The results obtained with dls-get-se -f (%s) are not those \
            expected (%s)" % (out,expected)
     self.assertEqual(out, expected, msg)

     # Delete the entry
     cmd = self.path + "/dls-delete -f 8_LFNs"
     st, out = run(cmd)
     msg = "Error in dls-delete -f 8_LFNs",out 
     self.assertEqual(st, 0, msg)



  # Test transactions on update (from file) 
  def testUpdateTrans(self):
    
     cmd = self.path + "/dls-add c1 CliTest-se1 ptime=333"
     st, out = run(cmd)
     msg = "Error in /dls-add c1 CliTest-se1 ptime=333",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-update -v 2 -t -f f_2_LFNs_with_SURLS_attrs"
     st, out = run(cmd)
     expected = "Transaction operations rolled back"
     contains = out.find(expected) != -1
     msg = "The results obtained (%s) with dls-update -t -f are not those expected (%s)"\
         % (out, expected)
     self.assert_(contains, msg)

     cmd = self.path + "/dls-get-se -l c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se -l c1",out 
     self.assertEqual(st, 0, msg)
     not_expected = "444"
     not_contains = out.find(not_expected) == -1
     msg = "The results obtained (%s) with dls-get-se -l should not contain: %s"%(out, not_expected)
     self.assert_(not_contains, msg)
     
     cmd = self.path + "/dls-list -l c1"
     st, out = run(cmd)
     msg = "Error in dls-list -l c1",out 
     self.assertEqual(st, 0, msg)
     not_expected = "777"
     not_contains = out.find(not_expected) == -1
     msg = "The results obtained (%s) with dls-list -l should not contain: %s)"%(out, not_expected)
     self.assert_(not_contains, msg)

     cmd = self.path + "/dls-update -v 2 -f f_2_LFNs_with_SURLS_attrs"
     st, out = run(cmd)
     msg = "Error in /dls-update -v 2 -f f_2_LFNs_with_SURLS_attrs",out 
     self.assertEqual(st, 0, msg)
     expected = "--Ending session"
     contains = out.find(expected) != -1
     msg = "The results obtained (%s) with dls-update -f (without -t) are not those expected  (%s)"\
         % (out, expected)
     self.assert_(contains, msg)

     cmd = self.path + "/dls-get-se -l c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "444"
     msg = "The results obtained (%s) with dls-get-se -l are not those expected (%s)"\
           % (out, expected)
     contains = out.find(expected) != -1
     self.assert_(contains, msg)

     cmd = self.path + "/dls-list -l c1"
     st, out = run(cmd)
     msg = "Error in dls-list -l c1",out 
     self.assertEqual(st, 0, msg)
     contains = out.find("777") != -1
     msg = "The results obtained (%s) with dls-list -l are not those expected (%s)"\
         % (out, expected)
     self.assert_(contains, msg)     




##############################################################################
# Module's methods to return the suites
##############################################################################

def getSuite():
  suite = []
  suite.append(getSuiteFromArgs_General())
  suite.append(getSuiteFromArgs_AddGetSEList())
  suite.append(getSuiteFromArgs_Deletion())
  suite.append(getSuiteFromArgs_ReplaceLoc())
  suite.append(getSuiteFromFile_AddGetSEList())
  suite.append(getSuiteFromFile_GetBlock())
  suite.append(getSuiteFromFile_DelUpdate())
  return unittest.TestSuite(suite)
  
def getSuiteFromArgs_General():
  return unittest.makeSuite(TestDlsCli_FromArgs_General)
   
def getSuiteFromArgs_AddGetSEList():
  return unittest.makeSuite(TestDlsCli_FromArgs_AddGetSEList)
   
def getSuiteFromArgs_Deletion():
  return unittest.makeSuite(TestDlsCli_FromArgs_Deletion)
  
def getSuiteFromArgs_ReplaceLoc():
  return unittest.makeSuite(TestDlsCli_FromArgs_ReplaceLoc)
   
def getSuiteFromFile_AddGetSEList():
  return unittest.makeSuite(TestDlsCli_FromFile_AddGetSEList)

def getSuiteFromFile_GetBlock():
  return unittest.makeSuite(TestDlsCli_FromFile_GetBlock)

def getSuiteFromFile_DelUpdate():
  return unittest.makeSuite(TestDlsCli_FromFile_DelUpdate)

# Module's methods to set the conf file
def setConf(filename):
  global conf_file
  conf_file = filename

 
##############################################################################
# Main method
##############################################################################

def help():
  print "This script runs tests on the LFC-based DLS CLI."
  print "It requires a configuration file with some variables (refer to the example)"
  print
  print "An optional additional argument may be used to execute only a subset"
  print "of the tests."
  print "   \"args_general\" ==> Tests on general options and setup, using command line args"
  print "   \"args_add\"     ==> Tests on add, get-se, list, using command line args"
  print "   \"args_del\"     ==> Tests on del, using command line args"
  print "   \"args_replace\" ==> Tests on replace-location, using command line args"
  print "   \"file_add\"     ==> Tests on add, get-se, list, rename, using args from a file (-f)"
  print "   \"file_getfb\"   ==> Tests on get-fileblock, using args from a file (-f)"
  print "   \"file_del\"     ==> Tests on del, update, using args from a file (-f)"
  
def usage():
  print "Usage:  DlsLfcCliTest.py <conf_file> [<subset_of_tests>]"

def main():
  if(len(sys.argv) < 2):
     msg = "Not enought input arguments!\n\n"
     msg += "You need to specify the configuration file as argument.\n"
     msg += "Probably you can use DlsLfcCliTest.conf, but please check it out first.\n"
     sys.stderr.write(msg+"\n")
     help()
     print
     usage()
     sys.exit(-1)

  setConf(sys.argv[1])
 
  suite = None
  if(len(sys.argv) > 2):
     if(sys.argv[2] == "args_general"):
        suite = getSuiteFromArgs_General()
     if(sys.argv[2] == "args_add"):
        suite = getSuiteFromArgs_AddGetSEList()
     if(sys.argv[2] == "args_del"):
        suite = getSuiteFromArgs_Deletion()
     if(sys.argv[2] == "args_replace"):
        suite = getSuiteFromArgs_ReplaceLoc()
     if(sys.argv[2] == "file_add"):
        suite = getSuiteFromFile_AddGetSEList()
     if(sys.argv[2] == "file_getfb"):
        suite = getSuiteFromFile_GetBlock()
     if(sys.argv[2] == "file_del"):
        suite = getSuiteFromFile_DelUpdate()

     if(not suite):
           msg = "Error: The optional second argument is not one of the supported ones!\n"
           sys.stderr.write("\n"+msg)
           help()
           print
           usage()
           return -1
  else:
     suite = getSuite()

  unittest.TextTestRunner(verbosity=4).run(suite)


# Make this runnable
if __name__ == '__main__':
  main()


