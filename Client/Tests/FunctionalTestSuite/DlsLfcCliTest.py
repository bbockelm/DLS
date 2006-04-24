#!/usr/bin/python
 
import unittest

import anto_utils

from commands import getstatusoutput
from os import putenv, unsetenv, chdir, getcwd, environ
from time import strftime
import sys 

run = getstatusoutput

# Need a global variable here
conf_file = ""


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

      # Create the directory where to work
      putenv("LFC_HOST", self.host)
#      putenv("LFC_HOME", self.testdir)
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
# Class for DLS CLI testing using CLI arguments
##############################################################################

class TestDlsCli_FromArgs(TestDlsCli):
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
     expected = "Unsupported interface type: SOMETHING"
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
     # The test with DLS_TYPE_MYSQL waits till we know what we should expect
#     cmd = self.path + "/dls-add -i DLS_TYPE_MYSQL c1"
#     st, out = run(cmd)
#     expected = ""
#     msg = "Results (%s) are not those expected (%s)" % (out, expected)
#     self.assertEqual(out, expected, msg)
   
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

     cmd = self.path + "/dls-get-fileblock -e %s%s CliTest_NOTHING" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-get-fileblock -e %s%s CliTest_NOTHING: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-delete -e %s%s -a c1" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-delete -e %s%s -a c1: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)
    

  # Test addition and get-se using command line arguments
  def testAdditionGetSE(self):
     cmd = self.path + "/dls-add c1 CliTest_se4 CliTest_se5 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest_se4 CliTest_se5 CliTest_se6",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest_se4\nCliTest_se5\nCliTest_se6"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
  # Test addition with non-existing parent directories using CLI args 
  def testAdditionWithParent(self):
     cmd = self.path + "/dls-add dir1/dir2/c1 CliTest_se1 CliTest_se2"
     st, out = run(cmd)
     msg = "Error in dls-add dir1/dir2/c1 CliTest_se1 CliTest_se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se dir1/dir2/c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se dir1/dir2/c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest_se1\nCliTest_se2"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
  # Test erroneous addition for FileBlock and locations using CLI args 
  def testErroneousAddition(self):
   
     # Erroneous root directory (not permitted)
     cmd = self.path + "/dls-add -e %s/ c1 CliTest_se4 CliTest_se5" % (self.host)
     st, out = run(cmd)
     msg = "Error in dls-add /c1 CliTest_se4 CliTest_se5",out 
     self.assertEqual(st, 0, msg)
     expected= "Warning: Error creating the FileBlock c1: Permission denied"
     msg = "Output obtained with dls-add /c1 (%s) is not that expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)

     # Erroneous location (existing)
     cmd = self.path + "/dls-add /c1 CliTest_se4 CliTest_se5"
     st, out = run(cmd)
     msg = "Error in dls-add /c1 CliTest_se4 CliTest_se5",out 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add /c1 CliTest_se4 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add /c1 CliTest_se4 CliTest_se6",out 
     self.assertEqual(st, 0, msg)
     expected = "Warning: Error adding location CliTest_se4 for FileBlock"
     contains = out.find(expected) != -1
     msg = "Output obtained with dls-add /c1 (%s) is not that expected (%s)" % (out, expected)
     self.assert_(contains, msg)
     cmd = self.path + "/dls-get-se /c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se /c1",out 
     self.assertEqual(st, 0, msg)
     expected = "CliTest_se4\nCliTest_se5\nCliTest_se6"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
     
    
  # Test deletion using command line arguments
  def testDeletion(self):
     # First add (already tested) 
     cmd = self.path + "/dls-add c1 CliTest_se4 CliTest_se5 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest_se4 CliTest_se5 CliTest_se6",out 
     self.assertEqual(st, 0, msg)

     # Delete some replicas
     cmd = self.path + "/dls-delete c1 CliTest_se4 CliTest_se5"
     st, out = run(cmd)
     msg = "Error in dls-delete c1 CliTest_se4 CliTest_se5",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     msg = "The results obtained with dls-get-se are not those expected",out
     self.assertEqual(out, "CliTest_se6", msg)

     # Delete the entry
     cmd = self.path + "/dls-delete -a c1"
     st, out = run(cmd)
     msg = "Error in dls-delete -a c1",out 
     self.assertEqual(st, 0, msg)


  # Test deletion of a directory using CL args
  def testDeletionDir(self):
     # Create a non-empty dir
     cmd = self.path + "/dls-add dir1/f1 CliTest_se1"
     st, out = run(cmd)
     msg = "Error in dls-add dir1/f1 CliTest_se1",out 
     self.assertEqual(st, 0, msg)

     # Test wrong arguments
     cmd = self.path + "/dls-delete dir1 CliTest_se_XXX"
     st, out = run(cmd)
     msg = "Error in dls-delete dir1 CliTest_se_XXX",out 
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



  # Test addition and get-se, for attributes using command line arguments
  def testAttrsAdditionGetSE(self):

     # TODO: This should not be necessary when dls-list is there
     putenv("LFC_HOST", self.host)
     putenv("LFC_HOME", self.testdir)

     # Generate GUID 
     cmd = "uuidgen"
     st, guid = run(cmd)
     msg = "Error generating the GUID",guid
     self.assertEqual(st, 0, msg)
     
     # Define some vars
     fattrs="filesize=400 guid=%s mode=0711" % (guid)
     rattrs="sfn=sfn://my_sfn/%s f_type=P ptime=45" % (guid)

     # Addition with attributes 
     cmd = self.path + "/dls-add c2 "+fattrs+" CliTest_se1 "+rattrs
     st, out = run(cmd)
     msg = "Error in dls-add c2 "+fattrs+" CliTest_se1 "+rattrs,out 
     self.assertEqual(st, 0, msg)

     # Test the fileblock attributes
     cmd = "lfc-ls -l c2"
     st, out = run(cmd)
     msg = "Error in lfc-ls -l c2",out 
     self.assertEqual(st, 0, msg)
     contains = (out.find("rwx--x--x") != -1) and (out.find("400") != -1)
     msg = "The listing of attributes is not as expected",out 
     self.assert_(contains, msg)
     
     # Test the guid retrieval
     cmd = "lcg-lg --vo dteam lfn:c2"
     st, out = run(cmd)
     msg = "Error in lcg-lg --vo dteam lfn:c2",out 
     self.assertEqual(st, 0, msg)
     contains = out.find(guid) != -1
     msg = "The guid retrieval was not correct",out 
     self.assert_(contains, msg)

     # Test the replica attributes
     the_date=strftime("%b %d %H:%M")
     cmd = self.path + "/dls-get-se -l c2"
     st, out = run(cmd)
     expected = "CliTest_se1 \t"+the_date+" \t45 \tP \tsfn://my_sfn/"+guid
     msg = "Results from dls-get-se -l c2 (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
  # Test verbosity (on update after addition)
  def testVerbosity(self):
     cmd = self.path + "/dls-add c1 CliTest_se1 CliTest_se2"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest_se1 CliTest_se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-update -v 0 c1 CliTest_se5"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = ""
     msg = "The results obtained (%s) with dls-update -v are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-update -v 1 c1 CliTest_se5"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     expected = "Warning: Not all locations could be found and updated"
     msg = "The results obtained (%s) with dls-update -v are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-update -v 2 c1 CliTest_se5"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     for i in ["--DlsApi.update", "--Starting session", "--updateFileBlock",\
               "--lfc.lfc_getreplica", "Warning: Not all locations", "--Ending session"]:
       expected = i
       msg = "The results obtained (%s) with dls-update -v are not those expected  (%s)"\
           % (out, expected)
       contains = out.find(expected) != -1
       self.assert_(contains, msg)


  # Test sessions (on dls-get-se for -l only) 
  def testSession(self):
     cmd = self.path + "/dls-add c1 CliTest_se4 CliTest_se5 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest_se4 CliTest_se5 CliTest_se6",out 
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
     cmd = self.path + "/dls-add c1 CliTest_se1 CliTest_se2"
     st, out = run(cmd)
     msg = "Error in dls-add c1 CliTest_se1 CliTest_se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-add -v 2 -t c1 CliTest_se3 CliTest_se1"
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
     expected = "CliTest_se1\nCliTest_se2"
     msg = "The results obtained (%s) with dls-get-se are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)

     cmd = self.path + "/dls-add -v 2 c1 CliTest_se3 CliTest_se1"
     st, out = run(cmd)
     msg = "Error in dls-add -v 2 c1 CliTest_se3 CliTest_se1",out 
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
     expected = "CliTest_se1\nCliTest_se2\nCliTest_se3"
     msg = "The results obtained (%s) with dls-get-se are not those expected  (%s)"\
           % (out, expected)
     self.assertEqual(out, expected, msg)



##############################################################################
# Class for DLS CLI testing using arguments from file (-f option)
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

     f_2_SEs.write("CliTest_se1\nCliTest_se3\n")
     for i in xrange(10):       
        f_10_LFNs.write("f%d\n" %i)
        if ((i != 2) and (i!= 6)): 
           f_8_LFNs.write("f%d\n" %i)
        if(i < 5):
            f_10_LFNs_with_SURLs.write("f%d CliTest_se1 CliTest_se2\n" % i)
        else:
            f_10_LFNs_with_SURLs.write("f%d CliTest_se3\n" %i )
     
     f_2_LFNs.write("f2\nf6\n")
     f_2_LFNs_with_SURLs.write("f2 CliTest_se1 CliTest_se2\n"+"f6 CliTest_se3\n")
     f_2_LFNs_with_SURLS_attrs.write("c1 filesize=777 adsf=324 CliTest_se1 ptime=444 jjj=999\n")
     f_2_LFNs_with_SURLS_attrs.write("f99 CliTest_se2 ptime=444 a=0\n")
     
     f_10_LFNs.close()
     f_8_LFNs.close()
     f_10_LFNs_with_SURLs.close()
     f_2_SEs.close()
     f_2_LFNs.close()
     f_2_LFNs_with_SURLs.close()
     f_2_LFNs_with_SURLS_attrs.close()


  def tearDown(self):
     # Invoke parent
     TestDlsCli.tearDown(self)

     # Remove the temp directory
     chdir(self.orgdir)
     cmd = "rm -r "+self.tempdir
     st, out = run(cmd)


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
     self.assertEqual(out, "CliTest_se1\nCliTest_se2\nCliTest_se3", msg)


  # Test get-se using list files
  def testGetSE(self):
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
     msg = "The results obtained with dls-get-se -f are not those expected",out
     self.assertEqual(out, "CliTest_se1\nCliTest_se2\nCliTest_se3", msg)


  # Test get-fileblock using command line arguments
  def testGetBlockArgs(self):
     # First add -f
     cmd = self.path + "/dls-add -f 10_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add -f 10_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

    # Now retrieve them
     cmd = self.path + "/dls-get-fileblock CliTest_se1"
     st, out = run(cmd)
     msg = "Error in /dls-get-fileblock CliTest_se1",out
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
     msg = "Error in dls-get-se -f 2_LFNs",out 
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
    
     # TODO: This should not be necessary when dls-list is there
     putenv("LFC_HOST", self.host)
     putenv("LFC_HOME", self.testdir)

     cmd = self.path + "/dls-add c1 CliTest_se1 ptime=333"
     st, out = run(cmd)
     msg = "Error in /dls-add c1 CliTest_se1 ptime=333",out 
     self.assertEqual(st, 0, msg)

#     f_2_LFNs_with_SURLS_attrs.write("c1 filesize=777 adsf=324 CliTest_se1   ptime=444 jjj=999\n")
#     f_2_LFNs_with_SURLS_attrs.write("f99 CliTest_se2 ptime=555 a=0\n")

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
     msg = "The results obtained (%s) with dls-get-se -l should not contain: %s"\
           % (out, not_expected)
     self.assert_(not_contains, msg)
     
     cmd = "lfc-ls -l c1"
     st, out = run(cmd)
     msg = "Error in lfc-ls -l c1",out 
     self.assertEqual(st, 0, msg)
     not_expected = "777"
     not_contains = out.find(not_expected) == -1
     msg = "The results obtained (%s) with lfc-ls -l should not contain: %s)"\
         % (out, not_expected)
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

     cmd = "lfc-ls -l c1"
     st, out = run(cmd)
     msg = "Error in lfc-ls -l c1",out 
     self.assertEqual(st, 0, msg)
     contains = out.find("777") != -1
     msg = "The results obtained (%s) with lfc-ls -l are not those expected (%s)"\
         % (out, expected)
     self.assert_(contains, msg)     



##############################################################################
# Module's methods to return the suites
##############################################################################

def getSuite():
  suite_args = getSuiteFromArgs()
  suite_file = getSuiteFromFile()
  return unittest.TestSuite((suite_args, suite_file))
  
def getSuiteFromArgs():
  return unittest.makeSuite(TestDlsCli_FromArgs)
   
def getSuiteFromFile():
  return unittest.makeSuite(TestDlsCli_FromFile)

def getSingleTestArgs(testname):
  suite = unittest.TestSuite()
  suite.addTest(CliTest.TestDlsCli_FromArgs(testname))
  return suite

def getSingleTestFile(testname):
  suite = unittest.TestSuite()
  suite.addTest(TestDlsCli_FromFile(testname))
  return suite

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
  print "Optionally, if \"args\" is specified only tests on commands using command line"
  print "arguments are perfomed; if \"file\" is specified instead, only tests using"
  print "arguments from a file (-f option) are done."
  
def usage():
  print "Usage:  DlsLfcCliTest.py <conf_file> [args | file]"

def main():
  if(len(sys.argv) < 2):
     msg = "Not enought input arguments!\n"
     msg += "You need to specify the configuration file as argument.\n"
     msg += "Probably you can use DlsLfcCliTest.conf, but please check it out first.\n"
     sys.stderr.write(msg+"\n")
     help()
     print
     usage()
     sys.exit(-1)

  setConf(sys.argv[1])
  
  if(len(sys.argv) > 2):
     if(sys.argv[2] == "args"):
        suite = getSuiteFromArgs()
     else:
        if(sys.argv[2] == "file"):
           suite = getSuiteFromFile()
        else:
           msg = "Error: The optional second argument must be either \"args\" or \"file\"!\n"
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


