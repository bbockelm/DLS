#!/usr/bin/env python
 
#
# $Id: DlsLfcCliTest.py,v 1.17 2007/02/05 15:17:47 delgadop Exp $
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

# Need some pre-existing fileblocks in DBS
#f1 = "/_20070227_14h44m01sthis/isatestblock#016712"
#f2 = "/TestPrimary_001_20070227_14h44m01s/TestProcessed_20070227_14h44m01s#dc081709-b903-45fc-a68e-b9c36b5a2397"
#f3 = "/MTCC-110-os-DAQ-MTCC2/MTCC2#01"

# TODO: Basic tests for dump, get-all-locations... but already tested in the API


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

      # Set necessary environmental variables
      putenv("DLS_ENDPOINT",  self.host + self.testdir)
      putenv("DLS_TYPE", "DLS_TYPE_DBS")
      # DLS code for direct api calls
      sys.path.append(self.path)

      # Pre-existing files
      msg1 = msg + " (DBS_f1)"
      self.f1 = self.conf.get("DBS_f1")
      self.assert_(self.f1, msg1)

      msg1 = msg + " (DBS_f2)"
      self.f2 = self.conf.get("DBS_f2")
      self.assert_(self.f2, msg1)

      msg1 = msg + " (DBS_f3)"
      self.f3 = self.conf.get("DBS_f3")
      self.assert_(self.f3, msg1)

      # Unset DBS_CLIENT_CONFIG, as it has to be managed by the commands
      self.dbscfg = self.conf.get("DBS_CONFIG_FILE")
      putenv("DBS_CLIENT_CONFIG", self.dbscfg)

      # Delete previous locations in interesting fileBlocks
      cmd = self.path + "/dls-delete -a %s" %(self.f1)
      st, out = run(cmd)
      msg = "Error in %s: %s" %(cmd,out)
      self.assertEqual(st, 0, msg)
      
      cmd = self.path + "/dls-delete -a %s" %(self.f2)
      st, out = run(cmd)
      msg = "Error in %s: %s" %(cmd,out)
      self.assertEqual(st, 0, msg)

      cmd = self.path + "/dls-delete -a %s" %(self.f3)
      st, out = run(cmd)
      msg = "Error in %s: %s" %(cmd,out)
      self.assertEqual(st, 0, msg)

  def tearDown(self):
     # Common clean up
     pass


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
     unsetenv("DBS_CLIENT_CONFIG")
      
     # First, wrong interface selection
     cmd = self.path + "/dls-add --skip-location-check -i SOMETHING c1"
     st, out = run(cmd)
     expected = "Unsupported interface type: SOMETHING\nSupported values: ['DLS_TYPE_LFC', 'DLS_TYPE_MYSQL', 'DLS_TYPE_DBS']"
     msg = "Results (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)

     # Next the DBS URL
     cmd = self.path + "/dls-add --skip-location-check %s"%(self.f1)
     st, out = run(cmd)
     expected = "Error when binding the DLS interface"
     expected2 = "Could not set the interface to the DLS server"
     msg = "Results (%s) don't contain expected (%s, %s)" % (out, expected, expected2)
     contains = (out.find(expected) != -1) and (out.find(expected2) != -1)
     self.assert_(contains, msg)
     
     # Now correct endpoint but incorrect type
     cmd = self.path + "/dls-add --skip-location-check -e %s%s -i DLS_TYPE_DLI %s CliTest-se1" % (self.host, self.testdir, self.f1)
     st, out = run(cmd)
     expected = "Unsupported interface type: DLS_TYPE_DLI"
     msg = "Results (%s) don't contain expected (%s)" % (out, expected)
     contains = (out.find(expected) != -1)
     self.assert_(contains, msg)
   
     # With everything right, the thing should work 
     putenv("DBS_CLIENT_CONFIG", self.dbscfg)
     cmd = self.path + "/dls-add --skip-location-check -e %s%s %s CliTest-se1"%(self.host, self.testdir, self.f1)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add --skip-location-check -e %s%s -i DLS_TYPE_DBS %s CliTest-se2" % (self.host, self.testdir, self.f2)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

# No update yet
#     cmd = self.path + "/dls-update -e %s%s c1 filesize=100" % (self.host, self.testdir)
#     st, out = run(cmd)
#     msg = "Error in dls-update -e %s%s c1 filesize=100: %s" % (self.host, self.testdir, out)
#     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se -e %s%s %s"%(self.host, self.testdir, self.f1)
     st, out = run(cmd)
     msg = "Error in dls-get-se -e %s%s %s: %s" % (self.host, self.testdir, self.f1, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-fileblock -e %s%s CliTest-NOTHING" % (self.host, self.testdir)
     st, out = run(cmd)
     msg = "Error in dls-get-fileblock -e %s%s CliTest-NOTHING: %s" % (self.host, self.testdir, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-delete -e %s%s -a %s" % (self.host, self.testdir,self.f1)
     st, out = run(cmd)
     msg = "Error in dls-delete -e %s%s -a %s: %s" % (self.host,self.testdir,self.f1,out)
     self.assertEqual(st, 0, msg)




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
     cmd = self.path + "/dls-add --skip-location-check %s CliTest-se4 CliTest-se5 CliTest-se6"%(self.f1)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     # Delete some replicas
     cmd = self.path + "/dls-delete %s CliTest-se4 CliTest-se5"%(self.f1)
     st, out = run(cmd)
     msg = "Error in dls-delete %s CliTest-se4 CliTest-se5: %s" %(self.f1, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se %s"%(self.f1)
     st, out = run(cmd)
     msg = "Error in dls-get-se %s: %s" % (self.f1,out)
     self.assertEqual(st, 0, msg)
     expected = "\nCliTest-se6"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" %(out,expected)
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
     cmd = self.path + "/dls-add --skip-location-check %s CliTest-se4 CliTest-se5 CliTest-se6"%(self.f1)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se %s"%(self.f1)
     st, out = run(cmd)
     msg = "Error in dls-get-se %s: %s"% (self.f1, out)
     self.assertEqual(st, 0, msg)
     expected = "\nCliTest-se4\nCliTest-se5\nCliTest-se6"
     msg = "The results obtained with dls-get-se (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)
     
     cmd = self.path + "/dls-add --skip-location-check --allow-empty-blocks %s"%(self.f2)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-list /"
     st, out = run(cmd)
     msg = "Error in dls-list /",out 
     self.assertEqual(st, 0, msg)
     expected = "%s"%(self.f1)
     expected2 = "%s"%(self.f2)
     msg = "Results (%s) don't contain expected (%s, %s)" % (out, expected, expected2)
     contains = (out.find(expected) != -1) and (out.find(expected) != -1)
     self.assert_(contains, msg)

     cmd = self.path + "/dls-list %s"%self.f2
     st, out = run(cmd)
     msg = "Error in dls-list %s: %s" % (out, self.f2)
     self.assertEqual(st, 0, msg)
     expected = '\n'+self.f2
     msg = "The results obtained with dls-list (%s) are not those expected (%s)" % (out, expected)
     self.assertEqual(out, expected, msg)


     
  # Test erroneous addition for FileBlock and locations using CLI args 
  def testErroneousAddition(self):
   
     # Erroneous location (wrong hostname)
     cmd = self.path + "/dls-add -t %s CliTest_#@_WRONG"%(self.f1)
     st, out = run(cmd)
     msg = "Unexpected success in %s: %s" % (cmd, out) 
     wasError = (st!=0)
     self.assert_(wasError, msg)
     expected = "Wrong specified host (CliTest_#@_WRONG) of DlsLocation object"
     contains = out.find(expected) != -1
     msg = "Output obtained with %s (%s) is not that expected (%s)" % (cmd, out, expected)
     self.assert_(contains, msg)
     cmd = self.path + "/dls-get-se %s"%(self.f1)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (msg,out)
     self.assertEqual(st, 0, msg)
     expected = "\n"
     contains = out == expected
     msg = "The results obtained with %s (%s) are not those expected (%s)"%(cmd,out,expected)
     self.assert_(not contains, msg)
     
     # Erroneous location (existing)
     # TODO: Check that DBS outputs the appropriate warning...
     


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
     cmd = self.path + "/dls-add --skip-location-check %s CliTest-se1 CliTest-se2" % (self.f1)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add --skip-location-check %s CliTest-se1" % (self.f2)
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)

    # Now replace one location
     cmd = self.path + "/dls-replace-location --skip-location-check -o CliTest-se1 -n CliTest-NEW"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)

    # Now check what happened
     cmd = self.path + "/dls-get-fileblock CliTest-NEW"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     contains = out.find(self.f1) != -1
     contains *= out.find(self.f2) != -1
     expected = self.f1+", "+self.f2
     msg = "The results obtained with %s (%s) are not those expected (%s)" % (cmd,out,expected)
     self.assert_(contains, msg)
     cmd = self.path + "/dls-get-fileblock CliTest-se1"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out) 
     self.assertEqual(st, 0, msg)
     expected = ""
     msg = "The results obtained with %s (%s) are not those expected (%s)" % (cmd,out,expected)
     self.assertEqual(out, expected, msg)


# TODO: When custodial flag is there, before replacing location we need to think
#       what we do in that case.


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
     f_3_LFNs = open('3_LFNs', 'w')
     f_3_LFNs_with_SURLs = open('3_LFNs_with_SURLs', 'w')
     f_check_locs= open('f_check_locs', 'w')

     f_3_LFNs.write("%s\n%s\n%s\n" % (self.f1,self.f2,self.f3))
     f_3_LFNs_with_SURLs.write("%s CliTest-se1 CliTest-se2\n%s CliTest-se2 CliTest-se3\n%s CliTest-se3\n" % (self.f1,self.f2,self.f3))
     f_check_locs.write("f_EMPTY\nfSTH2 sdf\n%s  www.google.com asfsd www.cern.ch\n" % (self.f1))
     
     f_3_LFNs.close()
     f_3_LFNs_with_SURLs.close()
     f_check_locs.close()


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


#TODO: add tests with -f for dls-get-fileblock, dls-list, ...
#TODO: add tests with patterns

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
  def testMulti(self):
     # Check there's none left
     cmd = self.path + "/dls-get-se -f 3_LFNs"
     st, buffer = run(cmd)
     msg = "Error in dls-get-se -f 3_LFNs",buffer
     self.assertEqual(st, 0, msg)
     out = buffer + "\n"
     expected="\n  FileBlock: %s\n"%self.f1
     expected += "  FileBlock: %s\n"%self.f2
     expected += "  FileBlock: %s\n"%self.f3
     msg = "Results obtained with dls-get-se -f (%s) are not those expected (%s)"%(out, expected)
     self.assertEqual(out, expected, msg)

     # Now add
     cmd = self.path + "/dls-add --skip-location-check -f 3_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add --skip-location-check -f 3_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)

     # Check they are there
     cmd = self.path + "/dls-get-se -f 3_LFNs"
     st, buffer = run(cmd)
     msg = "Error in dls-get-se -f 2_LFNs",buffer
     self.assertEqual(st, 0, msg)
     out = buffer + "\n"
     expected="\n  FileBlock: %s\nCliTest-se1\nCliTest-se2\n"%self.f1
     expected += "  FileBlock: %s\nCliTest-se2\nCliTest-se3\n"%self.f2
     expected += "  FileBlock: %s\nCliTest-se3\n"%self.f3
     msg = "Results obtained with dls-get-se -f (%s) are not those expected (%s)"%(out, expected)
     self.assertEqual(out, expected, msg)


  # Test host and empty fB checks using list files
  def testHostEmptyChecks(self):
     cmd = self.path + "/dls-add -f f_check_locs"
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd, out)
     self.assertEqual(st, 0, msg)
     contains = out.find("Warning: Skipping FileBlock f_EMPTY with no associated location.") != -1
     contains *= out.find("Warning: Skipping location. Wrong location sdf for FileBlock fSTH2: Wrong specified host (sdf) of DlsLocation object") != -1
     contains *= out.find("Warning: Skipping FileBlock fSTH2 with no associated location.") != -1
     contains *= out.find("Warning: Skipping location. Wrong location asfsd for FileBlock %s: Wrong specified host" % self.f1) != -1
     msg = "The results obtained (%s) with %s are not those expected (see code)"%(out,cmd)
     self.assert_(contains, msg)

     cmd = self.path + "/dls-get-se %s"%(self.f1)
     st, out = run(cmd)
     msg = "Error in dls-get-se fSTH1", out
     self.assertEqual(st, 0, msg)

     expected = "www.cern.ch"
     expected2 = "www.google.com"
     msg = "Results (%s) don't contain expected (%s, %s)" % (out, expected, expected2)
     contains = (out.find(expected) != -1) and (out.find(expected2) != -1)
     self.assert_(contains, msg)



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
     # First add
     cmd = self.path + "/dls-add --skip-location-check -f 3_LFNs_with_SURLs"
     st, out = run(cmd)
     msg = "Error in dls-add --skip-location-check -f 3_LFNs_with_SURLs",out 
     self.assertEqual(st, 0, msg)
     cmd = self.path + "/dls-add --skip-location-check %s CliTest-se3"%self.f1
     st, out = run(cmd)
     msg = "Error in %s: %s" % (cmd,out)
     self.assertEqual(st, 0, msg)


    # Now retrieve them
     cmd = self.path + "/dls-get-fileblock CliTest-se3"
     st, out = run(cmd)
     msg = "Error in /dls-get-fileblock CliTest-se3",out
     self.assertEqual(st, 0, msg)

     for i in [self.f1,self.f2,self.f3]:
       contains = out.find(i) != -1
       expected = self.f1 + ', ' +  self.f2 + ', ' + self.f3
       msg = "The results obtained with %s (%s) are not those expected (%s)" % (cmd,out,expected)
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


