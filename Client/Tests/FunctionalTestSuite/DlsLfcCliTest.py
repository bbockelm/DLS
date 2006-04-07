#!/usr/bin/python
 
import unittest

import anto_utils

from commands import getstatusoutput
from os import putenv, chdir, getcwd, environ
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

      msg1 = msg + " (DLS_TEST_ENDPOINT)"
      lfc_host = self.conf.get("DLS_TEST_ENDPOINT")
      self.assert_(lfc_host, msg1)

      msg1 = msg + " (DLS_CODE_PATH)"
      self.path = self.conf.get("DLS_CODE_PATH")
      self.assert_(self.path, msg1)

      msg1 = msg + " (LFC_DEL_PATH )"
      self.lfcdel = self.conf.get("LFC_DEL_PATH")
      self.assert_(self.lfcdel, msg1)

      # Set necessary environmental variables
      putenv("LFC_HOME", self.testdir)
      putenv("LFC_HOST", lfc_host)
      putenv("DLS_ENDPOINT", lfc_host)

      # Create the directory where to work
      cmd = "lfc-mkdir " + self.testdir 
      st, out = run(cmd)
      msg = "Error creating the LFC dir!",out 
      self.assertEqual(st, 0, msg)


  def tearDown(self):
     # Common clean up 
     st, out = run(self.lfcdel+"/lfc-del-dir -r "+self.testdir)


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

  # Test addition and get-se using command line arguments
  def testAdditionGetSE(self):
     cmd = self.path + "/dls-add $LFC_HOME/c1 CliTest_se4 CliTest_se5 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add $LFC_HOME/c1 CliTest_se4 CliTest_se5 CliTest_se6",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     msg = "The results obtained with dls-get-se are not those expected",out
     self.assertEqual(out, "CliTest_se4\nCliTest_se5\nCliTest_se6", msg)
     
  # Test deletion using command line arguments
  def testDeletion(self):
     # First add (already tested) 
     cmd = self.path + "/dls-add $LFC_HOME/c1 CliTest_se4 CliTest_se5 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add $LFC_HOME/c1 CliTest_se4 CliTest_se5 CliTest_se6",out 
     self.assertEqual(st, 0, msg)

     # Delete some replicas
     cmd = self.path + "/dls-delete $LFC_HOME/c1 CliTest_se4 CliTest_se5"
     st, out = run(cmd)
     msg = "Error in dls-delete $LFC_HOME/c1 CliTest_se4 CliTest_se5",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-get-se c1"
     st, out = run(cmd)
     msg = "Error in dls-get-se c1",out 
     self.assertEqual(st, 0, msg)
     msg = "The results obtained with dls-get-se are not those expected",out
     self.assertEqual(out, "CliTest_se6", msg)

     # Delete the entry
     cmd = self.path + "/dls-delete -a $LFC_HOME/c1"
     st, out = run(cmd)
     msg = "Error in dls-delete -a $LFC_HOME/c1",out 
     self.assertEqual(st, 0, msg)

# Test addition and get-se, for attributes using command line arguments
  def testAttrsAdditionGetSE(self):
     # Generate GUID 
     cmd = "uuidgen"
     st, guid = run(cmd)
     msg = "Error generating the GUID",guid
     self.assertEqual(st, 0, msg)
     
     # Define some vars
     fattrs="filesize=400 guid="+guid+" mode=0711"
     rattrs="sfn=sfn://my_sfn f_type=P ptime=45"

     # Addition with attributes 
     cmd = self.path + "/dls-add $LFC_HOME/c2 "+fattrs+" CliTest_se1 "+rattrs
     st, out = run(cmd)
     msg = "Error in dls-add $LFC_HOME/c2 "+fattrs+" CliTest_se1 "+rattrs,out 
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
     expected = "CliTest_se1 \t"+the_date+" \t45 \tP \tsfn://my_sfn"
     cmd = self.path + "/dls-get-se -l c2"
     st, out = run(cmd)
     msg = "Error in dls-get-se -l c2",out 
     self.assertEqual(out, expected, msg)
     
  # Test verbosity (on update after addition)
  def testVerbosity(self):
     cmd = self.path + "/dls-add $LFC_HOME/c1 CliTest_se1 CliTest_se2"
     st, out = run(cmd)
     msg = "Error in dls-add $LFC_HOME/c1 CliTest_se1 CliTest_se2",out 
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
     cmd = self.path + "/dls-add $LFC_HOME/c1 CliTest_se4 CliTest_se5 CliTest_se6"
     st, out = run(cmd)
     msg = "Error in dls-add $LFC_HOME/c1 CliTest_se4 CliTest_se5 CliTest_se6",out 
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
     cmd = self.path + "/dls-add $LFC_HOME/c1 CliTest_se1 CliTest_se2"
     st, out = run(cmd)
     msg = "Error in dls-add $LFC_HOME/c1 CliTest_se1 CliTest_se2",out 
     self.assertEqual(st, 0, msg)

     cmd = self.path + "/dls-add -v 2 -t $LFC_HOME/c1 CliTest_se3 CliTest_se1"
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

     cmd = self.path + "/dls-add -v 2 $LFC_HOME/c1 CliTest_se3 CliTest_se1"
     st, out = run(cmd)
     msg = "Error in dls-add -v 2 $LFC_HOME/c1 CliTest_se3 CliTest_se1",out 
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
        f_10_LFNs.write(self.testdir+"/f%d\n" %i)
        if ((i != 2) and (i!= 6)): 
           f_8_LFNs.write(self.testdir+"/f%d\n" %i)
        if(i < 5):
            f_10_LFNs_with_SURLs.write(self.testdir+"/f%d CliTest_se1 CliTest_se2\n" % i)
        else:
            f_10_LFNs_with_SURLs.write("f%d CliTest_se3\n" %i )
     
     f_2_LFNs.write(self.testdir+"/f2\nf6\n")
     f_2_LFNs_with_SURLs.write("f2 CliTest_se1 CliTest_se2\n"+self.testdir+"/f6 CliTest_se3\n")
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
     cmd = self.path + "/dls-get-se $LFC_HOME/f6"
     st, out = run(cmd)
     msg = "Error in dls-get-se $LFC_HOME/f6",out 
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
     cmd = self.path + "/dls-get-se $LFC_HOME/f6"
     st, out = run(cmd)
     msg = "Error in dls-get-se $LFC_HOME/f6",out 
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
       contains = out.find(self.testdir+"/f%d" % i) != -1
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
       contains = firsthalf.find(self.testdir+"/f%d" % i) != -1
       msg = "The results obtained (%s) with dls-get-fileblock -f are not those \
              expected (%s/f/%d)"  % (firsthalf, self.testdir, i)
       self.assert_(contains, msg)
           
     for i in xrange(5,10):
       contains = secondhalf.find(self.testdir+"/f%d" % i) != -1
       msg = "The results obtained (%s) with dls-get-fileblock -f are not those \
              expected (%s/f/%d)"  % (secondhalf, self.testdir, i)
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

     expected = "  FileBlock: "+self.testdir+"/f2\n"
     expected += "  FileBlock: "+self.testdir+"/f6"

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

     expected = "Error in the DLS query: Error when accessing the DLI for "
     expected += "%s/f2 of type lfn. SOAP-ENV:Client, InputData." % (self.testdir)

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
     cmd = self.path + "/dls-add $LFC_HOME/c1 CliTest_se1 ptime=333"
     st, out = run(cmd)
     msg = "Error in /dls-add $LFC_HOME/c1 CliTest_se1 ptime=333",out 
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


