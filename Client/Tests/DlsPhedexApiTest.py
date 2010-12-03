#!/usr/bin/env python

"""
DLS-phedex API and endpoint tests
"""

import getopt, os, sys
import unittest

import dlsClient
from dlsDataObjects import *



############  CONSTANTS  ############

# API type (don't change)
TYPE = dlsClient.DLS_TYPE_PHEDEX

## Default endpoint (if not overriden by env vars)
ENDPOINT = 'https://cmsweb.cern.ch/phedex/datasvc/xml/prod'

## Some predefined fileblocks and locations
## NOTE: Modify this definitions if these become old-fashioned
fbPattern = '/Zmumu/Summer09-MC_31X_V3-v1/GEN-SIM-RECO#8*'
fbA = "/Zmumu/Summer09-MC_31X_V3-v1/GEN-SIM-RECO#84bbc9b5-1185-4d6a-8607-ecdd93e29417"
fbB = "/Zmumu/Summer09-MC_31X_V3-v1/GEN-SIM-RECO#82801173-7a7b-4185-bd94-e1c71d762db5"
fbC = "/Zmumu/Summer09-MC_31X_V3-v1/GEN-SIM-RECO#85fa9514-b9d5-4b15-a366-9567999c31e3"
seA = "srm.ciemat.es"
seB = "storm.ifca.es"



############  FUNCTIONS ############

def usage():
  print "Usage:"
  print "\tTestDlsPhedexApi.py"
  print "\tTestDlsPhedexApi.py  -h"
  print "\tTestDlsPhedexApi.py  -u"
  print "\nOptions:"
  print "\t-h,--help \t\t\t Show usage information"
  print "\t-h,--help \t\t\t Show help information"

def help():
  print """By running this script, some queries are performed upon the specified
PhEDEx server (DLS back-end). 

The script uses some pre-defined Fileblock names and locations, assuming they
exist in the specified PhEDEx back-end. You can edit the code to use new ones if
that's not the case.

For each query on the server, if the API throws any exception, this is caught
and the error message shown. Otherwise the script informs that no exception
was produced. However, you will need to inspect the output of the commands
to be totally sure that the obtained results are as expected (the added
locations are where they should and disappear after deletion...).

Be aware that some Fileblocks may contain, from other users, other locations
than those added during the tests (and other Fileblocks the same locations).
This does not mean that the tests or the API do not work.
"""
  usage()


class DlsPhedexApiTest(unittest.TestCase):
    """
    TestCase for DlsPhedexApi module
    """

    # Couple of class variables to use in several tests
    fblockA = DlsFileBlock(fbA)
    fblockB = DlsFileBlock(fbB)
    fblockPat = DlsFileBlock(fbPattern)
    locA = DlsLocation(seA)
    locB = DlsLocation(seB)

    def setUp(self):
        """
        Code to execute to in preparation for the test
        """
        # Set a shorter reference to access our class
        self.C = self.__class__

    def tearDown(self):
        """
        code to execute to clean up after tests
        """
        pass


    def testA_CreateAPI(self):
        print "\n\n########## Testing API creation"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT, check_endpoint = True)
        self.assert_(True)


    def testB_ListOneFileblock(self):
        print "\n\n########## Testing the listing of one fileblock"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "List a DLS entry with fileblock=%s" % fbA
        fList = api.listFileBlocks(self.C.fblockA)
        print "\nfList: %s" % ([x.name for x in fList],)
        self.assert_(len(fList) == 1)
        self.assertEqual(fList[0].name, fbA)


    def testC_ListFileblockPattern(self):
        print "\n\n########## Testing the listing of one fileblock and a pattern"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "List several DLS entries for pattern=%s and block=%s"%(fbPattern, fbB)
        fList = api.listFileBlocks([self.C.fblockPat, self.C.fblockB])
        print "\nfList: %s" % ([x.name for x in fList],)
        self.assert_(len(fList) >= 2)
        self.assert_(fbA in [x.name for x in fList])
        self.assert_(fbB in [x.name for x in fList])

        # Store fileblock list for use at test H
        self.C.fListFbPat = fList


    def testD_GetBlocksOfLocation(self):
        print "\n\n########## Testing the retrieval of fileblocks for one location"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Get FileBlocks given the location: %s" % (seA)
        entryList = api.getFileBlocks(self.C.locA, errorTolerant=False)
        fList = [x.fileBlock.name for x in entryList]
        print "\nlen(fList): %s" % len(fList)
        self.assert_(len(fList) > 1)

        # Store one fileblock at the given location for next test
        self.C.fblockSeA = fList[0]

        # Store the block list length to compare with test F
        self.C.lenListSeA = len(fList)


    def testE_GetLocationOfOneBlock(self):
        print "\n\n########## Getting location of one fileblock"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Get location of fileblock=%s" % self.C.fblockSeA
        entryList = api.getLocations(self.C.fblockSeA, errorTolerant = False)
        self.assert_(len(entryList) == 1)

        locs = [x.host for x in entryList[0].locations]
        print "\nlocations: %s" % locs
        self.assert_(seA in locs)

        # Store the locations list to compare with those of tests G and I
        self.C.locsFblockSeA = locs


    def testF_GetBlocksOfLocationList(self):
        print "\n\n########## Testing the retrieval of fileblocks for several locations"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Get FileBlocks given the locations: %s, %s" % (seA, seB)
        entryList = api.getFileBlocks([self.C.locA, self.C.locB], errorTolerant=False)
        fList = [x.fileBlock.name for x in entryList]
        print "\nlen(fList): %s" % len(fList)
        self.assert_(len(fList) > self.C.lenListSeA)


    def testG_GetLocationOfOneBlockWithT01(self):
        print "\n\n########## Getting location of one fileblock including T0 and T1s"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Get location of fileblock=%s" % self.C.fblockSeA
        entryList = api.getLocations(self.C.fblockSeA, errorTolerant=False, showProd=True)
        self.assert_(len(entryList) == 1)

        locs = [x.host for x in entryList[0].locations]
        print "\nlocations: %s" % locs
        self.assert_(seA in locs)
        self.assert_(len(locs) > len(self.C.locsFblockSeA))
        for loc in self.C.locsFblockSeA:
            self.assert_(loc in locs)


    def testH_DumpEntriesWithPattern(self):
        print "\n\n########## Dump several entries for pattern"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Dumping entries for pattern: %s" % fbPattern
        entryList = api.dumpEntries(fbPattern, errorTolerant = False)
        self.assert_(len(entryList) > 1)

        fList = [x.fileBlock.name for x in entryList]
        print "\nfList: %s" % fList
        print "\nself.C.fListFbPat: %s" % [x.name for x in self.C.fListFbPat]

        # Check the fileblocks are the same as those listed before
        self.assertEqual(len(fList), len(self.C.fListFbPat))
        for fb in fList:
            self.assert_(fb in [x.name for x in self.C.fListFbPat])

        # Check each fileblock has a non-emtpy list of locations
        for entry in entryList:
            self.assert_(len(entry.locations) > 0)


    def testI_GetLocationOfListOfBlocks(self):
        print "\n\n########## Getting locations of a list of fileblocks"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Get location of fileblocks: %s, %s" % (self.C.fblockSeA, fbA)
        entryList = api.getLocations([self.C.fblockSeA, self.C.fblockA], errorTolerant=False)
    
        for entry in entryList:
            
            block = entry.fileBlock.name
            locs = [x.host for x in entry.locations]
            print "\nlocations for block %s: %s" % (block, locs)

            if block == fbA:
                self.assert_(len(entry.locations) > 0)
            else:
                self.assertEqual(len(entry.locations), len(self.C.locsFblockSeA))
                for loc in self.C.locsFblockSeA:
                    self.assert_(loc in locs)


    def testJ_GetAllLocations(self):
        print "\n\n########## Getting all existing locations"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        locList = api.getAllLocations()
        self.assert_(len(locList) > 20)
        locs = [x.host for x in  locList]
        print "All locations: %s" % locs

        self.assert_(seA in locs)
        self.assert_(seB in locs)
        for loc in self.C.locsFblockSeA:
            self.assert_(loc in locs)


    def testK_GetFileLocsForOneBlock(self):
        print "\n\n########## Getting file locs for one fileblock"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT)

        print "Get file locs for: %s" % self.C.fblockSeA
        fileList = api.getFileLocs(self.C.fblockSeA)
        self.assert_(len(fileList) == 1)

        block = fileList[0][0]
        self.assertEqual(block.name, self.C.fblockSeA)

        fileLocs = fileList[0][1]
        self.assert_(len(fileLocs) > 1)

        for f in fileLocs:
            for loc in fileLocs[f]:
#                print 'f, locs: %s, %s' % (f, fileLocs[f])
                self.assert_(loc.host in self.C.locsFblockSeA)


    def testL_QueryWithUserAgent(self):
        print "\n\n########## Querying with user agent set"
        api = dlsClient.getDlsApi(dls_type=TYPE, dls_endpoint=ENDPOINT, 
                       uaFlexString = 'This is a test', 
                       uaClientsList = [['Tester', '1.1'], ['Sth', '2_4']])

        print "Get location of fileblock=%s" % self.C.fblockSeA
        entryList = api.getLocations(self.C.fblockSeA, errorTolerant = False)
        self.assert_(len(entryList) == 1)

        locs = [x.host for x in entryList[0].locations]
        print "\nlocations: %s" % locs
        self.assert_(seA in locs)

        # Store the locations list to compare with those of tests G and I
        self.C.locsFblockSeA = locs




########## MAIN ###########

if __name__ == '__main__':
    
    long_options=["help", "usage"]
    short_options="hu"
    try:
         opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
    except getopt.GetoptError:
         usage()
         sys.exit(2)
                                                     
    for o, a in opts:
                if o in ("-u", "--usage"):
                    usage()
                    sys.exit(0)
                if o in ("-h", "--help"):
                    help()
                    sys.exit(0)
            
    if 'DLS_ENDPOINT' in os.environ:
            ENDPOINT = os.environ['DLS_ENDPOINT']
    elif 'DLS_PHEDEX_ENDPOINT' in os.environ:
            ENDPOINT = os.environ['DLS_PHEDEX_ENDPOINT']

    unittest.main()
