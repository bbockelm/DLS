#!/usr/bin/env python

import getopt,sys

import dlsClient
from dlsDataObjects import *



# #############################
def usage():
  print "Usage:"
  print "\tTestDlsPhedexApi.py  -e <endpoint>  [-v]"
  print "\tTestDlsPhedexApi.py  -h"
  print "\tTestDlsPhedexApi.py  -u"
  print "Options:"
  print "\t-h,--help \t\t\t Show usage information"
  print "\t-h,--help \t\t\t Show help information"
  print "\t-v,--verbose \t\t\t Show output of procedures"
  print "\t-e,--endpoint <hostname> \t DLS endpoint (PhEDEx servlet URL)\n"

def help():
  print """By running this script, some queries are performed upon the specified
PhEDEx server (DLS back-end). 

The script uses some pre-defined Fileblock names and locations, assuming they
exist in the specified PhEDEx back-end. You can edit the code to use new ones if
that's more convenient for you.

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


long_options=["help","verbose","usage","endpoint="]
short_options="hv:ue:"

try:
     opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
except getopt.GetoptError:
     usage()
     sys.exit(2)
											     
if len(opts)<1:
   usage()
   sys.exit(2)

type = dlsClient.DLS_TYPE_PHEDEX
endpoint = None
verbose = False
for o, a in opts:
            if o in ("-v", "--verbose"):
                verbose = True
            if o in ("-u", "--usage"):
                usage()
                sys.exit(0)
            if o in ("-h", "--help"):
                help()
                sys.exit(0)
            if o in ("-e", "--endpoint"):
                endpoint=a
        
if endpoint==None:
      usage()
      print "error: --endpoint <DLS endpoint> is required"
      sys.exit(2)


# #############################
## Some predefined fileblocks and locations
# #############################
fbPattern='/CSA07AllEvents/CMSSW_1_6_7-CSA07-Stew-B1-PDAllEvents-Skims2-muonL1AODSIM/USER#a*'
fbA="/CSA07AllEvents/CMSSW_1_6_7-CSA07-Stew-B1-PDAllEvents-Skims2-muonL1AODSIM/USER#ad0560bb-9bc1-4e84-aa81-6f32c16f9c65"
fbB="/CSA07AllEvents/CMSSW_1_6_7-CSA07-Stew-B1-PDAllEvents-Skims2-muonL1AODSIM/USER#a83d4262-afbe-463e-aeac-d9c21724b4b6"
fbC="/CSA07AllEvents/CMSSW_1_6_7-CSA07-Stew-B1-PDAllEvents-Skims2-muonL1AODSIM/USER#a06748f3-441b-4b32-ab85-c2fa4ec67e44"
seA = "srm.ciemat.es"
seB = "dpm01.ifca.es"

# #############################
## API
# #############################

print ""
print " DLS Server type: %s endpoint: %s"%(type,endpoint)
print ""
try:
     api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      rc = inst.rc
      sys.exit(rc)


# #############################
## List a DLS entry
# #############################
# first entry
fileblockA=DlsFileBlock(fbA)
fileblockB = DlsFileBlock(fbB)
print "*** list a DLS entry with fileblock=%s"%(fileblockA.name)
try:
  fList = api.listFileBlocks(fileblockA)
  for f in fList:
     print f
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error listing a DLS fileBlock: %s." % str(inst)
     print msg+'\n'

#second entry
fileblockPattern = DlsFileBlock(fbPattern)
print "*** list several DLS entries for pattern=%s"%(fileblockPattern.name)
try:
  fList = api.listFileBlocks([fileblockPattern])
  for f in fList:
     print f
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error listing DLS pattern: %s." % str(inst)
     print msg+'\n'


# #############################
## get Location of the DLS entry
# #############################
locationA=DlsLocation(seA)
locationB=DlsLocation(seB)
print "*** get Locations for fileblock=%s (should return: %s, %s)"%(fileblockA.name, seA, seB)
entryList=[]
try:
 entryList = api.getLocations(fileblockA, errorTolerant=False)
 print
 for entry in entryList:
   print entry.simpleStr()
 print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'

# #############################
## Dump several DLS entries with pattern
# #############################
# first entry
print "*** dump DLS entries for pattern=%s"%(fileblockPattern.name)
try:
  entryList = api.dumpEntries(fileblockPattern.name)
  print
  for entry in entryList:
    print entry.simpleStr()
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg+'\n'

# #############################
## get Location of the DLS entries
# #############################

print "*** get Locations for fileblocks = %s , %s"%(fileblockA.name,fileblockB.name)
entryList=[]
try:
  entryList=api.getLocations([fileblockA,fileblockB], errorTolerant=False)
  print
  for entry in entryList:
    print entry.simpleStr()
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'

     
# #############################
## get FileBlocks given the location
# #############################
print "*** get FileBlocks given the location=%s"%locationA.host
entryList=[]
try:
  entryList=api.getFileBlocks(locationA, errorTolerant=False)
  print
  for entry in entryList:
     print entry.simpleStr()
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'


# #############################
## get FileBlocks given a list of locations
# #############################
print "*** get FileBlocks given the locations = %s , %s"%(locationA.host,locationB.host)
entryList=[]
try:
  entryList=api.getFileBlocks([locationA,locationB], errorTolerant=False)
  for entry in entryList:
       print entry.simpleStr()
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'
                                                                                

# #############################
## get all existing locations
# #############################
print "*** get all existing locations"
locList=[]
try:
  locList=api.getAllLocations()
  for loc in locList:
       print loc
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'
