#!/usr/bin/env python

import getopt,sys

import dlsClient
from dlsDataObjects import *



# #############################
def usage():
  print "Usage:"
  print "\tTestDlsDbsApi.py  -e <endpoint>  [-v]"
  print "\tTestDlsDbsApi.py  -h"
  print "\tTestDlsDbsApi.py  -u"
  print "Options:"
  print "\t-u,--usage\t\t\t Show usage information"
  print "\t-h,--help \t\t\t Show help information"
  print "\t-v,--verbose \t\t\t Show output of procedures"
  print "\t-e,--endpoint <hostname> \t DLS endpoint (DBS servlet URL)\n"

def help():
  print """By running this script, some queries are performed upon the specified
DBS server (DLS back-end). 

NOTE: This scripts adds some fake locations to existing FileBlocks, and then
deletes them afterwards. Be careful with what DBS you are running against.
Use some local or testing instance and not the global DBS.

At time of writing a usable testing DBS instance (this may have changed!) was:
https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_local_10_writer/servlet/DBSServlet

The script uses some pre-defined Fileblock names and locations, assuming they
exist in the specified DBS back-end. You can edit the code to use new ones if
that's more convenient for you. You probably need to because we can't know
what the contents of the DBS you are querying are.

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


long_options=["help","verbose=","usage","endpoint="]
short_options="hvue:"

try:
     opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
except getopt.GetoptError:
     usage()
     sys.exit(2)
											     
if len(opts)<1:
   usage()
   sys.exit(2)

type = dlsClient.DLS_TYPE_DBS
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
fbA="/test_primary_001/TestProcessedDS001/GEN-SIM#0cb2cf92-059f-49e9-92fa-44beeb09ef56"
fbB="/test_primary_001/TestProcessedDS001/GEN-SIM#0caad34b-df30-49c4-b527-ddd3a5685bcf"
fbC="/test_primary_001/TestProcessedDS001/GEN-SIM#0ca683d8-d860-40b6-b9fb-3f398c43d27e"
#fbA = "/mc-onsel-120_Incl_ttbar/CMSSW_1_2_0-FEVT-1166726158#2615ce70-e608-4a1b-b493-30e2ec29c699"
#fbB = "/mc-onsel-120_PU_Photon_Jets_pt_300_500/CMSSW_1_2_0-NoPU-DIGI-1171655775#60d74335-1ec6-45f7-ad0f-491694c2f75c"
#fbC = "/mc-onsel-120_PU_QCD_pt_15_20/CMSSW_1_2_0-FEVT-1167322268#e6d80d00-db26-410d-b25e-193e122f36de"
seA = "www.microsoft.com"
seB = "www.ibm.com"

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
## add a DLS entry
# #############################
# first entry
fileblockA=DlsFileBlock(fbA)
locationA=DlsLocation(seA)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblockA.name,locationA.host)
entry=DlsEntry(fileblockA,[locationA])
try:
  api.add([entry], errorTolerant=False)
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg+'\n'

#second entry
fileblockB=DlsFileBlock(fbB)
locationB=DlsLocation(seB)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblockB.name,locationB.host)
entry=DlsEntry(fileblockB,[locationB])
try:
  api.add([entry], errorTolerant=False)
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg+'\n'

#third entry: different fileblock same location as A
fileblockC=DlsFileBlock(fbC)
locationC=DlsLocation(seA)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblockC.name,locationC.host)
entry=DlsEntry(fileblockC,[locationC])
try:
  api.add([entry], errorTolerant=False)
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg+'\n'


# #############################
## get Location of the added DLS entry
# #############################
print "*** get Locations for fileblock=%s"%fileblockA.name
entryList=[]
try:
 entryList=api.getLocations(fileblockA, errorTolerant=False)
 print
 for entry in entryList:
  print entry.simpleStr()
 print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'

# #############################
## get Location of the added DLS entries
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
## get FileBlocks given the added location
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
## delete a DLS first entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblockA.name,locationA.host)
entry=DlsEntry(fileblockA,[locationA])
try:
  api.delete([entry], errorTolerant=False)
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg+'\n'

# #############################
## delete a DLS second entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblockB.name,locationB.host)
entry=DlsEntry(fileblockB,[locationB])
try:
  api.delete([entry], errorTolerant=False)
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg+'\n'

# #############################
## delete a DLS third entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblockC.name,locationC.host)
entry=DlsEntry(fileblockC,[locationC])
try:
  api.delete([entry], errorTolerant=False)
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg+'\n'


# #############################
## check the removed DLS entry
# #############################
print "*** get Locations for the removed fileblock=%s"%fileblockA.name
entryList=[]
try:
  entryList=api.getLocations(fileblockA, errorTolerant=False)
  for entry in entryList:
    for loc in entry.locations:
       print "locations found: %s"%loc.host
  print "===> No Exception thrown\n"
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg+'\n'
