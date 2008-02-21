#!/usr/bin/env python

import getopt,sys

import dlsClient
from dlsDataObjects import *

# #############################
def usage():
        print "Options"
        print "-h,--help \t\t\t Show this help"
        print "-v,--verbose \t\t\t Show output of procedures"
        print "-i,--iface_type <DLS type> \t DLS type "
        print "-e,--endpoint <hostname> \t DLS endpoint \n"

long_options=["help","verbose","iface_type=","endpoint="]
short_options="hvi:e:"

try:
     opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
except getopt.GetoptError:
     usage()
     sys.exit(2)
											     
if len(opts)<1:
   usage()
   sys.exit(2)

type = None
endpoint = None
verbose = False
for o, a in opts:
            if o in ("-v", "--verbose"):
                verbose = True
            if o in ("-h", "--help"):
                usage()
                sys.exit(2)
            if o in ("-i", "--iface_type"):
                type=a
            if o in ("-e", "--endpoint"):
                endpoint=a
        
if type==None:
      usage()
      print "error: --type <DLS type> is required"
      sys.exit(2)

if endpoint==None:
      usage()
      print "error: --endpoint <DLS endpoint> is required"
      sys.exit(2)

# #############################
## API
# #############################

## MySQL proto
#type='DLS_TYPE_MSQL'
#endpoint='lxgate10.cern.ch:18081'
## LFC proto
#type='DLS_TYPE_LFC' # or type='DLS_TYPE_DLI'
#endpoint=lfc-cms-test.cern.ch/grid/cms/fanfani/DLS/

print ""
print " DLS Server type: %s endpoint: %s"%(type,endpoint)
print ""
try:
     api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()


# #############################
## add a DLS entry
# #############################
# first entry
fbA="testblockA-part1/testblockA-part2"
seA="test-A-SE"
fileblockA=DlsFileBlock(fbA)
locationA=DlsLocation(seA)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblockA.name,locationA.host)
entry=DlsEntry(fileblockA,[locationA])
try:
  api.add([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg

#second entry
fbB="testblockB-part1/testblockB-part2"
seB="test-B-SE"
fileblockB=DlsFileBlock(fbB)
locationB=DlsLocation(seB)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblockB.name,locationB.host)
entry=DlsEntry(fileblockB,[locationB])
try:
  api.add([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg

#third entry: different fileblock same location as A
fbC="testblockC-part1/testblockC-part2"
fileblockC=DlsFileBlock(fbC)
locationC=DlsLocation(seA)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblockC.name,locationC.host)
entry=DlsEntry(fileblockC,[locationC])
try:
  api.add([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg



# #############################
## get Location of the added DLS entry
# #############################
print "*** get Locations for fileblock=%s"%fileblockA.name
entryList=[]
try:
 entryList=api.getLocations(fileblockA)
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
for entry in entryList:
  #print entry
  print "block %s"%entry.fileBlock.name
  for loc in entry.locations:
     print "locations found: %s"%loc.host

# #############################
## get Location of the added DLS entries
# #############################

print "*** get Locations for fileblocks = %s , %s"%(fileblockA.name,fileblockB.name)
entryList=[]
try:
  entryList=api.getLocations([fileblockA,fileblockB])
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
for entry in entryList:
  #print entry
  print "block %s"%entry.fileBlock.name
  for loc in entry.locations:
     print "locations found: %s"%loc.host


     
# #############################
## get FileBlocks given the added location
# #############################
print "*** get FileBlocks given the location=%s"%locationA.host
entryList=[]
try:
     entryList=api.getFileBlocks(locationA)
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg

print entryList
for entry in entryList:
       print entry

# #############################
## get FileBlocks given a list of location
# #############################
print "*** get FileBlocks given the locations = %s , %s"%(locationA.host,locationB.host)
entryList=[]
try:
     entryList=api.getFileBlocks([locationA,locationB])
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
                                                                                
print entryList
for entry in entryList:
       print entry 

# #############################
## delete a DLS first entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblockA.name,locationA.host)
entry=DlsEntry(fileblockA,[locationA])
try:
  api.delete([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg

# #############################
## delete a DLS second entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblockB.name,locationB.host)
entry=DlsEntry(fileblockB,[locationB])
try:
  api.delete([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg

# #############################
## delete a DLS third entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblockC.name,locationC.host)
entry=DlsEntry(fileblockC,[locationC])
try:
  api.delete([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg


# #############################
## check the removed DLS entry
# #############################
print "*** get Locations for the removed fileblock=%s"%fileblockA.name
entryList=[]
try:
  entryList=api.getLocations(fileblockA)
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
for entry in entryList:
  for loc in entry.locations:
     print "locations found: %s"%loc.host




# #############################
## get Locations given a CMS fileblock
# #############################
fb="bt_DST871_2x1033PU_g133_CMS/bt03_tt_2tauj"
print "*** get Locations for a CMS really existing fileblock=%s"%fb
entryList=[]
try:
     entryList=api.getLocations(fb)
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
for entry in entryList:
  for loc in entry.locations:
    print "locations found: %s"%loc.host


