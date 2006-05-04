#!/usr/bin/env python
#
# $Id: $
#
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
short_options="hv:i:e:"

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
fb="testblock-part1/testblock-part2"
se="testSE"
fileblock=DlsFileBlock(fb)
location=DlsLocation(se)
print "*** add a DLS entry with fileblock=%s location=%s"%(fileblock.name,location.host)
entry=DlsEntry(fileblock,[location])
try:
  api.add([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg

#myList=api.listFileBlocks("")
#for entry in myList:
#  print entry

# #############################
## get Location of the added DLS entry
# #############################
print "*** get Locations for fileblock=%s"%fileblock.name
entryList=[]
try:
  entryList=api.getLocations(fileblock)
except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
for entry in entryList:
  print "block %s"%entry.fileBlock.name
  for loc in entry.locations:
     print "locations found: %s"%loc.host
     
# #############################
## get FileBlocks given the added location
# #############################
#print "*** get FileBlocks given the location=%s"%location.host
#entryList=[]
#try:
#     entryList=api.getFileBlocks(location)
#except dlsApi.DlsApiError, inst:
#     msg = "Error in the DLS query: %s." % str(inst)
#     print msg
#print entryList
#for entry in entryList:
#      print entry.fileBlock.name

# #############################
## delete a DLS entry
# #############################
print "*** delete a DLS entry with fileblock=%s location=%s"%(fileblock.name,location.host)
entry=DlsEntry(fileblock,[location])
try:
  api.delete([entry])
except dlsApi.DlsApiError, inst:
     msg = "Error in deleting DLS entry: %s." % str(inst)
     print msg

# #############################
## check the removed DLS entry
# #############################
print "*** get Locations for the removed fileblock=%s"%fileblock.name
entryList=[]
try:
  entryList=api.getLocations(fileblock)
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


