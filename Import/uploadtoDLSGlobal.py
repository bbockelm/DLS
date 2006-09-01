#!/usr/bin/env python

import getopt,sys,os,string

import dlsClient
from dlsDataObjects import *

# #############################
def usage():
        print "\nOptions"
        print "-h,--help \t\t\t Show this help"
        print "-v,--verbose \t\t\t Show output of procedures"
        print "-i,--iface_type <DLS type> \t Global DLS type "
        print "-e,--endpoint <hostname> \t Global DLS endpoint "
        print "-d,--dbslocal <DBS instance> \t DBS Local instance to start from"
        print "-l,--localblock <blockname> \t original fileblock name in local DLS"
        print "-g,--globalblock <blockname> \t new fileblock name in global DLS"
        print "-m,--mapblockFile <file name> \t File with the mapping of local and new fileblock names for a given DBS local instance \n"

long_options=["help","iface_type=","endpoint=","dbslocal=","localblock=","globalblock=","mapblockFile="]
short_options="h:i:e:d:l:g:m:"

try:
     opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
except getopt.GetoptError:
     usage()
     sys.exit(2)

type = None
endpoint = None
localDBS = None
localblock = None
globalblock = None
mapblockFile = None

for o, a in opts:
            if o in ("-h", "--help"):
                usage()
                sys.exit(2)
            if o in ("-i", "--iface_type"):
                type=a
            if o in ("-e", "--endpoint"):
                endpoint=a
            if o in ("-d", "--dbslocal"):
                localDBS=a
            if o in ("-l", "--localblock"):
                localblock=a
            if o in ("-g", "--globalblock"):
                globalblock=a
            if o in ("-m", "--mapblockFile"):
                mapblockFile=a

        
if type==None:
      usage()
      print "error: --iface_type <Global DLS type> is required"
      sys.exit(2)

if endpoint==None:
      usage()
      print "error: --endpoint <Global DLS endpoint> is required"
      sys.exit(2)

if localDBS==None:
      usage()
      print "error: --dbslocal <local DBS instance> is required"
      sys.exit(2)


if ( (localblock == None) or (globalblock == None)) and (mapblockFile == None) :
    print "\n either --localfileblock and --globalfileblock OR --mapblockFile option has to be provided"
    usage()
    sys.exit(2)

if (localblock != None or globalblock != None ) and (mapblockFile != None) :
    print "\n options --localfileblock/globalfileblock or --mapblockFile are mutually exclusive"
    usage()
    sys.exit(2)

if (mapblockFile != None) :
 expand_mapblockFile=os.path.expandvars(os.path.expanduser(mapblockFile))
 if not os.path.exists(expand_mapblockFile):
    print "File not found: %s" % expand_mapblockFile


#  //
# //  Map the local DBS instances with the DLS ones
#//
MapDBSDLSinstance={
 'MCLocal_1': 'lxgate10.cern.ch:18081', 
 'MCLocal_2': 'prod-lfc-cms-central.cern.ch/grid/cms/DLS/MCLocal_2',
 'MCLocal_3': 'prod-lfc-cms-central.cern.ch/grid/cms/DLS/MCLocal_3',
 'MCLocal_4': 'prod-lfc-cms-central.cern.ch/grid/cms/DLS/MCLocal_4',
 }

#  //
# //  Local DLS instance to start from
#//
localDLSendpoint=MapDBSDLSinstance[localDBS]
localDLStype='DLS_TYPE_LFC'
if localDBS == 'MCLocal_1':
  localDLStype='DLS_TYPE_MYSQL'

#  //
# //  Local and Global DLS API
#//
print ""
print " From Local DLS Server endpoint: %s (type: %s) "%(localDLSendpoint,localDLStype)
try:
     localDLSapi = dlsClient.getDlsApi(dls_type=localDLStype,dls_endpoint=localDLSendpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()

print " to Global DLS Server endpoint: %s (type: %s)"%(endpoint,type)
print ""
try:
     globalDLSapi = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()


def UploadBlock(localfileblock,globalfileblock):

  #  //
  # // get location of the fileblock in local DLS 
  #//
  entryList=[]
  locationList=[]
  try:
     entryList=localDLSapi.getLocations(localfileblock)
  except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
     return
  for entry in entryList:
    print ">> local fileblock: %s located at : "%entry.fileBlock.name
    for loc in entry.locations:
      print " %s"%loc.host
      locationList.append(DlsLocation(loc.host))

  #  //
  # // add fileblock in global DLS with locations of the original local fileblock
  #//
  global_block=DlsFileBlock(globalfileblock)
  entry=DlsEntry(global_block,locationList)


  try:
     globalDLSapi.add([entry])
  except dlsApi.DlsApiError, inst:
     msg = "Error adding a DLS entry: %s." % str(inst)
     print msg
     return
  print ">> migrated to global fileblock: %s located at :"%(global_block.name,)
  for loc in locationList:
    print "%s"%loc.host

  return

#  //
# //   Upload a fileblock from local to global
#//
if localblock != None and globalblock != None :
  UploadBlock(localblock,globalblock)

if mapblockFile != None :
   map_file = open(expand_mapblockFile,'r')
   for line in map_file.readlines():
      try:
          if line.strip():
           localblock=''
           globalblock=''  
           data=string.split(string.strip(line),'global:')
           if data != '\n':
 	     localblock=string.split(data[0],'local:')[1].strip()
	     globalblock=data[1].strip()
            #print "====  localblock: xx %s xx globalblock: xx %s xx"%(localblock,globalblock)
      except:
          print "ERROR: \n The File %s content must have lines in the form: \n local:<oldfileblockname> global:<newfileblockname> "%expand_mapblockFile
          sys.exit(1)

      UploadBlock(localblock,globalblock)

