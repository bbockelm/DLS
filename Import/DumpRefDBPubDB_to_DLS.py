#!/usr/bin/env python

import sys, os, string, re, getopt
import exceptions
import urllib, urllister
import urllib2
from unserializePHP import *

import logging

# #########################################################################
def usage():
  """
  print the command line and exit
  """
  print " "
  print 'usage: ',sys.argv[0],' <options>'
  print "Options"
  print "-h,--help \t\t Show this usage"
  print "-d,--debug \t\t set debug logging"
  print "-t,--typedls <type> \t type of DLS client (lfc or proto)"
  print "-c,--clientdir <dir>   For example /bohome/fanfani/LFC/COMP/DLS/Client/LFCClient , /bohome/fanfani/COMP/DLS/Client/SimpleClient \n"
# #########################################################################
def checkPubDBversion(baseurl):
  """
  check pubDB version 
  """
  newversion=1;
  
  try:
      #print ' Accessing '+baseurl+'pubdb-get-version.php'
      v = urllib2.urlopen(baseurl+'pubdb-get-version.php')
  except urllib2.URLError, msg:
          newversion=0;
  except urllib2.HTTPError, msg:
          pass

  return newversion

# #########################################################################
def getPubDBV3Info(baseurl,collid):
  """
  get PubDB info
  """
  try:
        #print "getPubDBV3Info: "+baseurl+'get-pubdb-analysisinfo.php?CollID='+collid
        url=baseurl+'get-pubdb-analysisinfo.php?CollID='+collid
        f = urllib.urlopen(url)
  except IOError:
        raise PubDBError(url,' urlopen fails')

  data = f.read()
  reCE=re.compile(r'CE=(\w+)')
  reSE=re.compile(r'SE=(\w+)')   
  CE=reCE.search(data).group(1)
  SE=reSE.search(data).group(1)
  #print " CE ",CE
  #print " SE ",SE
## ERRORE: the regexp do not pick up the domain
  return CE

# #########################################################################
def getPubDBInfo(baseurl,collid): 
  """
  get PubDB info
  """
  try:
        #url=baseurl+'pubdb-get-analysisinfo.php?dataset='+ds+'&owner='+ow
        url=baseurl+'pubdb-get-analysisinfo.php?collid='+collid
        f = urllib.urlopen(url)
  except IOError:
        raise PubDBError(url,' urlopen fails')

  data = f.read()
  if len(data)>0:
    if data[0]=='<':
        PubDBError(url,' Wrong output ')
        return 'Null'
    if data[0]=='\n':
      PubDBError(url,' Wrong output ')
      return 'Null'
    if string.find(data,'not defined') > 0 :
      PubDBError(url,' Not published dataset/owner ')
      return 'Null'
    if string.find(data,'such colection collid:') > 0 :
      PubDBError(url,' Not published collection ')
      return 'Null'

  else:
     raise PubDBError(url,' No output ')
  try:
        catalogues = PHPUnserialize().unserialize(data)
  except IOError,Exception:
         PHPUnserializeError(data)
         return 'Null'
  try:
        catinfos=[]
        collmap={}
        for k in catalogues.keys():
           ## get collection ID and type
           CollId=catalogues[k]['CollectionId']
           CollType=catalogues[k]['CollectionType']
           #print ">>> Catalogues for Collection: "+CollId+"\n"

           cat=catalogues[k]['Catalogue']
           for kcat in cat.keys():
              if cat[kcat]:
                #print ("AFAF key %s, val %s" %(kcat,cat[kcat]))
                ce=cat[kcat]['CEs']
                CElist=[]
                for kce in ce.keys():
                   ##print ("key %s, val %s" %(kce,ce[kce]))
                   CE=ce[kce]
                   CElist.append(ce[kce])
                #print " CE list :"
                for aCE in CElist:
                  #print " CE : "+aCE
                  logging.debug(" CE : "+aCE)
                cc=cat[kcat]['CatalogueContents']
                for kcc in cc.keys():
                   ##print ("key %s, val %s" %(kcc,cc[kcc]))
                   SE=cc[kcc]['SE']
                   logging.debug("SE: "+SE) 

  except IOError,Exception:
       PHPUnserializeError(data)
       return 'Null'

  return CElist
#  return CElist[0]


# ########################################################################
def getDataFromRefDB2PubDBs(br_filename):
  """
  look for published data in RefDB+PubDBs 
  and write a file with block:location
  """
  ## get the content of the RefDB2PubDBs map page
  PubDBCentralUrl_ = 'http://cmsdoc.cern.ch/cms/production/www/PubDB/'
  RefDBPubDBsmapPhp_ = 'GetPublishedCollectionInfoFromRefDB.php'

  primaryUrl=PubDBCentralUrl_+RefDBPubDBsmapPhp_
  logging.debug("MapListUrl="+ primaryUrl)

  try:
      sock = urllib.urlopen(primaryUrl)
  except IOError:
      raise RefDBError(primaryUrl)
 
  parser = urllister.URLLister()
  parser.feed(sock.read())
  sock.close()
  parser.close()

  ##  HREF dataset-discovery.php  (for a dataset/owner)
  ##  HREF PubDBURL for collecitons.php
  ##  HREF mantainer

  ## parsing the RefDB2PubDBs map to find the published dataset/owner and their PubDB URL

  List_blockreplica=[]
  br_file = open(br_filename,'w')
  ## regexp to get the dataset/owner
  reDsOw = re.compile(r'DSPattern=(\w+)&OwPattern=(\w+)')
  reDsOw2 = re.compile(r'DSPattern=(\w+\-\w+)&OwPattern=(\w+)')
  reDsOw3 = re.compile(r'DSPattern=(\w+\.\w+\.\w+)&OwPattern=(\w+)')
  ## mu04_mu_pt10._eta0.0
  recollid= re.compile(r'collid=(\d+)')
  count=0
  for url in parser.linksList:
    count=count+1
    if string.find(url, 'dataset-discovery.php') != -1 :
     #`print "trying with url  "+url
     try:
      dsow=reDsOw.search(string.strip(url))
      ds=dsow.group(1)
      ow=dsow.group(2)
     except:
      try:
       dsow=reDsOw2.search(string.strip(url))
       ds=dsow.group(1)
       ow=dsow.group(2)
       pass
      except: 
       dsow=reDsOw3.search(string.strip(url))
       ds=dsow.group(1)
       ow=dsow.group(2)
       pass
     ## PubDB URL is the next URL 
     puburl=parser.linksList[count]
     #print "Dataset/Owner is "+ds+"/"+ow+" PubDB URL is "+puburl
     logging.debug("Dataset/Owner is "+ds+"/"+ow+" PubDB URL is "+puburl)
     end=string.rfind(puburl,'/')
     collid=recollid.search(string.strip(puburl)).group(1)

     ## get location from PubDB 
     if (checkPubDBversion(puburl[:end+1])):
      ## for PubDB V4
        locationList=getPubDBInfo(puburl[:end+1],collid)
     else:
      ## for PubDB V3
       collid=recollid.search(string.strip(puburl)).group(1)
       #print ' collid '+collid+' Do nothing for PubDB V3....'
       PubDBError(puburl,' still PubDB V3') 
       locationList=['Null']

     ## save block-replica to a file
     block=ow+'/'+ds
     for location in locationList:
      blockreplica=block+':'+location
      List_blockreplica.append(blockreplica)
      br_file.write(blockreplica+'\n')

  br_file.close()

# ####################################
class PubDBError:
  def __init__(self, url, reason):
    #print 'ERROR accessing PubDBURL '+url+' reason: '+reason
    logging.error('accessing PubDBURL '+url+' reason: '+reason)
    pass
                                                                                                     
# ####################################
class RefDBError:
  def __init__(self, url):
    #print '\nERROR accessing URL '+url+'\n'
    logging.error('accessing URL '+url+'\n')
    pass

# ####################################
class DLSproto:
        """
        interface to DLS prototype instance
        """
        def __init__(self):
            self.serverhost='lxgate10.cern.ch'
            self.serverport='18081'
        def add(self,clientdir,block,location):
            if os.path.exists(clientdir+'/dls-add-replica'):
              add_cmd=clientdir+'/dls-add-replica --datablock '+block+' --se '+location+' --host '+self.serverhost+' --port '+self.serverport
              logging.debug(add_cmd)
              os.system(add_cmd)
            else:
              logging.error('No DLS CLI %s/dls-add-replica found'%clientdir)
 
# ####################################
class DLSlfc:
        """
        interface to DLS-LFC instance
        """
        def __init__(self):
            self.serverhost='lfc-cms.cern.ch'
            self.serverhome='/grid/cms/DLS/LFCProto'
            os.putenv("LFC_HOST",self.serverhost)
            os.putenv("LFC_HOME",self.serverhome)
            os.putenv("LCG_CATALOG_TYPE","lfc")
                                                                                                     
        def add(self,clientdir,block,location):
            if os.path.exists(clientdir+'/dlslfc-add'):
              add_cmd=clientdir+'/dlslfc-add -p '+block+' '+location
              logging.debug(add_cmd)
              os.system(add_cmd)
            else: 
              logging.error('No DLS CLI %s/dlslfc-add found'%clientdir)                                                                                         
#######################################################
if __name__ == '__main__':
     """
     Script to extract info from RefDB/PubDBs an fill DLS
     """
     long_options=["help","typedls=","clientdir=","debug"]
     short_options="ht:c:d"
     try:
         opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
     except getopt.GetoptError:
         usage()
         print "error defining the options"
         sys.exit(1)
                                                                                                     
     if len(opts)<1:
         usage()
         sys.exit(1)
                                                                                                     
     dlstype='proto'
     loglevel=logging.INFO
     clientdir=None
                                                                                                     
     for o, a in opts:
            if o in ("-h", "--help"):
                usage()
                sys.exit(1)
            if o in ("-t", "--typedls"):
                dlstype=a
            if o in ("-c", "--clientdir"):
                clientdir=a
                if not os.path.exists(clientdir):
                 print "Error: --clientdir %s is not there"%clientdir
                 sys.exit(1)

            if o in ("-d", "--debug"):
                loglevel=logging.DEBUG
                                                                                                     
     if dlstype==None:
            usage()
            print "Error: --dlstype <lfc or proto> is required"
            sys.exit(1)
     if clientdir==None:
            usage()
            print "Error: --clientdir <DLS clientdir> is required"
            sys.exit(1)

 
     ## Set log file
     logfilename='DumpRefDBPubDBs_toDLS%s.log'%dlstype
     logging.basicConfig(level=loglevel,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfilename,
                    filemode='w')

     ### Extract info from RefDB/PubDBs
     logging.info('Start extracting info from RefBD/PubDBs')
     blockreplica_file='blockreplica.txt'
     getDataFromRefDB2PubDBs(blockreplica_file)
     logging.info('End extracting info from RefBD/PubDBs')
     logging.info('Extracted info are in file: '+blockreplica_file)

     ### Fill the DBS 
     # select the DLS server
     if (dlstype=='lfc'):
       dls = DLSlfc()
     elif (dlstype=='proto'):
       dls = DLSproto()

     #TEST: blockreplica_file="blockreplica.txt.one"     
     logging.info('Start filling DLS of type: '+dlstype+' reading file: '+blockreplica_file)                                                                                                
     br_file_r = open(blockreplica_file,'r')
     for line in br_file_r.readlines():
       blockreplica=string.split(string.strip(line),':')
       block=blockreplica[0]
       location=blockreplica[1]
       if ( location != 'Null' ):
         dls.add(clientdir,block,location)
                                                                                                     
     br_file_r.close()
     logging.info('End filling DLS of type: '+dlstype) 
                                                                                                     
     sys.exit(0)
