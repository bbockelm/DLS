#
# $Id: dlsPhedexApi.py,v 1.2 2008/05/09 15:29:17 delgadop Exp $
#
# DLS Client. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server embedded in the CMS Physicx Experimental Data Export
 service (PhEDEx).

 With PhEDEx as back-end, the namespace of fileblocks is shared between
 DLS and PhEDEx, and since it makes more sense that this is handled through
 PhEDEx interfaces, some of the methods of the DLS API are not implemented
 or present a limited functionality in this implementation. For the moment,
 only read operations on the catalog are supported.
 
 The module contains the DlsPhedexApi class that implements most of the
 methods defined in dlsApi.DlsApi class and a couple of extra convenient
 (implementation specific) methods. Python applications interacting with
 a PhEDEx-embedded DLS will instantiate a DlsPhedexApi object and use its
 methods.
"""

#########################################
# Imports 
#########################################
import dlsApi
from dlsApiExceptions import *
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry, DlsDataObjectError
# TODO: From what comes next, should not import whole modules, but what is needed...
import warnings
warnings.filterwarnings("ignore","Python C API version mismatch for module _lfc",RuntimeWarning)
import sys
import commands
import time
import getopt
from os import environ
from stat import S_IFDIR
from dlsXmlParser import DlsXmlParser
from xml.sax import SAXException
from urllib2 import HTTPError, URLError

#########################################
# Module globals
#########################################
DLS_PHEDEX_BLOCKS = "DLS_PHEDEX_BLOCKS"
DLS_PHEDEX_FILES = "DLS_PHEDEX_FILES"
DLS_PHEDEX_ALL_LOCS = "DLS_PHEDEX_ALL_LOCS"


#########################################
# DlsPhedexApiError class
#########################################
# We no longer define our own exceptions, but use those of dlsApi.


#########################################
# DlsDbsApi class
#########################################

class DlsPhedexApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on DLS information
  being embedded in the CMS Physicx Experimental Data Export service (PhEDEx).

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsApiError.
  """

  def __init__(self, dls_endpoint=None, verbosity=dlsApi.DLS_VERB_WARN, **kwd):
    """
    Constructor of the class. It creates a DlsXmlParser object using the
    specified dls_endpoint and optionally additional parameters in **kwd. 
    It also sets the verbosity of the DLS API.
    
    At the time of writing, the minimum argument required to build the DBS
    interface is the DBS server endpoint. For others, please check DBS
    client documentation.
    
    The server endpoint is got from a string in the URL form, usually:
    "http[s]://hname[:port]/path/to/DLS". This API tries to retrieve that
    value from several sources (in this order):   
         - specified dls_endpoint 
         - DLS_ENDPOINT environmental variable
         - DLS_PHEDEX_ENDPOINT environmental variable
         - DLS catalog advertised in the Information System (if implemented)
         
    If the necessary arguments cannot be obtained in any of these ways, the
    instantiation is denied and a DlsConfigError is raised.
 
    The verbosity level affects invocations of all methods in this object.
    See the dlsApi.DlsApi.setVerbosity method for information on accepted values.

    If the checkEndpoint (**kwd) is set to True, the provided endpoint is
    checked. This makes sense where more than one query are to be made next.
    For simple queries, any error in the endpoint will be noticed in the query
    itself, so the check would be redundant and not efficient.
      
    @exception DlsConfigError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string "hname[:port]/path/to/DLS"
    @param verbosity: value for the verbosity level
    @param kwd: Flags 
       - checkEndpoint: Boolean (default False) for testing of the DLS endpoint
    """

    # Keywords
    checkEndpoint = False
    if(kwd.has_key("checkEndpoint")):
       checkEndpoint = kwd.get("checkEndpoint")

    # Let the parent set the server endpoint (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)

    # If the server is not there yet, try from LFC_HOST
    if(not self.server):
      self.server = environ.get("DLS_PHEDEX_ENDPOINT")

    # If still not there, give up 
    if(not self.server):
       raise DlsConfigError("Could not set the DLS server to use")

    self.server = self.server.strip('/')
  
    # Create the parser
    self.parser = DlsXmlParser()

    # Check that the provided URL is OK (by listing an inexisting fileblock)
    if(checkEndpoint):
      try:
         url = self._buildXmlUrl(self.server, DLS_PHEDEX_BLOCKS, "-")
         self.parser.xmlToEntries(url)
      except Exception, inst:
         msg = "Could not set the interface to the DLS server. "
         msg += "Error when accessing provided PhEDEx URL: %s" %   (self.server)
         self._mapException(inst, msg, msg, errorTolerant = False)

    

  ############################################
  # Methods defining the main public interface
  ############################################

  
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If longList (**kwd) is set to True, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
     - bytes
     - files
     - node
     - node_id
     - time_create
     - time_update
     - complete

    If instead of a FileBlock name, a pattern is provided (with '%' or '*' as
    wildcards), the method includes in the returned list the DlsEntry objects
    for all the matching FileBlocks.

    In the current implementation the cost of doing a long listing
    is the same as doing a normal one.

    The showProd flag is taken into account and if not set to True some 
    FileBlock replicas are filtered out.

    The following keyword flags are ignored: session.
    """
    # Keywords
    longList = False 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    errorTolerant = False
    if(kwd.has_key("errorTolerant")):   errorTolerant = kwd.get("errorTolerant")

    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]

    eList = []

    # Loop on the entries
    for fB in theList:
       # Check what was passed (DlsFileBlock or string)
       if(isinstance(fB, DlsFileBlock)):
         lfn = fB.name
       else:
         lfn = fB

       # If '/' is given, we want all blocks back
       if(lfn=='/'): lfn = None

       # Build the xml query to use
       url = self._buildXmlUrl(self.server, DLS_PHEDEX_BLOCKS, lfn, showProd = showProd)
       self._debug("Using PhEDex xml url: "+url)

       # Get the locations
       try:  
          partList = self.parser.xmlToEntries(url)
       except Exception, inst:
          msg = "Error retrieving locations for %s" % (lfn)
          msg_w = msg + ". Skipping"
          self._mapException(inst, msg, msg_w, errorTolerant)
       
       # Check if the list was empty
       if(not partList):
          if(not lfn): lfn = '/'
          msg = "No existing fileblock matching %s" % (str(lfn))
          msg_w = msg + ". Skipping"
          if(not errorTolerant):  raise DlsInvalidBlockName(msg)
          else:                   self._warn(msg_w)
       else:
          for entry in partList:
             eList.append(entry)
         
    # Return what we got
    return eList



  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The showProd flag is taken into account and if not set to True some 
    FileBlock replicas are filtered out.

    The following keyword flags are ignored: session.
    """

    # Keywords
    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")

    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList 
    else:
       theList = [locationList]

    eList = []
    
    # Loop on the entries
    for loc in theList:
       
       # Check what was passed (DlsLocation or string)
       if(isinstance(loc, DlsLocation)):
         host = loc.host
       else:
         host = loc

       # Build the xml query to use
       url = self._buildXmlUrl(self.server, DLS_PHEDEX_BLOCKS, None, host, showProd = showProd)
       self._debug("Using PhEDex xml url: "+url)

       # Get the locations
       try:  
          partList = self.parser.xmlToEntries(url)
       except Exception, inst:
          msg = "Error retrieving FileBlocks for %s" % (host)
          self._mapException(inst, msg, msg, False)

       for entry in partList:
          eList.append(entry)
         
    # Return what we got
    return eList



  def listFileBlocks(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.listFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx FileBlock namespace is not hierarchical, there is no concept
    of FileBlock directory, and recursive listing makes no sense.

    If instead of a FileBlock name, a pattern (with '%' as wildcard) is provided,
    the method includes in the returned list the DlsEntry objects for all the
    matching FileBlocks.

    If longList (**kwd) is set to True, the attributes returned with
    the FileBlock are the following:
      - bytes
      - files 
      - is_open
      - id

    The following keyword flags are ignored: session, recursive.
    """
    # Keywords
    longList = False 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]

    bList = []

    # Loop on the entries
    for fB in theList:
       # Check what was passed (DlsFileBlock or string)
       if(isinstance(fB, DlsFileBlock)):
         lfn = fB.name
       else:
         lfn = fB

       # If '/' or '*' or '%' is given, we want all blocks back
       if (lfn=='/') or (lfn == '*') or (lfn == '%'):
          lfn = '/%'

       # Build the xml query to use
       url = self._buildXmlUrl(self.server, DLS_PHEDEX_BLOCKS, lfn)
       self._debug("Using PhEDex xml url: "+url)

       # Get the locations
       partList = []
       try:  
          partList = self.parser.xmlToBlocks(url)
       except Exception, inst:
          msg = "Error retrieving fileblock information for %s" % (lfn)
          msg_w = msg + ". Skipping"
          self._mapException(inst, msg, msg_w, errorTolerant = True)
       
       # Check if the list was empty
       if(not partList):
          if(not lfn): lfn = '/'
          msg = "No existing fileblock matching %s" % (str(lfn))
          msg_w = msg + ". Skipping"
#          if(not errorTolerant):  raise DlsInvalidBlockName(msg)
#          else:                   self._warn(msg_w)
          self._warn(msg_w)
       else:
          for entry in partList:
             bList.append(entry)
         
    # Return what we got
    return bList



  def getFileLocs(self, fileblock, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileLocs method.
    Refer to that method's documentation.

    Implementation specific remarks:
    
    The showProd flag is taken into account and if not set to True some 
    file replicas are filtered out.

    The following keyword flags are ignored: session.
    """
    
    # Keywords
    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")

    # Check that the passed FileBlock is not a pattern
    if (fileblock.find('*') != -1) or (fileblock.find('%') != -1):
      msg = "FileBlock patterns (with '*' or '%%' wildcards) are not acceptable: %s" % (fileblock)
      raise DlsInvalidBlockName(msg)

    # Build the xml query to use
    url = self._buildXmlUrl(self.server, DLS_PHEDEX_FILES, fileblock, showProd = showProd)
    self._debug("Using PhEDex xml url: "+url)

    # Get the file-locs dict
    try:  
       flDict = self.parser.xmlToFileLocs(url)
    except Exception, inst:
       msg = "Error getting files for FileBlock in DLS"
       self._mapException(inst, msg, msg, errorTolerant = False)

    # Return what we got
    return flDict




  def getAllLocations(self, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getAllLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The following keyword flags are ignored: session.
    """
 
    # Build the xml query to use
    url = self._buildXmlUrl(self.server, DLS_PHEDEX_ALL_LOCS)
    self._debug("Using PhEDex xml url: "+url)

    # Get the locations
    try:  
       locList = self.parser.xmlToLocations(url)
    except Exception, inst:
       msg = "Error getting all locations in DLS"
       self._mapException(inst, msg, msg, errorTolerant = False)

    # Return what we got
    return locList


  def dumpEntries(self, dir = "/", **kwd):
    """
    Implementation of the dlsApi.DlsApi.dumpEntries method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx FileBlock namespace is not hierarchical, there is no concept
    of FileBlock directory, and recursive listing makes no sense. The dir
    argument is interpreted as representing a FileBlock name pattern (with
    '%' as wildcard), and the matching FileBlocks and associated locations
    are dumped.

    The showProd flag is taken into account and if not set to True some 
    FileBlock replicas are filtered out.

    The following keyword flags are ignored: session, recursive.
    """

    # Keywords
    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")


    # This can be achieved by listing the fBs and associated locations
    result = self.getLocations(dir, longList = False, errorTolerant = True, showProd = showProd)

    # Return what we got
    return result



  def startSession(self):
    """
    Implementation of the dlsApi.DlsApi.startSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    Since PhEDEx does not support sessions, this method does nothing.
    """
    self._debug("Starting session with %s (no action)" % (self.server))

 
  def endSession(self):
    """
    Implementation of the dlsApi.DlsApi.endSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    Since PhEDEx does not support sessions, this method does nothing.
    """
    self._debug("Ending session with %s (no action)" % (self.server))
  
 
  def startTrans(self):
    """
    Implementation of the dlsApi.DlsApi.startTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx does not support transactions, this method does nothing.
    """
    self._debug("Starting transaction with %s (no action)" % (self.server))


  def endTrans(self):
    """
    Implementation of the dlsApi.DlsApi.endTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx does not support transactions, this method does nothing.
    """
    self._debug("Ending transaction with %s (no action)" % (self.server))
  
  
  def abortTrans(self):
    """
    Implementation of the dlsApi.DlsApi.abortTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx does not support transactions, this method does nothing.
    """
    self._debug("Aborting transaction with %s (no action)" % (self.server))
 

  
  ##################################
  # Private methods
  ##################################

  def _buildXmlUrl(self, xml_base, type, block=None, se=None, **kwd):
    """
    Returns an appropriate URL which queries the PhEDEx FileBlock location
    information with the specified parameters.

    @param xml_base: base URL for the PhEDEx location query service
    @param type: the type of query to make: DLS_PHEDEX_BLOCKS, DLS_PHEDEX_FILES, DLS_PHEDEX_ALL_LOCS
    @param block: name of the FileBlock to query (or add); wildcard '%' or '*' allowed
    @param se:  name of the location to query (or add); use None for 'any'
    @param **kwd: Flags:
      - incomplete: boolean (default False) for getting incomplete blocks also returned 
      - updated_since: unix timestamp, for replicas updated since specified time
      - created_since: unix timestamp, for replicas cretated since specified time
      - showProd: boolean (default False) for turning off the filtering of prod-only replicas
      - showEmpty: boolean (default False) for showing empty locations (DLS_PHEDEX_ALL_LOCS)
    """

    admitted_vals = [DLS_PHEDEX_BLOCKS, DLS_PHEDEX_FILES, DLS_PHEDEX_ALL_LOCS]
    if(type not in admitted_vals):
      msg = "Error building the PhEDEx xml url."
      msg += "The specified type of query is not one of the admitted values"
      raise DlsValueError(msg)

    url = xml_base
    
    # Most common case (querying blocks or locations)
    if(type == DLS_PHEDEX_BLOCKS):
    
       url += "/blockReplicas?"
       
       if(not (block or se)): 
         msg = "Error building the PhEDEx xml url. A FileBlock or a location must be specified"
         raise DlsArgumentError(msg)

       if(block):
         url += "block=" + block.replace('*','%')
         url = url.replace('#','%23')
       
       if(se):
          if(block): url += "&"
          url += "se="+se
   
       if not (kwd.has_key("incomplete") and kwd.get("incomplete")):
          url += "&complete=y"
   
       if(kwd.has_key("updated_since")):
          url += "&updated_since="+kwd.get("updated_since")
       
       if(kwd.has_key("created_since")):
          url += "&created_since="+kwd.get("created_since")
 
       if not (kwd.has_key("showProd") and kwd.get("showProd")):
          url += "&op=node:and&node=!T0*&node=!T1*"

    # Query for individual files in a given block 
    if(type == DLS_PHEDEX_FILES):
       url += "/fileReplicas?"

       if(not (block)): 
         msg = "Error building the PhEDEx xml url. A FileBlock must be specified"
         raise DlsArgumentError(msg)
       else:
         url += "block=" + block.replace('*','%')
         url = url.replace('#','%23')
       
       if(se):
          if(block): url += "&"
          url += "se="+se
   
       if not (kwd.has_key("incomplete") and kwd.get("incomplete")):
          url += "&dist_complete=y"
#          url += "&complete=y"
   
       if(kwd.has_key("updated_since")):
          url += "&updated_since="+kwd.get("updated_since")
       
       if(kwd.has_key("created_since")):
          url += "&created_since="+kwd.get("created_since")
 
       if not (kwd.has_key("showProd") and kwd.get("showProd")):
          url += "&op=node:and&node=!T0*&node=!T1*"


    # Query for all locations
    if(type == DLS_PHEDEX_ALL_LOCS):

       url += "/nodes"

       if not (kwd.has_key("showEmpty") and kwd.get("showEmpty")):
          url += "?noempty=y"


    return url
       
   
  def _mapException(self, inst, excp_msg, warn_msg, errorTolerant=False):
    """
    If errorTolerant==False, analyzes the passed exception and raises the 
    appropriate corresponding DlsApiError exception including the message
    excp_msg. If errorTolerant==True, just prints the passed warn_msg using
    the _warn function.

    @param inst: the Exception object to be analyzed
    @param excp_msg: the message passed to the exception in creation
    @param warn_msg: the message to print as warning
    @param errorTolerant: boolean to control operation 

    @exception: raises the appropriate DlsApiError, if errorTolerant==False
    """

    caught_msg = str(inst) 

    if(not errorTolerant):
       excp_msg = excp_msg + '. Caught: %s' % (caught_msg)
      
       if (isinstance(inst, SAXException)):
         excp_msg = "Error parsing server reply. " + excp_msg
         raise DlsErrorWithServer(excp_msg)
         
       if (isinstance(inst, HTTPError) or (isinstance(inst, URLError))):
         excp_msg = excp_msg + ' Server endpoint: ' + self.server
         raise DlsConnectionError(excp_msg)
         
       # Otherwise, we raise the default exception
       raise DlsErrorWithServer(excp_msg)
       
    else:
       warn_msg = warn_msg + '. Caught: %s' % (caught_msg)
       self._warn(warn_msg)


