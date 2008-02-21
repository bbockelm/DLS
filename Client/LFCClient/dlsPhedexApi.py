#
# $Id$
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
DLS_PHEDEX_LIST = "DLS_PHEDEX_LIST"
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

    If the checkEndpoint (**kwd) is set to False, the provided endpoint is
    not checked. This makes sense where a simple query is to be made next.
    Any error in the endpoint will be noticed in the query itself, so the
    check would be redundant and not efficient.
      
    @exception DlsConfigError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string "hname[:port]/path/to/DLS"
    @param verbosity: value for the verbosity level
    @param kwd: Flags 
       checkEndpoint: Boolean (default True) for testing of the DLS endpoint
    """

    # Keywords
    checkEndpoint = True
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
         url = self._buildXmlUrl(self.server, DLS_PHEDEX_LIST, "-")
         self.parser.xmlToEntries(url)
      except Exception, inst:
         msg = "Could not set the interface to the DLS server. "
         msg += "Error when accessing provided PhEDEx URL: %s" %   (self.server)
         self._mapException(inst, msg, msg, errorTolerant = False)

    

  ############################################
  # Methods defining the main public interface
  ############################################

#  def add(self, dlsEntryList, **kwd):
#    """
#    Implementation of the dlsApi.DlsApi.add method.
#    Refer to that method's documentation.
#
#    Implementation specific remarks:
#
#    Fileblocks in PhEDEx can be created only through PhEDEx interface, not through 
#    DLS API. Thus, this method can be used to add locations to an existing fileblock
#    only. Trying to add locations to a non-existing fileblock will result
#    in a dlsApiError exception.
#
#    Likewise, fileblock attributes are handled by DBS, and they can't be set 
#    or updated through DLS. Therefore, this method will ignore any specified
#    FileBlock attribute.
#    
#    The list of supported attributes for the locations is:
#     - custodial  (values: "True" or "False"; if not specified, False is assumed)
#
#    Specified GUID for the FileBlocks and SURL for the replicas are ignored.
#    
#    The following keyword flags are ignored: allowEmptyBlocks, createParent,
#    trans, session.
#    """
#
#    # Keywords
#    checkLocations = True
#    if(kwd.has_key("checkLocations")):
#       checkLocations = kwd.get("checkLocations")       
#       
#    errorTolerant = True 
#    if(kwd.has_key("errorTolerant")):
#       errorTolerant = kwd.get("errorTolerant")
#    
#    # Make sure the argument is a list
#    if (isinstance(dlsEntryList, list)):
#       theList = dlsEntryList 
#    else:
#       theList = [dlsEntryList]
#
#
#    # Loop on the entries
#    for entry in theList:
#    
#      # First check locations if asked for (not to leave empty blocks)
#      if(checkLocations):
#         locList = []
#         exitLoop = False
#         for loc in entry.locations:
#            try:
#               loc.checkHost = True
#               loc.host = loc.host
#               locList.append(loc)
#            except DlsDataObjectError, inst:
#               msg = "Error in location %s for "%(loc.host)
#               msg += "FileBlock %s: %s" % (entry.fileBlock.name, inst.msg)
#               self._warn(msg)
#               if(not errorTolerant):
#                  raise DlsInvalidLocation(msg)
#      else:
#         locList = entry.locations
#
#      # Skip empty fileBlocks
#      if(not locList):  
#         self._warn("No locations for fileblock %s. Skipping." % entry.fileBlock.name)
#         continue
#
#
#      # Add locations
#      for loc in locList:
#         dbsSE = self._mapLocToDbs(loc)
#         self._debug("dbs.addReplicaToBlock(%s,%s)" % (entry.fileBlock.name, loc))
#         try:
#            self.dbsapi.addReplicaToBlock(entry.fileBlock.name, dbsSE)
#         except DbsApiException, inst:
#            msg = "Error inserting locations for fileblock %s" % (entry.fileBlock.name)
#            w_msg = msg + ". Skipping"
#            self._mapException(inst, msg, w_msg, errorTolerant)


# There are no attributes to update implemented yet in DBS
# but the code should be more or less what follows

#  def update(self, dlsEntryList, **kwd):
#    """
#    Implementation of the dlsApi.DlsApi.update method.
#    Refer to that method's documentation.
#
#    Implementation specific remarks:
#
#    For a given FileBlock, specified locations that are not registered in the
#    catalog will be ignored.
#
#    There are no FileBlock attributes that can be updated (that should be made
#    through the DBS interface).
#    
#    The list of supported attributes for the locations is:
#     - custodial  (values: "True" or "False"; if not specified, False is assumed)
#
#    The following keyword flags are ignored: trans, session.
#    """
#
#    # Keywords
#    errorTolerant = True 
#    if(kwd.has_key("errorTolerant")):
#       errorTolerant = kwd.get("errorTolerant")
#       
#    # Make sure the argument is a list
#    if (isinstance(dlsEntryList, list)):
#       theList = dlsEntryList 
#    else:
#       theList = [dlsEntryList]
#
#
#    # Loop on the entries
#    for entry in theList:
# 
#      # Loop on the locations
#      for loc in entry.locList:
#      
#         # Prepare the DBS SE object
#         dbsSE = self._mapLocToDbs(loc)
#
#         # Skip replicas with no attribute to update
#         if(not dbsSE["custodial"]):
#           continue 
# 
#         #  Update
#         self._debug("dbs.updateStorageElement(%s,%s)" % (entry.fileBlock.name, loc.host))
#         try:
#            self.dbsapi.updateStorageElement(entry.fileBlock.name, dbsLoc)
#         except DbsApiException, inst:
#            if(not errorTolerant):
#              # TODO, analyze DBS exception
#              raise DlsApiError(inst.getErrorMessage())
#            else:
#              # For FileBlocks not accessible, go to next fileblock
#              # TODO: check what DBS exception we should be catching here
#              if(isinstance(inst, NotAccessibleError)):
#                 self._warn("Not updating inaccessible FileBlock: %s" % (inst.msg))
#                 break 
#              # For error on attributes, just warn and go on
#              else:
#                 self._warn("Error when updating location: %s" % (inst.msg))
#
#
    
#  def delete(self, dlsEntryList, **kwd):
#    """
#    Implementation of the dlsApi.DlsApi.delete method.
#    Refer to that method's documentation.
#
#    Implementation specific remarks:
#
#    This method will only delete locations for a five FileBlock, but not
#    the FileBlock itself (even if all the replicas are removed). With DBS
#    back-end, the FileBlock itself might be deleted (or invalidated) through
#    DBS interface only, but not through DLS interface.
#    
#    The following keyword flags are ignored: keepFileBlock, session, 
#    """
#
#    # Keywords
#    force = False 
#    if(kwd.has_key("force")):    force = kwd.get("force")
#       
#    all = False 
#    if(kwd.has_key("all")):      all = kwd.get("all")
#
#    errorTolerant = True 
#    if(kwd.has_key("errorTolerant")):  errorTolerant = kwd.get("errorTolerant")
#
#       
#    # Make sure the argument is a list
#    if (isinstance(dlsEntryList, list)):
#       theList = dlsEntryList 
#    else:
#       theList = [dlsEntryList]
#
#    # Loop on the entries
#    for entry in theList:
#      
#      # Get the FileBlock name
#      lfn = entry.fileBlock.name
#
#      # Get the specified locations
#      seList = []
#    # TODO: what if attributes are not all string
#      if(not all):
#         for loc in entry.locations:
#            seList.append(loc.host)
#
#
#      # Retrieve the existing associated locations (from the catalog)      
#      locList = []
#      entryList = self.getLocations(lfn, errorTolerant = errorTolerant)
#      for i in entryList:
#          for j in i.locations:
#              locList.append(j)
#     
#      # Make a copy of location list (to keep control of how many are left)
#      remainingLocs = []
#      for i in xrange(len(locList)):
#         remainingLocs.append(locList[i].host)
#         
#      # Loop on associated locations
#      for filerep in locList:
#
#         host = filerep.host
#      
#         # If this host (or all) was specified, remove it
#         if(all or (host in seList)):
#         
#            # Don't look for this SE further
#            if(not all): seList.remove(host)
#            
##            # But before removal, check if it is custodial
##            if ((filerep["custodial"]!="True") and (not force)):
##               code = 503
##               msg = "Can't delete custodial replica in",host,"of",lfn
##               if(not errorTolerant): 
##                  raise DlsApiError(msg, code)
##               else: 
##                  self._warn(msg)
##                  continue
#               
#            # Perform the deletion
#            try:
#               self._debug("dbs.deleteReplicaFromBlock(%s, %s)" % (lfn, host))
#               self.dbsapi.deleteReplicaFromBlock(lfn, host) 
#               remainingLocs.remove(host)
#            except DbsApiException, inst:
#               msg = "Error removing location %s from FileBlock %s"%(host,lfn)
#               self._mapException(inst, msg, msg, errorTolerant)
#        
#            # And if no more SEs, exit
#            if((not all) and (not seList)):
#               break
#   
#      # For the SEs specified, warn if they were not all removed
#      if(seList and (not all)):
#         self._warn("Not all specified locations could be found and removed")
#  
  
  
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If longList (**kwd) is set to True, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
     - custodial 

    If instead of a FileBlock name, a pattern is provided (with '%' as wildcard),
    the method includes in the returned list the DlsEntry objects for all the
    matching FileBlocks.

    In the current implementation the cost of doing a long listing
    is the same as doing a normal one.

    The following keyword flags are ignored: session.
    """
    # Keywords
    longList = False 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    errorTolerant = False
    if(kwd.has_key("errorTolerant")):   errorTolerant = kwd.get("errorTolerant")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]


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
       url = self._buildXmlUrl(self.server, DLS_PHEDEX_LIST, lfn)
       self._debug("Using PhEDex xml url: "+url)

       # Get the locations
       try:  
          eList = self.parser.xmlToEntries(url)
       except Exception, inst:
          msg = "Error retrieving locations for %s" % (lfn)
          msg_w = msg + ". Skipping"
          self._mapException(inst, msg, msg_w, errorTolerant)
       
       # Check if the list was empty
       if(not eList):
          if(not lfn): lfn = '/'
          msg = "No existing fileblock matching %s" % (str(lfn))
          msg_w = msg + ". Skipping"
          if(not errorTolerant):  raise DlsInvalidBlockName(msg)
          else:                   self._warn(msg_w)
         
    # Return what we got
    return eList



  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The following keyword flags are ignored: session.
    """

    # Keywords

    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList 
    else:
       theList = [locationList]

    # Loop on the entries
    for loc in theList:
       
       # Check what was passed (DlsLocation or string)
       if(isinstance(loc, DlsLocation)):
         host = loc.host
       else:
         host = loc

       # Build the xml query to use
       url = self._buildXmlUrl(self.server, DLS_PHEDEX_LIST, None, host)
       self._debug("Using PhEDex xml url: "+url)

       # Get the locations
       try:  
          eList = self.parser.xmlToEntries(url)
       except Exception, inst:
          msg = "Error retrieving FileBlocks for %s" % (host)
          self._mapException(inst, msg, msg, False)
         
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
      - "bytes"
      - "files" 
      - "is_open"

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

       # If '/' is given, we want all blocks back
       if(lfn=='/'): lfn = None

       # Build the xml query to use
       url = self._buildXmlUrl(self.server, DLS_PHEDEX_LIST, lfn)
       self._debug("Using PhEDex xml url: "+url)

       # Get the locations
       try:  
          bList = self.parser.xmlToBlocks(url)
       except Exception, inst:
          msg = "Error retrieving fileblock information for %s" % (lfn)
          msg_w = msg + ". Skipping"
          self._mapException(inst, msg, msg_w, errorTolerant = True)
       
       # Check if the list was empty
       if(not bList):
          if(not lfn): lfn = '/'
          msg = "No existing fileblock matching %s" % (str(lfn))
          msg_w = msg + ". Skipping"
#          if(not errorTolerant):  raise DlsInvalidBlockName(msg)
#          else:                   self._warn(msg_w)
          self._warn(msg_w)
         
    # Return what we got
    return bList



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

    The following keyword flags are ignored: session, recursive.
    """

    # This can be achieved by listing the fBs and associated locations
    result = self.getLocations(dir, longList = False, errorTolerant = True)

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
  # Other public methods (utilities)
  ##################################

#  def changeFileBlocksLocation(self, org_location, dest_location, **kwd):
#    """
#    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
#    Refer to that method's documentation.
#    """
#
#    # Keywords
#    checkLocations = True
#    if(kwd.has_key("checkLocations")):
#       checkLocations = kwd.get("checkLocations")       
#
#    # First check the new location if asked for it
#    if(checkLocations):
#       try:
#          loc = DlsLocation(dest_location, checkHost = True)
#       except DlsDataObjectError, inst:
#          msg = "Error replacing location %s with %s: %s"%(org_location,dest_location,inst.msg)
#          raise DlsInvalidLocation(msg)
#
#    # Perform the replacement
#    self._debug("dbs.renameSE(%s, %s)"%(org_location, dest_location))
#    try:
#       self.dbsapi.renameSE(org_location, dest_location)
#    except DbsApiException, inst:
#       msg = "Error replacing location in DLS"
#       try:       
#         rc = int(inst.getErrorCode())
#       except Exception:
#         rc = 0
#       if(rc in [2000]):
#         msg += ". New location (%s) exists in DLS server already" % (dest_location)
#         caught_msg = inst.getClassName() + ' ' + inst.getErrorMessage()
#         msg = msg + '. Caught: [%d] %s' % (rc, caught_msg)
#         raise DlsInvalidLocation(msg, server_error_code=rc)
#         
#       self._mapException(inst, msg, msg, errorTolerant = False)
#


  ##################################
  # Private methods
  ##################################

  def _buildXmlUrl(self, xml_base, type, block=None, se=None, **kwd):
    """
    Returns an appropriate URL which queries the PhEDEx FileBlock location
    information with the specified parameters.

    @param xml_base: base URL for the PhEDEx location query service
    @param type: the type of query to make: DLS_PHEDEX_LIST
    @param block: name of the FileBlock to query (or add); wildcard '%' or '*' allowed
    @param se:  name of the location to query (or add); use None for 'any'
    @param **kwd: Flags:
      - complete: boolean (default True) for demanding that returned blocks are complete
      -  updated_since: unix timestamp, for replicas updated since specified time
      -  created_since: unix timestamp, for replicas cretated since specified time
    """

    admitted_vals = [DLS_PHEDEX_LIST, DLS_PHEDEX_ALL_LOCS]
    if(type not in admitted_vals):
      msg = "Error building the PhEDEx xml url."
      msg += "The specified type of query is not one of the admitted values"
      raise DlsValueError(msg)

    url = xml_base
    
    # Most common case (querying blocks or locations)
    if(type == DLS_PHEDEX_LIST):
    
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
   
       if(kwd.has_key("complete")):
          if(kwd.get("complete")): url += "&complete=y"
   
       if(kwd.has_key("updated_since")):
          url += "&updated_since="+kwd.get("updated_since")
       
       if(kwd.has_key("created_since")):
          url += "&created_since="+kwd.get("created_since")
  

    # Query for all locations
    if(type == DLS_PHEDEX_ALL_LOCS):
       url += "/nodes"


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


