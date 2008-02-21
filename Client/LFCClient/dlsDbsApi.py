#
# $Id: dlsDbsApi.py,v 1.4 2007/03/30 13:13:47 delgadop Exp $
#
# DLS Client. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server embedded in the CMS Dataset Bookkeeping System (DBS).

 With DBS as back-end, the namespace of fileblocks is shared between
 DLS and DBS, and since it makes more sense that this is handled through
 DBS interfaces (with information regarding datasets, etc.), some of
 the methods of the DLS API are not implemented or present a limited
 functionality in this implementation. For example, the FileBlocks
 themselves cannot be created through this API, and the
 renameFileBlock method is not implemented.

 Such particularities are documented in every method.
 
 The module contains the DlsDbsApi class that implements most of the
 methods defined in dlsApi.DlsApi class and a couple of extra convenient
 (implementation specific) methods. Python applications interacting with
 a DBS-embedded DLS will instantiate a DlsDbsApi object and use its
 methods.
"""

#########################################
# Imports 
#########################################
import dlsApi
#from dlsApi import DlsApiError
from dlsApiExceptions import *
#import dlsDliClient   # for a fast getLocations implementation
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
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsApiException import *
from DBSAPI.dbsException import *
from DBSAPI.dbsStorageElement import *
from DBSAPI.dbsFileBlock import *
#########################################
# Module globals
#########################################
#S_IFDIR = 0x4000

#########################################
# DlsDbsApiError class
#########################################
# We no longer define our own exceptions, but use those of dlsApi.


#########################################
# DlsDbsApi class
#########################################

class DlsDbsApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on DLS information
  being embedded in the CMS Dataset Bookkeeping System (DBS).

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsApiError.
  """

  def __init__(self, dls_endpoint=None, verbosity=dlsApi.DLS_VERB_WARN, **kwd):
    """
    Constructor of the class. It creates a DBS interface object using the
    specified dls_endpoint and optionally additional parameters in **kwd. 
    It also sets the verbosity of the DLS API.
    
    Any additional arguments for the DBS interface may be specified as part
    of **kwd. Otherwise they may be specified in a DBS configuration 
    file (see below). For every case, the arguments in the configuration
    file take less precedence.

    At the time of writing, the minimum argument required to build the DBS
    interface is the DBS server endpoint. For others, please check DBS
    client documentation.
    
    The server endpoint is got from a string in the URL form, usually:
    "http[s]://hname[:port]/path/to/DLS". This API tries to retrieve that
    value from several sources (in this order):   
         - specified dls_endpoint 
         - DLS_ENDPOINT environmental variable
         - specified URL argument (in **kwd)
         - specified URL in the configuration file (see below)
         - DLS catalog advertised in the Information System (if implemented)
         
    Any other DBS-specific parameter can be specified in the following ways
    (in order of precedence):
         - variable within **kwd (version, for the DBS client version)
         - variable within the DBS configuration file
    
    The DBS configuration file (see DBS documentation) will be passed to the
    DBS interface. It can specify server endpoint, logging level, and other
    variables, but it is overwritten by values specified by other means 
    (as explained above). The config file will be searched in (in this order):
       - specified dbs_client_config argument (in **kwd)
       - DBS_CLIENT_CONFIG environmental variable
    
    If the necessary arguments cannot be obtained in any of these ways, the
    instantiation is denied and a DlsConfigError is raised.
 
    The verbosity level affects invocations of all methods in this object.
    See the dlsApi.DlsApi.setVerbosity method for information on accepted values.
    The underlying DBS interface verbosity can be controlled via the corresponding
    DBS configuration parameter (in **kwd or config file).
      
    @exception DlsConfigError: if the DBS interface object cannot be set up correctly 

    @param dls_endpoint: the DLS server to be used, as a string "hname[:port]/path/to/DLS"
    @param verbosity: value for the verbosity level
    @param kwd: Flags:
                  - URL: DLS server (DBS) endpoint
                  - version: DBS client version
                  - dbs_client_config: config file for DBS interface to use
                  - any other DBS-specific
    """

    # Let the parent set the server endpoint (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)

    # If the server is there, put it in the dict to pass
    kwd["url"] = self.server
    
    # Now go for the configuration file
    if kwd.has_key("dbs_client_config"):
       self.dbscfg = kwd["dbs_client_config"]
       if(self.dbscfg):
          environ["DBS_CLIENT_CONFIG"]=self.dbscfg

    # Don't do this. Leave DBS default (dealing with level in conf file is also missing)
#    # Adjust DBS verbosity to WARNING (unless overriden)
#    keys = map(str.upper, kwd.keys())
#    if(not kwd.has_key("LEVEL")):
#       kwd["level"] = 'WARNING'

    # Try to create the DBS interface object (if there are missing params, it'll shout)
    try:
       self.dbsapi = DbsApi(kwd)
    except (DbsApiException, DbsException), inst:
       msg = "Could not set the interface to the DLS server. "
       msg += "Error trying to instantiate DBS interface object"
       self._mapException(inst, msg, msg, errorTolerant = False)
   
    # Set the server in our own variable (will this be needed?)
    self.server = self.dbsapi.url()

    # TODO: add the checkEndpoint flag to avoid innecessary checking
    # Check that the provided URL is OK (by listing an inexisting fileblock)
    try:
       self.dbsapi.listBlocks(block_name="-") 
    except DbsApiException, inst:
       msg = "Could not set the interface to the DLS server. "
       msg += "Error when accessing provided DBS URL: %s" %   (self.server)
       self._mapException(inst, msg, msg, errorTolerant = False)

    

  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.add method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Fileblocks in DBS can be created only through DBS interface, not through DLS
    API. Thus, this method can be used to add locations to an existing fileblock
    only. Trying to add locations to a non-existing fileblock will result
    in a dlsApiError exception.

    Likewise, fileblock attributes are handled by DBS, and they can't be set 
    or updated through DLS. Therefore, this method will ignore any specified
    FileBlock attribute.
    
    The list of supported attributes for the locations is:
     - custodial  (values: "True" or "False"; if not specified, False is assumed)

    Specified GUID for the FileBlocks and SURL for the replicas are ignored.
    
    The following keyword flags are ignored: allowEmptyBlocks, createParent,
    trans, session.
    """

    # Keywords
    checkLocations = True
    if(kwd.has_key("checkLocations")):
       checkLocations = kwd.get("checkLocations")       
       
    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):
       errorTolerant = kwd.get("errorTolerant")
    
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]


    # Loop on the entries
    for entry in theList:
    
      # First check locations if asked for (not to leave empty blocks)
      if(checkLocations):
         locList = []
         exitLoop = False
         for loc in entry.locations:
            try:
               loc.checkHost = True
               loc.host = loc.host
               locList.append(loc)
            except DlsDataObjectError, inst:
               msg = "Error in location %s for "%(loc.host)
               msg += "FileBlock %s: %s" % (entry.fileBlock.name, inst.msg)
               self._warn(msg)
               if(not errorTolerant):
                  raise DlsInvalidLocation(msg)
      else:
         locList = entry.locations

      # Skip empty fileBlocks
      if(not locList):  
         self._warn("No locations for fileblock %s. Skipping." % entry.fileBlock.name)
         continue


      # Add locations
      for loc in locList:
         dbsSE = self._mapLocToDbs(loc)
         self._debug("dbs.addReplicaToBlock(%s,%s)" % (entry.fileBlock.name, loc))
         try:
            self.dbsapi.addReplicaToBlock(entry.fileBlock.name, dbsSE)
         except DbsApiException, inst:
            msg = "Error inserting locations for fileblock %s" % (entry.fileBlock.name)
            w_msg = msg + ". Skipping"
            self._mapException(inst, msg, w_msg, errorTolerant)


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
    
  def delete(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.delete method.
    Refer to that method's documentation.

    Implementation specific remarks:

    This method will only delete locations for a five FileBlock, but not
    the FileBlock itself (even if all the replicas are removed). With DBS
    back-end, the FileBlock itself might be deleted (or invalidated) through
    DBS interface only, but not through DLS interface.
    
    The following keyword flags are ignored: keepFileBlock, session, 
    """

    # Keywords
    force = False 
    if(kwd.has_key("force")):    force = kwd.get("force")
       
    all = False 
    if(kwd.has_key("all")):      all = kwd.get("all")

    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):  errorTolerant = kwd.get("errorTolerant")

       
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Loop on the entries
    for entry in theList:
      
      # Get the FileBlock name
      lfn = entry.fileBlock.name

      # Get the specified locations
      seList = []
    # TODO: what if attributes are not all string
      if(not all):
         for loc in entry.locations:
            seList.append(loc.host)


      # Retrieve the existing associated locations (from the catalog)      
      locList = []
      entryList = self.getLocations(lfn, errorTolerant = errorTolerant)
      for i in entryList:
          for j in i.locations:
              locList.append(j)
     
      # Make a copy of location list (to keep control of how many are left)
      remainingLocs = []
      for i in xrange(len(locList)):
         remainingLocs.append(locList[i].host)
         
      # Loop on associated locations
      for filerep in locList:

         host = filerep.host
      
         # If this host (or all) was specified, remove it
         if(all or (host in seList)):
         
            # Don't look for this SE further
            if(not all): seList.remove(host)
            
#            # But before removal, check if it is custodial
#            if ((filerep["custodial"]!="True") and (not force)):
#               code = 503
#               msg = "Can't delete custodial replica in",host,"of",lfn
#               if(not errorTolerant): 
#                  raise DlsApiError(msg, code)
#               else: 
#                  self._warn(msg)
#                  continue
               
            # Perform the deletion
            try:
               self._debug("dbs.deleteReplicaFromBlock(%s, %s)" % (lfn, host))
               self.dbsapi.deleteReplicaFromBlock(lfn, host) 
               remainingLocs.remove(host)
            except DbsApiException, inst:
               msg = "Error removing location %s from FileBlock %s"%(host,lfn)
               self._mapException(inst, msg, msg, errorTolerant)
        
            # And if no more SEs, exit
            if((not all) and (not seList)):
               break
   
      # For the SEs specified, warn if they were not all removed
      if(seList and (not all)):
         self._warn("Not all specified locations could be found and removed")
  



    
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If longList (**kwd) is set to True, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
     - custodial 

    If instead of a FileBlock name, a pattern is provided, the method
    includes in the returned list the DlsEntry objects for all the matching
    FileBlocks.

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

    entryList = []


    # Loop on the entries
    for fB in theList:
       # Check what was passed (DlsFileBlock or string)
       if(isinstance(fB, DlsFileBlock)):
         lfn = fB.name
       else:
         lfn = fB

       # If '/' is given, we want all blocks back
       if(lfn=='/'): lfn = '*'

       # Get the locations for the given FileBlock
       dbsList = None
       self._debug("dbs.listBlocks(block_name=%s)" % lfn)
       try:  
          dbsList = self.dbsapi.listBlocks(block_name=lfn) 
       except DbsApiException, inst:
          msg = "Error retrieving locations for %s" % (lfn)
          msg_w = msg + ". Skipping"
          self._mapException(inst, msg, msg_w, errorTolerant)
       
       # Build the result
       if(dbsList):
          for dbsFb in dbsList:
             entry = self._mapEntryFromDbs(dbsFb)
             entryList.append(entry)
       else:
          msg = "No existing fileblock matching %s" % (lfn)
          msg_w = msg + ". Skipping"
          if(not errorTolerant):  raise DlsInvalidBlockName(msg)
          else:                   self._warn(msg_w)
         
    # Return what we got
    return entryList



  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If instead of a location host, a pattern is provided, the method
    includes in the returned list the DlsEntry objects for all the matching
    locations. If '*' is provided, FileBlocks with no associated location
    are also returned (with no DlsLocation object in their composing list).

    The following keyword flags are ignored: session.
    """

    # Keywords

    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList 
    else:
       theList = [locationList]

    entryList = []

    # Loop on the entries
    for loc in theList:
       
       # Check what was passed (DlsLocation or string)
       if(isinstance(loc, DlsLocation)):
         host = loc.host
       else:
         host = loc

       # Retrieve list of locations 
       dbsList = None
       self._debug("dbs.listBlocks(storage_element_name=%s)" % host)
       try:
          dbsList = self.dbsapi.listBlocks(storage_element_name=host)
       except DbsApiException, inst:
          msg = "Error retrieving locations for %s" % (host)
          self._mapException(inst, msg, msg, False)

       # Build the result
       if(dbsList != None):
          for dbsFb in dbsList:
             entry = self._mapEntryFromDbs(dbsFb)
             entryList.append(entry)

    # Return what we got
    return entryList



  def listFileBlocks(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.listFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since DBS FileBlock namespace is not hierarchical, there is no concept
    of FileBlock directory, and recursive listing makes no sense.

    If instead of a FileBlock name, a pattern is provided, the method
    includes in the returned list the DlsEntry objects for all the matching
    FileBlocks.

    If longList (**kwd) is set to True, the attributes returned with
    the FileBlock are the following:
      - "BlockSize"
      - "NumberOfFiles" 
      - "NumberOfEvents"
      - "OpenForWriting"
      - "Dataset"
      - "FileList"
      - "CreationDate"
      - "CreatedBy"
      - "LastModificationDate"
      - "CreatedBy"

    TODO: Is next sentence true?
    Additionally, the GUID is set in the corresponding dlsFileBlock object.

    The following keyword flags are ignored: session, recursive.
    """
    # Keywords
    longList = True 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    # With DBS, this is just the same as getLocations, removing the locations
    entryList = self.getLocations(fileBlockList, longList = longList, errorTolerant = True)

    fbList = []
    for entry in entryList:
      fbList.append(entry.fileBlock)

    # Return what we got
    return fbList



  def getAllLocations(self, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getAllLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The following keyword flags are ignored: session.
    """
    locList = []
 
    # Get all the locations from the root dir
    lfn = "/"
    self._debug("dbs.listStorageElements(storage_element_name=\'*\')")
    try:
       dbsSeList = self.dbsapi.listStorageElements(storage_element_name='*')
    except DbsApiException, inst:
       msg = "Error getting all locations in DLS"
       self._mapException(inst, msg, msg, errorTolerant = False)

    # Build the list of DlsLocation objects
    for dbsSE in dbsSeList:
       loc = self._mapLocFromDbs(dbsSE)
       locList.append(loc)

    # Return what we got
    return locList


  def dumpEntries(self, dir = "/", **kwd):
    """
    Implementation of the dlsApi.DlsApi.dumpEntries method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since DBS FileBlock namespace is not hierarchical, there is no concept
    of FileBlock directory, and recursive listing makes no sense. The dir
    argument is interpreted as representing a FileBlock name pattern, and 
    the matching FileBlocks and associated locations are dumped.

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

    Since DBS does not support sessions, this method does nothing.
    """
    self._debug("Starting session with %s (no action)" % (self.server))

 
  def endSession(self):
    """
    Implementation of the dlsApi.DlsApi.endSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    Since DBS does not support sessions, this method does nothing.
    """
    self._debug("Ending session with %s (no action)" % (self.server))
  
 
  def startTrans(self):
    """
    Implementation of the dlsApi.DlsApi.startTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since DBS does not support transactions, this method does nothing.
    """
    self._debug("Starting transaction with %s (no action)" % (self.server))


  def endTrans(self):
    """
    Implementation of the dlsApi.DlsApi.endTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since DBS does not support transactions, this method does nothing.
    """
    self._debug("Ending transaction with %s (no action)" % (self.server))
  
  
  def abortTrans(self):
    """
    Implementation of the dlsApi.DlsApi.abortTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since DBS does not support transactions, this method does nothing.
    """
    self._debug("Aborting transaction with %s (no action)" % (self.server))
 

  
  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location, **kwd):
    """
    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
    Refer to that method's documentation.
    """

    # Keywords
    checkLocations = True
    if(kwd.has_key("checkLocations")):
       checkLocations = kwd.get("checkLocations")       

    # First check the new location if asked for it
    if(checkLocations):
       try:
          loc = DlsLocation(dest_location, checkHost = True)
       except DlsDataObjectError, inst:
          msg = "Error replacing location %s with %s: %s"%(org_location,dest_location,inst.msg)
          raise DlsInvalidLocation(msg)

    # Perform the replacement
    self._debug("dbs.renameSE(%s, %s)"%(org_location, dest_location))
    try:
       self.dbsapi.renameSE(org_location, dest_location)
    except DbsApiException, inst:
       msg = "Error replacing location in DLS"
       try:       
         rc = int(inst.getErrorCode())
       except Exception:
         rc = 0
       if(rc in [2000]):
         msg += ". New location (%s) exists in DLS server already" % (dest_location)
         caught_msg = inst.getClassName() + ' ' + inst.getErrorMessage()
         msg = msg + '. Caught: [%d] %s' % (rc, caught_msg)
         raise DlsInvalidLocation(msg, server_error_code=rc)
         
       self._mapException(inst, msg, msg, errorTolerant = False)




  ##################################
  # Private methods
  ##################################

  def _mapEntryFromDbs(self, dbsFb):
    """
    Builds and returns a DlsEntry object based on the specified DbsFileBlock 
    object.  It copies FileBlock name, location host and all present attributes.

    @param dbsFb: the DbsFileBlock object to be translated

    @return: the newly created DlsEntry object 
    """
#    attrs = {"CreationDate": dbsFb.CreationDate, "CreatedBy": dbsFb.CreatedBy,
#             "LastModificationDate": dbsFb.LastModificationDate,
#             "Dataset": dbsFb.Dataset, "NumberOfFiles": dbsFb.NumberOfFiles
#             "NumberOfEvents": dbsFb.NumberOfEvents, "BlockSize": dbsFb.BlockSize
#             "OpenForWriting": dbsFb.OpenForWriting}
    # TODO: what if attributes are not all string
    attrs={}
    for key in dbsFb.keys():
       if(key == "Name"): continue
       if(key == "StorageElementList"): continue
       attrs[key] = str(dbsFb[key])

    entry = DlsEntry(DlsFileBlock(dbsFb["Name"], attrs)) 
    locList = []
    for dbsSE in dbsFb["StorageElementList"]:
       loc = self._mapLocFromDbs(dbsSE)
       locList.append(loc)
    entry.locations = locList
    
    return entry


  def _mapLocFromDbs(self, dbsSE):
    """
    Builds and returns a DlsLocation object based on the specified DbsStorageElement
    object.  It copies location host and all present attributes.

    @param dbsSE: the DbsStorageElement object to be translated

    @return: the newly created DlsLocation object 
    """
    # Argument is a dbsSE object (with attributes)
    if(isinstance(dbsSE, DbsStorageElement)):    
#       attrs = {"CreationDate": dbsLoc.CreationDate, "CreatedBy": dbsLoc.CreatedBy,
#                "LastModificationDate": dbsLoc.LastModificationDate,
#                "custodial": dbsLoc.custodial}
       # TODO: what if attributes are not all string
       attrs = {}
       for key in dbsSE.keys():
          if(key == "Name"): continue
          attrs[key] = str(dbsSE[key])
          
       loc = DlsLocation(dbsSE["Name"], attrs)
    # Argument is a string (just SE host)
    else: 
       loc = DlsLocation(dbsSE)
    
    return loc



  def _mapLocToDbs(self, loc):
    """
    Builds and returns a DbsStorageElement object based on the specified DlsLocation
    object.  It copies location host and all supported attributes.

    @param loc: the DlsLocation object to be translated

    @return: the newly created DbsStorageElement object 
    """
    # For now, we just provide a string, since this is what DBS expects
    dbsLoc = loc.host
    return dbsLoc
####################

    # This would be to add attribute support
    dbsLoc = DbsStorageElement(loc.host)
    attrList = loc.attribs
    for attr in attrList:
      if(attr == "custodial"):      
         custodial = attrList[attr]
         if((custodial != "True")and (custodial != "False")):
             msg = "Unsupported value (%s) for attribute %s. Skipping."%(attrList[attr],attr)
             self._warn(msg)
             continue
         # TODO: what if attributes are not all string
         dbsLoc.custodial = attrList[attr]
      else:
         self._warn("Attribute %s of location (%s) unknown. Skipping" % (attr,loc.host))

    return dbsLoc



  def _mapException(self, inst, excp_msg, warn_msg, errorTolerant=False):
    """
    If errorTolerant==False, analyzes the passed DBS exception and raises the 
    appropriate corresponding DlsApiError exception including the message
    excp_msg. If errorTolerant==True, just prints the passed warn_msg using
    the _warn function.

    @param inst: the DbsApiException object to be analyzed
    @param excp_msg: the message passed to the exception in creation
    @param warn_msg: the message to print as warning
    @param errorTolerant: boolean to control operation 

    @exception: raises the appropriate DlsApiError, if errorTolerant==False
    """
 
    try:
       rc = int(inst.getErrorCode())
    except Exception:
       rc = 0
    caught_msg = inst.getClassName() + ' ' + inst.getErrorMessage()
    if(not errorTolerant):
       excp_msg = excp_msg + '. Caught: [%d] %s' % (rc, caught_msg)
       
       if(rc in [1004, 1005, 1016, 1017, 1036, 1037]):
         raise DlsArgumentError(excp_msg, server_error_code=rc)
         
       if(rc in [1010]):
         raise DlsUnexistingBlockError(excp_msg, server_error_code=rc)
         
       if(rc in [1006, 1014, 1015, 1022]):
         raise DlsInvalidBlockName(excp_msg, server_error_code=rc)
         
       if(rc in xrange(1050, 1062)):
         raise DlsConfigError(excp_msg, server_error_code=rc)

       # Otherwise, we raise the default exception
       # This should at least cover: 1001/2/3/8/9/11/12
       raise DlsErrorWithServer(excp_msg, server_error_code=rc)
       
    else:
       warn_msg = warn_msg + '. Caught: [%d] %s' % (rc, caught_msg)
       self._warn(warn_msg)

