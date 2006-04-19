#
# $Id: dlsLfcApi.py,v 1.4 2006/04/19 16:25:48 delgadop Exp $
#
# DLS Client. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server using a LCG File Catalog (LFC) as back-end.

 For the implementation of the getLocations method, the dlsDliClient
 module is used (for performance reasons). Check that module for
 dependencies.

 The module contains the DlsLfcApi class that implements all the methods
 defined in dlsApi.DlsApi class and a couple of extra convenient
 (implementation specific) methods. Python applications interacting with
 a LFC-based DLS will instantiate a DlsLFCApi object and use its methods.

 It also contains some exception classes to propagate error conditions
 when interacting with the DLS catalog.
"""

#########################################
# Imports 
#########################################
import dlsApi
DLS_VERB_HIGH = dlsApi.DLS_VERB_HIGH
DLS_VERB_WARN = dlsApi.DLS_VERB_WARN
import dlsDliClient   # for a fast getLocations implementation
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
# From what comes next, should not import whole modules, but what is needed...
import lfc
import sys
import commands
import time
import getopt
from os import environ, putenv

#########################################
# Module globals
#########################################
#DLS_VERB_NONE = 0    # print nothing
#DLS_VERB_WARN = 10   # print only warnings (to stdout)
#DLS_VERB_HIGH = 20   # print warnings (stdout) and error messages (stderr)

#########################################
# DlsLfcApiError class
#########################################

class DlsLfcApiError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the DlsLfcApi
  class. It normally contains a string message (empty by default), and optionally
  an  error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.
  """

class NotImplementedError(DlsLfcApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsLfcApiError):
  """
  Exception class for invocations of DlsApi methods with an incorrect value
  as argument.
  """
  
class SetupError(DlsLfcApiError):
  """
  Exception class for errors when setting up the system (configuration,
  communication errors...)
  """



#########################################
# DlsApì class
#########################################

class DlsLfcApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on a Lcg File Catalog
  (LFC) as DLS back-end.

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsLfcApiError.
  """

  def __init__(self, dls_endpoint= None, verbosity = DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLS (LFC) server to communicate with
    and the verbosity level.
    
    It tries to retrieve that value from several sources (in this order):
    
         - specified dls_endpoint 
         - DLS_ENDPOINT environmental variable
         - LFC_HOST environmental variable
         - DLS catalog advertised in the Information System (if implemented)

    If the DLS server cannot be set in any of this ways, the instantiation is 
    denied and a SetupError is raised.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.
      
    @exception SetupError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string of form "hostname[:port]"
    @param verbosity: value for the verbosity level
    """

    # Let the parent set the server (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)

    # If the server is not there yet, try from LFC_HOST
    if(not self.server):
      self.server = environ.get("LFC_HOST")

    # If still not there, give up 
    if(not self.server):
       raise SetupError("Could not set the DLS server to use")
    else:
       # Set the server for LFC API use
       putenv("LFC_HOST", self.server)




  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.add method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Accepts the createParent flag as described in dlsApi.DlsApi.add.

    The list of supported attributes for the FileBlocks is:
     - guid
     - mode
     - filesize
     - csumtype
     - csumvalue
    
    The list of supported attributes for the locations is:
     - sfn
     - f_type
     - ptime

    If the composing DlsFileBlock objects used as argument include the GUID of
    the FileBlock, then that value is used in the catalog. Likewise, if the
    composing DlsLocation objects include the SURL of the FileBlock copies,
    that value is also used. Notice that both uses are discouraged as they may
    lead to catalog corruption if used without care.
    
    The specified FileBlock names may be relative or absolute (see the
    _checkDlsHome method). 
    """

    # Keywords
    createParent = True
    if(kwd.has_key("createParent")):
       createParent = kwd.get("createParent")
       
    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):
       errorTolerant = kwd.get("errorTolerant")
    
    trans = False 
    if(kwd.has_key("trans")):
       trans = kwd.get("trans")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    if(trans):
      errorTolerant = False
      session = False
   
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Start transaction/session
    if(trans): self.startTrans()
    else:
       if(session): self.startSession()

    # Loop on the entries
    for entry in dlsEntryList:
      # FileBlock
      try:
         guid = self._addFileBlock(entry.fileBlock, createParent=createParent)
      except DlsLfcApiError, inst:
         if(not errorTolerant):
           if(session): self.endSession()
           if(trans):   inst.msg += ". Transaction operations rolled back"
           raise inst
         else: # Can't add locations without guid, so go to next FileBlock
           continue
      # Locations
      for loc in entry.locations:
         try:
            self._addLocationToGuid(guid, loc)
         except DlsLfcApiError, inst:
            if(not errorTolerant):
               if(session): self.endSession()
               if(trans):   inst.msg += ". Transaction operations rolled back"
               raise inst

    # End transaction/session
    if(trans): self.endTrans()
    else:
       if(session): self.endSession()

 
  def update(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.update method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The specified FileBlock names may be relative or absolute (see the
    _checkDlsHome method). 
    
    For a given FileBlock, specified locations that are not registered in the
    catalog will be ignored.

    The list of supported attributes for the FileBlocks is:
     - filesize
     - csumtype
     - csumvalue
    
    The list of supported attributes for the locations is:
     - ptime
     - atime*

    (*) NOTE: For "atime", the value of the attribute is not considered, but the
    access time is set to current time.
    """

    # Keywords
    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):
       errorTolerant = kwd.get("errorTolerant")
       
    trans = False
    if(kwd.has_key("trans")):
       trans = kwd.get("trans")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    if(trans):
      errorTolerant = False
      session = False   
 
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Start transaction/session
    if(trans): self.startTrans()
    else:
       if(session): self.startSession()

    # Loop on the entries
    for entry in dlsEntryList:
      # FileBlock
      try:
         self._updateFileBlock(entry.fileBlock)
      except DlsLfcApiError, inst:
         if(not errorTolerant):
           if(session): self.endSession()
           if(trans):   inst.msg += ". Transaction operations rolled back"
           raise inst

      # Locations (must retrieve the SURLs from the catalog and compare)
      seList = []
      for loc in entry.locations:
         seList.append(loc.host)
      lfn = entry.fileBlock.name
      lfn = self._checkDlsHome(lfn)
      if(self.verb >= DLS_VERB_HIGH):
         print "--lfc.lfc_getreplica(\""+ lfn +"\", \"\",\"\")"
      err, repList = lfc.lfc_getreplica(lfn, "", "")
      if(err):
         if(not errorTolerant):
           if(session): self.endSession()
           raise inst
           code = lfc.cvar.serrno
           msg = "Error retrieving locations for(%s): %s" % (lfn, lfc.sstrerror(code))
           if(trans):   msg += ". Transaction operations rolled back"
           raise DlsLfcApiError(msg, code)
         else: continue
    
      for filerep in repList:
      
         if (filerep.host in seList):
            loc = entry.getLocation(filerep.host)
            loc.setSurl(filerep.sfn)
         
            # Don't look for this SE further
            seList.remove(filerep.host)            
            
            # Update location
            try:
              self._updateSurl(loc)
            except DlsLfcApiError, inst:
              if(not errorTolerant): 
                 if(session): self.endSession()
                 if(trans):   inst.msg += ". Transaction operations rolled back"
                 raise inst

            # And if no more SEs, exit
            if(not seList):
               break
   
      # For the SEs specified, warn if they were not all updated 
      if(seList and (self.verb >= DLS_VERB_WARN)):
         print "Warning: Not all locations could be found and updated"

    # End transaction/session
    if(trans): self.endTrans()
    else:
       if(session): self.endSession()


    
  def delete(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.delete method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The LFC-based DLS supports symlinks to a FileBlock. This method accepts both
    original FileBlocks or symlinks. Only the specified one will be deleted,
    unless removeLinks (**kwd) is set to True; in that case, all the symlinks and
    the original FileBlock will be removed from the DLS.
    NOTE: THIS FLAG IS NOT YET PROPERLY IMPLEMENTED!

    The specified FileBlock names may be relative or absolute (see the
    _checkDlsHome method). 
    
    NOTE: It is not safe to use this method within a transaction.
    
    Additional parameters:
    @param kwd: Flags:
     - removeLinks: boolean (default False) for removing all the symlinks. THIS
       FLAG IS NOT YET PROPERLY IMPLEMENTED!
    """

    # Keywords
    force = False 
    if(kwd.has_key("force")):
       force = kwd.get("force")
       
    all = False 
    if(kwd.has_key("all")):
       all = kwd.get("all")

    removeLinks = False 
    if(kwd.has_key("removeLinks")):
       removeLinks = kwd.get("removeLinks")

    keepFileBlock = False 
    if(kwd.has_key("keepFileBlock")):
       keepFileBlock = kwd.get("keepFileBlock")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")
       
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Start session
    if(session): self.startSession()

    # Loop on the entries
    for entry in theList:
      
      # Get the FileBlock name
      lfn = entry.fileBlock.name
      lfn = self._checkDlsHome(lfn)

      # Get the specified locations
      seList = []
      if(not all):
         for loc in entry.locations:
            seList.append(loc.host)
  
      ###### Locations part #####

      # Retrieve the existing associated locations (from the catalog)
      if(self.verb >= DLS_VERB_HIGH):
         print "--lfc.lfc_getreplica(\""+lfn+"\", \"\",\"\")"
      err, locList = lfc.lfc_getreplica(lfn, "", "")
      if(err):
         if(session): self.endSession()
         code = lfc.cvar.serrno
         msg = "Error retrieving locations for FileBlock (%s): %s" % (lfn, lfc.sstrerror(code))
         raise DlsLfcApiError(msg, code)
     
      # Make a copy of location list (to keep control of how many are left)
      remainingLocs = []
      for i in xrange(len(locList)):
         remainingLocs.append(locList[i].host)
         
      # Loop on associated locations
      for filerep in locList:
      
         # If this host (or all) was specified, remove it
         if(all or (filerep.host in seList)):
         
            # Don't look for this SE further
            if(not all): seList.remove(filerep.host)
            
            # But before removal, check if it is custodial
            if ((filerep.f_type == 'P') and (not force)):
               if(self.verb >= DLS_VERB_WARN):
                  print "Warning: Not deleting custodial replica in",filerep.host,"of",lfn
               continue
               
            # Perform the deletion
            try:
               err = self._deleteSurl(filerep.sfn)
               remainingLocs.remove(filerep.host)
            except DlsLfcApiError, inst:
               if(not errorTolerant): 
                  if(session): self.endSession()
                  raise inst
        
            # And if no more SEs, exit
            if((not all) and (not seList)):
               break
   
      # For the SEs specified, warn if they were not all removed
      if(not all):
         if(seList and (self.verb >= DLS_VERB_WARN)):
            print "Warning: Not all specified locations could be found and removed"
   

      ###### FileBlock part #####
      
      # If all the replicas (even custodial) were deleted and no keepFileBlock specified
      if((not remainingLocs) and (not keepFileBlock)):
  
         # If it was specified, delete all links (even main FileBlock name)
         if(removeLinks):
  
            if(self.verb >= DLS_VERB_HIGH):
            # TODO: Have to check this call... lxplus UI conf issue
               print "--lfc.lfc_getlinks(\""+lfn+"\", \"\",\"\")"
            err, linkList = lfc.lfc_getlinks(lfn, "", "")
  
            if(err):
               if(session): self.endSession()
               code = lfc.cvar.serrno
               msg = "Error retrieving links for FileBlock (%s): %s" % (lfn, lfc.sstrerror(code))
               raise DlsLfcApiError(msg, code)
  
            for link in linkList:
               try:
                  err = self._deleteFileBlock(link.path)
               except DlsLfcApiError, inst:
                  if(not errorTolerant):
                     if(session): self.endSession()
                     raise inst
            
         # Not links, delete only specified name
         else:
            try:
               err = self._deleteFileBlock(lfn)
            except DlsLfcApiError, inst:
               if(not errorTolerant):
                  if(session): self.endSession()
                  raise inst

    # End session
    if(session): self.endSession()

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The specified FileBlock names may be relative or absolute (see the
    _checkDlsHome method). 
    
    If longList (**kwd) is set to true, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
     - atime
     - ptime
     - f_type.

    NOTE: The long listing may be quite more expensive (slow) than the normal
    invocation, since the first one has to query the LFC directly (secure),
    while the normal gets the information from the DLI (insecure), by using
    the dlsDliClient.DlsDliClient class.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    
    # Keywords
    longlist = False 
    if(kwd.has_key("longlist")):
       longlist = kwd.get("longlist")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]

    entryList = []
    
    # For long listing (need to query the LFC directly)    
    if(longlist):

      # Start session
      if(session): self.startSession()

      # Loop on the entries
      for fB in theList:
         # Check what was passed (DlsFileBlock or string)
         if(isinstance(fB, DlsFileBlock)):
           lfn = fB.name
         else:
           lfn = fB
         lfn = self._checkDlsHome(lfn)
         entry = DlsEntry(DlsFileBlock(lfn))
 
         # Get the locations for the given FileBlock
         if(self.verb >= DLS_VERB_HIGH):
             print "--lfc.lfc_getreplica(\""+lfn+"\", \"\", \"\")"   
         err, filerepList = lfc.lfc_getreplica(lfn, "", "")
         if(err):
            if(session): self.endSession()
            code = lfc.cvar.serrno
            msg = "Error retrieving locations for %s: %s" % (lfn, lfc.sstrerror(code))
            raise DlsLfcApiError(msg, code)
         
         # Build the result
         locList = []
         for filerep in filerepList:
            attrs = {"atime": filerep.atime, "ptime": filerep.ptime,
                     "f_type": filerep.f_type, "sfn": filerep.sfn}
            
            loc = DlsLocation(filerep.host, attrs)
            locList.append(loc)
         entry.locations = locList
         entryList.append(entry)

      # End session
      if(session): self.endSession()

    

    # For normal listing (go through the DLI)
    else:      
      # Build a FileBlock name list with absolute paths
      dliList = []
      for fB in theList:
         # Check what was passed (DlsFileBlock or string)
         if(isinstance(fB, DlsFileBlock)):
           lfn = fB.name
         else:
           lfn = fB
         dliList.append(self._checkDlsHome(lfn))
         
      # Create the binding (that tries also DLI_ENDPOINT if server not yet set)
      try:
        dliIface = dlsDliClient.DlsDliClient(self.server, verbosity = self.verb)
      except dlsDliClient.SetupError, inst:
        raise DlsLfcApiError("Error creating the binding with the DLI interface: "+str(inst))

      # Query the DLI 
      try:
          entryList = dliIface.getLocations(dliList)
      except dlsDliClient.DlsDliClientError, inst:
        raise DlsLfcApiError(inst.msg)


    # Return what we got
    if(len(entryList) == 1):
      return entryList[0]
    else:
      return entryList

      
  
  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    NOTE: This method may be quite more expensive (slow) than the getLocations
    method.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """

    # Keywords
    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList 
    else:
       theList = [locationList]

    listList = []

    # Start session
    if(session): self.startSession()

    # Loop on the entries
    for loc in theList:
       entryList = []
       
       # Check what was passed (DlsLocation or string)
       if(isinstance(loc, DlsLocation)):
         host = loc.host
       else:
         host = loc

       # Retrieve list of locations 
       locList=lfc.lfc_list()
       flags_rep=lfc.CNS_LIST_BEGIN 
    
       # Call the retrieval in a loop
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_listreplicax(\"\", \""+host+"\", \"\",",flags_rep,",locList)"   
       filerep = lfc.lfc_listreplicax("", host, "", flags_rep, locList)

       while(filerep):
          path = ' ' * (lfc.CA_MAXPATHLEN+1)
          if(self.verb >= DLS_VERB_HIGH):
             print "--lfc.lfc_getpath(\"\",",filerep.fileid,", path)  (("+filerep.sfn+"))"   
          if(lfc.lfc_getpath("", filerep.fileid, path)<0):
             if(session): self.endSession()
             code = lfc.cvar.serrno
             msg = "Error retrieving the path for %s: %s" % (filerep.sfn, lfc.sstrerror(code))
             raise DlsLfcApiError(msg, code)
  
          # Build the result 
          entry = DlsEntry(DlsFileBlock(path), [DlsLocation(host, surl = filerep.sfn)])
          entryList.append(entry)

          # Go on with the listing
          flags_rep=lfc.CNS_LIST_CONTINUE
          if(self.verb >= DLS_VERB_HIGH):
             print "--lfc.lfc_listreplicax(\"\", \""+host+"\", \"\",",flags_rep,",locList)"   
          filerep=lfc.lfc_listreplicax("", host, "", flags_rep, locList)
         
       # Finish the location retriving
       flags_rep=lfc.CNS_LIST_END
       filerep=lfc.lfc_listreplicax("", host, "", flags_rep, locList)

       # Add the list to the list of lists
       listList.append(entryList)


    # End session
    if(session): self.endSession()

    # Return what we got
    if(len(listList) == 1):
      return listList[0]
    else:
      return listList


  def listFileBlocks(self, fileBlockList, **kwd):
    """
    THIS METHOD IS NOT YET IMPLEMENTED.

    Implementation of the dlsApi.DlsApi.listFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    This implementation supports a FileBlock directory (not a list) 
    as argument of the method, as well as a FileBlock name/object, or
    list of those.

    If longList (**kwd) is set to true, the attributes returned with
    the FileBlock are the following:
     - filemode
     - nbfiles (number of files in a directory)
     - owner
     - group (group owner)
     - mtime (last modification date)

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    msg += "Not yet implemented"
    raise NotImplementedError(msg)


  def startSession(self):
    """
    Implementation of the dlsApi.DlsApi.startSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Starting session with "+self.server
    if(lfc.lfc_startsess("", "")):
      code = lfc.cvar.serrno
      msg = "Error starting session with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)

 
  def endSession(self):
    """
    Implementation of the dlsApi.DlsApi.endSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Ending session with "+self.server
    if(lfc.lfc_endsess()):
      code = lfc.cvar.serrno
      msg = "Error ending session with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
  
 
  def startTrans(self):
    """
    Implementation of the dlsApi.DlsApi.startTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Dealing with transactions in LFC is not trivial. Within a transaction, some of
    the methods in the LFC API see the normal view of the catalog (as it was before
    the transaction was initiated) and some see the updated view (after changes
    produced by the not yet comitted operations). For that reason, modifying the 
    catalog and then checking it can lead to unpredictable results in some
    situations. As a general rule, please use transactions only for addition or 
    update operations and do not enclose too many operations within a
    transaction.
    
    Please check LFC documentation for details on transactions.

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Starting transaction with "+self.server
    if(lfc.lfc_starttrans("", "")):
      code = lfc.cvar.serrno
      msg = "Error starting transaction with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)


  def endTrans(self):
    """
    Implementation of the dlsApi.DlsApi.endTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    See the startTrans method.

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Ending transaction with "+self.server
    if(lfc.lfc_endtrans()):
      code = lfc.cvar.serrno
      msg = "Error ending transaction with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
  
  
  def abortTrans(self):
    """
    Implementation of the dlsApi.DlsApi.abortTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    See the startTrans method.

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Aborting transaction with "+self.server
    if(lfc.lfc_aborttrans()):
      code = lfc.cvar.serrno
      msg = "Error aborting transaction with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
  
 

  
  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    NOT YET IMPLEMENTED (IF EVER).

    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
    Refer to that method's documentation.
    """

    # Implement here...
    msg += "Not yet implemented"
    raise NotImplementedError(msg)

        
  ################################################
  # Other public methods (implementation specific)
  ################################################

  def getGUID(self, fileBlock):
    """
    THIS METHOD IS NOT YET IMPLEMENTED.

    Returns the GUID used in the DLS for the specified FileBlock, by querying
    the DLS.

    The FileBlock may be specified as a DlsFileBlock object or as a simple string
    (holding the FileBlock name). In the first case, the method returns a
    reference to the object, after having set its internal GUID variable. In the
    second case, the method returns the GUID as a string (without any prefix,
    such as "guid://").

    Notice that this method will only work if the FileBlock has already been
    registered with the DLS catalog.

    The specified FileBlock name may be relative or absolute (see the
    _checkDlsHome method). 

    @exception XXXX: On error with the DLS catalog (non-existing FileBlock, etc.)
    
    @param fileBlock: the FileBlock, as a string/DlsFileBlock object

    @return: the GUID, as a string or the DlsFileBlock object with the GUID set
    """
    
    # Implement here...
    msg += "Not yet implemented"
    raise NotImplementedError(msg)

    
  def getSURL(self, dlsEntry):
    """
    THIS METHOD IS NOT YET IMPLEMENTED.

    Gets the SURLs associated with the specified DlsEntry object. It querys the
    DLS and sets the SURLs (one per each location in the composing location list)
    in the specified DlsEntry object. It returns a reference to this completed
    object. 

    The specified FileBlock name may be relative or absolute (see the
    _checkDlsHome method). 

    @exception XXXX: On error with the DLS catalog (non-existing FileBlock, etc.)
    
    @param dlsEntry: the DlsEntry object for which the SURLs must be retrieved

    @return: the DlsEntry object with the SURLs added for each location
    """

    # Implement here...
    msg += "Not yet implemented"
    raise NotImplementedError(msg)


  #########################
  # Internal methods 
  #########################
  
  def _checkDlsHome(self, fileBlock, working_dir=None):
    """
    Checks if the specified FileBlock is relative (not starting by '/') and if so,
    it tries to complete it with the DLS client working directory.

    This method is required to support the use of relative FileBlocks by client 
    applications (like the CLI for example).

    The DLS client working directory should be read from (in this order):
       - specified working_dir
       - DLS_HOME environmental variable
       - LFC_HOME environmental variable

    If this DLS client working directory cannot be read, or if the specified
    FileBlock is an absolute path, the original FileBlock is returned.

    @param fileBlock: the FileBlock to be changed, as a string
    @param working_dir: the DLS client working directory (FileBlock), as a string
      
    @return: the FileBlock name (possibly with a path prepended) as a string
    """
    if(not fileBlock.startswith('/')):
       lfc_home = working_dir
       if(not lfc_home):
          lfc_home = environ.get("DLS_HOME")
       if(not lfc_home):
          lfc_home = environ.get("LFC_HOME")          
       if(lfc_home):
          fileBlock = lfc_home + '/' + fileBlock
  
    return fileBlock



  def _checkAndCreateDir(self, dir, mode=0755):
    """
    Checks if the specified directory and all its parents exist in the DLS server.
    Otherwise, it creates the whole tree (with the specified mode).

    This method is required to support the automatic creation of parent directories
    within the add() method.

    If the specified directory exists, or if it can be created, the method returns 
    correctly. Otherwise (there is an error creating the specified dir or one of its
    parents), the method returns raises an exception.

    @exception DlsLfcApiError: On error with the DLS catalog

    @param dir: the directory tree to be created, as a string
    @param mode: the mode to be used in the directories creation
    """
    if(dir == "/"):
       # The root directory is already there
       return
#       msg = "Cannot create the root directory"
#       raise DlsLfcApiError(msg)

    dir = dir.rstrip('/')
    
    fstat = lfc.lfc_filestatg()
    parentdir = dir[0:dir.rfind('/')+1]  
    fstat = lfc.lfc_filestatg()
    if(lfc.lfc_statg(dir, "", fstat)<0):
       self._checkAndCreateDir(parentdir, mode)
       guid = commands.getoutput('uuidgen')         
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_mkdirg(",dir,",",guid,",",mode,")"
       if(lfc.lfc_mkdirg(dir, guid, mode) < -1):
          code = lfc.cvar.serrno
          msg = "Error creating parent directory %s: %s" % (dir, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)


  def _addFileBlock(self, dlsFileBlock, **kwd):  
    """
    Adds the specified FileBlock to the DLS and returns its GUID.

    If the DlsFileBlock object includes the GUID of the FileBlock, or
    it is included as "guid" attribute then that value is used in the
    catalog, otherwise one is generated.

    The DlsFileBlock object may include FileBlock attributes. See the
    add method for the list of supported attributes.
    
    The method will raise an exception in the case that there is an
    error adding the FileBlock.

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dlsFileBlock: the dlsFileBlock object to be added to the DLS

    @return: the GUID of the FileBlock (newly set or existing one)
    """

    # Exctract interesting values 
    lfn = dlsFileBlock.name
    lfn = self._checkDlsHome(lfn)
    attrList = dlsFileBlock.attribs
    guid = dlsFileBlock.getGuid()

    # Keywords
    createParent = True
    if(kwd.has_key("createParent")):
       createParent = kwd.get("createParent")
    
    if(self.verb >= DLS_VERB_HIGH):
      print "--addFileBlock("+str(dlsFileBlock)+")"

  # Analyze attribute list
  
    # Defaults
    [mode, filesize, csumtype, csumvalue] = [0775, long(1000), '', '']
 
    # Get what was passed
    for attr in attrList:
       if(attr == "guid"):
          guid=attrList[attr]
          continue
       if(attr == "mode"): 
          if((attrList[attr])[0] == '0'):
             mode = int(attrList[attr], 8)
          else:
             mode = int(attrList[attr])
          continue
       if(attr == "filesize"):     
          filesize=long(attrList[attr])
          continue
       if(attr == "csumtype"):  
          csumtype=attrList[attr]
          continue
       if(attr == "csumvalue"): 
          csumvalue=attrList[attr]
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of FileBlock (%s) unknown." % (attr, lfn)
 
  # Check if entry exists
    fstat=lfc.lfc_filestatg()
    if(lfc.lfc_statg(lfn, "", fstat) <0):
    
       # If it does not exist...
 
       # First, check parents (only for absolute paths!, and if asked for)
       if(createParent and lfn.startswith('/')):
          if(self.verb >= DLS_VERB_HIGH):
             print "--Checking parents of requested file: "+lfn
          parentdir = lfn[0:lfn.rfind('/')+1]
          self._checkAndCreateDir(parentdir, mode)
 
       # Now, create it
       if(not guid):
          guid=commands.getoutput('uuidgen')         
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_creatg(\""+lfn+"\", \""+guid+"\",",mode,")"   
       if(lfc.lfc_creatg(lfn, guid, mode) < 0):
          code = lfc.cvar.serrno
          msg = "Error creating the FileBlock %s: %s" % (lfn, lfc.sstrerror(code))
          if(self.verb >= DLS_VERB_WARN):
            print "Warning: " + msg
          raise DlsLfcApiError(msg, code)
          
       # And set the size and cksum
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_setfsizeg(\""+guid+"\",",filesize,",",csumtype,",",csumvalue,")"
       if (lfc.lfc_setfsizeg(guid, filesize, csumtype, csumvalue)):
          code = lfc.cvar.serrno
          msg = "Error setting the filesize/cksum for the LFN %s: %s" % (lfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)
 
    else:
       # If it exists, get the real GUID
       guid=fstat.guid
 
     # If everything went well, return the GUID
    return guid



  def _addLocationToGuid(self, guid, dlsLocation):  
    """
    Adds the specified location to the FileBlock identified by the specified
    GUID in the DLS.
    
    If the DlsFileBlock object includes the SURL of the location, or
    it is included as "sfn" attribute then that value is used in the catalog,
    otherwise one is generated.

    The GUID should be specified as a string in the format defined for the 
    Universally Unique IDentifier (UUID). Do not include any prefix, such as
    "guid://".

    The DlsLocation object may include location attributes. See the add
    method for the list of supported attributes.
    
    The method will raise an exception in the case that the specified GUID
    does not exist or there is an error adding the location.

    @exception DlsLfcApiError: On error with the DLS catalog

    @param guid: the GUID for the FileBlock, as a string
    @param dlsLocation: the DlsLocation objects to be added as location
    """
    se = dlsLocation.host 
    sfn = dlsLocation.getSurl()
    attrList = dlsLocation.attribs

    # Default values
    if(not sfn):
      sfn = "srm://"+str(se)+'/'+str(guid) 
    [f_type, ptime] = ['V', 0]
 
    # Get what was passed
    for attr in attrList:
       if(attr == "sfn"):      
          sfn = attrList[attr]
          continue
       if(attr == "f_type"):      
          f_type = attrList[attr]
          continue
       if(attr == "ptime"):      
          ptime = int(attrList[attr])
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of location (%s) unknown." % (attr, sfn)
 
  # Register location 
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_addreplica(\""+guid+"\", None, \""+\
              se+"\", \""+sfn+"\",'-',\""+f_type+"\", \"\", \"\")"
    if(lfc.lfc_addreplica(guid, None, se, sfn, '-', f_type, "", "") < 0):
       code = lfc.cvar.serrno
       msg = "Error adding location (%s) for FileBlock (%s): %s" % (se, guid, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
         print "Warning: " + msg
       raise DlsLfcApiError(msg, code)
    
  # Set pin time
    if(ptime):
       if(lfc.lfc_setptime(sfn, ptime)<0):
          code = lfc.cvar.serrno
          msg = "Error setting pin time for location (%s): %s" % (sfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)
 
    return(0)


  def _updateFileBlock(self, dlsFileBlock):  
    """
    Updates the attributes of the specified FileBlock in the DLS.

    If the DlsFileBlock object includes the GUID of the FileBlock, then
    that value is used to identify the FileBlock in the catalog, rather
    than its name. Otherwise the name is used.

    The attributes are specified as a dictionary in the attributes data
    member of the spefied dlsFileBlock object. See the update method for
    the list of supported FileBlock attributes.
    
    The method will raise an exception in the case that the FileBlock does
    not exist or that there is an error updating its attributes.

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dlsFileBlock: the dlsFileBlock object to be added to the DLS
    """
 
    lfn = dlsFileBlock.name 
    lfn = self._checkDlsHome(lfn)
    guid = dlsFileBlock.getGuid()
    attrList = dlsFileBlock.attribs
       
    if(self.verb >= DLS_VERB_HIGH):
       print "--updateFileBlock("+str(dlsFileBlock)+")"
 
    # Check if entry exists
    fstat=lfc.lfc_filestatg()
    if(guid):
      if(lfc.lfc_statg("", guid, fstat) <0):
        code = lfc.cvar.serrno
        msg = "Error accessing FileBlock(%s): %s" % (guid, lfc.sstrerror(code))
        raise DlsLfcApiError(msg, code)
    else:
      if(lfc.lfc_statg(lfn, "", fstat) <0):
        code = lfc.cvar.serrno
        msg = "Error accessing FileBlock(%s): %s" % (lfn, lfc.sstrerror(code))
        raise DlsLfcApiError(msg, code)
 
    # Get current guid, filesize, csumtype, csumvalue
    guid = fstat.guid
    filesize = fstat.filesize
    csumtype = fstat.csumtype
    csumvalue = fstat.csumvalue
    
    # Analyze attribute list to modify what was passed
    update = False
    for attr in attrList:
       if(attr == "filesize"):
          filesize = long(attrList[attr])
          update = True
          continue
       if(attr == "csumtype"):  
          csumtype = attrList[attr]
          update = True
          continue
       if(attr == "csumvalue"): 
          csumvalue = attrList[attr]
          update = True
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Fileblock attribute \""+attr+"\" unknown."
 
    # Set the size and cksum
    if(update):
       if(self.verb >= DLS_VERB_HIGH):
         print "--lfc.lfc_setfsizeg(\""+guid+"\",",filesize,",",csumtype,",",csumvalue,")"
       if(lfc.lfc_setfsizeg(guid, filesize, csumtype, csumvalue)):
         code = lfc.cvar.serrno
         msg = "Error setting the size/cksum for FileBlock(%s): %s" % (guid, lfc.sstrerror(code))
         raise DlsLfcApiError(msg, code)



  def _updateSurl(self, dlsLocation):  
    """
    Updates the attributes of the specified FileBlock location (identified 
    by its SURL).
   
    The DlsLocation object must include internal SURL field set, so that the 
    FileBlock copy can be identified. Otherwise, an exception is raised.

    The attributes are specified as a dictionary in the attributes data member
    of the dlsLocation object. See the update method for the list of supported
    location attributes.
    
    The method will raise an exception in case the specified location does not
    exist or there is an error updating its attributes.

    @exception DlsLfcApiError: On error with the DLS catalog

    @param dlsLocation: the DlsLocation object whose attributes are to be updated
    """

    sfn = dlsLocation.getSurl()
    if(not sfn):
      msg = "Error updating location(%s): the SURL field is not specified" % dlsLocation.name
      raise DlsLfcApiError(msg)
      
    if(self.verb >= DLS_VERB_HIGH):
      print "--updateSurl("+str(dlsLocation)+")"

    attrList = dlsLocation.attribs
 
    # Analyze attribute list to modify what was passed
    update_atime = False
    update_ptime = False
    for attr in attrList:
       if(attr == "atime"):      
          update_atime = True
          continue
       if(attr == "ptime"):
          ptime = int(attrList[attr])
          update_ptime = True
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of location (%s) unknown." % (attr, sfn)
 
    # Set pin time
    if(update_ptime):
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_setptime(\""+sfn+"\",",ptime,")"   
       if(lfc.lfc_setptime(sfn, ptime)<0):
          code = lfc.cvar.serrno
          msg = "Error setting pin time for location(%s): %s" % (sfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)         
 
    # Set access time
    if(update_atime):
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_atime(\""+sfn+"\")"   
       if(lfc.lfc_setratime(sfn)<0):
          code = lfc.cvar.serrno
          msg = "Error accessing access time for location(%s): %s" % (sfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)



  def _deleteFileBlock(self, fileBlock):  
    """
    Removes the specified FileBlock from the DLS catalog.

    The FileBlock is specified as a string, and it can be a primary
    FileBlock name or a symlink. In  the first case, the removal will only
    succeed if the FileBlock has no associated location. The second one
    will succeed even in that case.

    The method will raise an exception in the case that there is an
    error removing the FileBlock (e.g. if the FileBlock does not exist, or
    not all the associated locations have been removed).

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param fileBlock: the FileBlock to be removed, as a string
    """
 
    lfn = fileBlock
   
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_unlink(\""+lfn+"\")"
    if(lfc.lfc_unlink(lfn)<0):
       code = lfc.cvar.serrno
       msg = "Error deleting FileBlock (%s): %s" % (lfn, lfc.sstrerror(code))
       raise DlsLfcApiError(msg, code)


  def _deleteSurl(self, surl): 
    """
    Removes the specified FileBlock location (identified by its SURL),
    
    The method will raise an exception in case the specified location does not
    exist or there is an error removing it.

    The SURL should be specified as a string with a particular format (which
    is sometimes SE dependent). It is usually something like:
    "srm://SE_host/some_string".

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param surl: the SURL to be deleted, as a string
    """
 
    sfn = surl 
 
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_delreplica(\"\", None, \""+sfn+"\")"   
    if(lfc.lfc_delreplica("", None, sfn) < 0):   
       code = lfc.cvar.serrno
       msg = "Error deleting location (%s): %s" % (sfn, lfc.sstrerror(code))
       raise DlsLfcApiError(msg, code)
