#
# $Id$
#
# Dls Client v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server using a LCG File Catalog (LFC) as back-end.

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
import dlsDliClient   # for a fast getLocations implementation

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

class NotImplementedError(DlsApiLfcError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsApiLfcError):
  """
  Exception class for invocations of DlsApi methods with an incorrect value
  as argument.
  """
  
class SetupError(DlsApiLfcError):
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

  def __init__(self, dls_endpoint= None, verbosity = dlsApi.DLS_VERB_WARN):
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

    PARAM:
      dls_endpoint: the DLS server to be used, as a string of form "hostname[:port]"
      verbosity: value for the verbosity level
      
    EXCEPTIONS:
      SetupError: if no DLS server can be found.
    """

    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)
    
    if(not self.server):
      # Do whatever... 

    if(not self.server):
       raise SetupError("Could not set the DLS server to use")

  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.add method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The list of supported attributes for the FileBlocks is:
      guid, mode, filesize, csumtype, csumvalue
    
    The list of supported attributes for the locations is:
      sfn, f_type, ptime

    If the composing DlsFileBlock objects used as argument include the GUID of
    the FileBlock, then that value is used in the catalog. Likewise, if the
    composing DlsLocation objects include the SURL of the FileBlock copies,
    that value is also used. Notice that both uses are discouraged as they may
    lead to catalog corruption if used without care.
    """

    # Implement here...
    pass

 
  def update(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.update method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The list of supported attributes for the FileBlocks is:
      filesize, csumtype, csumvalue
    
    The list of supported attributes for the locations is:
      ptime, atime*

    (*) NOTE: For "atime", the value of the attribute is not considered, but the
              access time is set to current time.
    """

    # Implement here...
    pass

    
  def delete(self, dlsFileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.delete method.
    Refer to that method's documentation.

    Implementation specific remarks:

    NOTE: It is not safe to use this method within a transaction.
    """

    # Implement here...
    pass

    
  def deleteLocations(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.deleteLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    NOTE: It is not safe to use this method within a transaction.
    """

    # Implement here...
    pass

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If longList (**kwd) is set to true, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
      atime, ptime, f_type.

    NOTE: The long listing may be quite more expensive (slow) than the normal
    invocation.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """

    # Implement here...
    pass
    

  def getFileBlocks(self, locationsList):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    NOTE: This method may be quite more expensive (slow) than the getLocations
    method.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    
    # Implement here...
    pass
   
 
  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
    Refer to that method's documentation.
    """

    # Implement here...
    pass

        
  def checkDlsHome(self, fileBlock, working_dir=None):
    """
    Implementation of the dlsApi.DlsApi.checkDlsHome method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The DLS client working directory should be read from (in this order):
       - specified working_dir
       - DLS_HOME environmental variable
       - LFC_HOME environmental variable
    """

    # Implement here...
    pass


  ################################################
  # Other public methods (implementation specific)
  ################################################

  def getGUID(self, fileBlock):
    """
    Returns the GUID used in the DLS for the specified FileBlock, by querying
    the DLS.

    The FileBlock may be specified as a DlsFileBlock object or as a simple string
    (holding the FileBlock name). In the first case, the method returns a
    reference to the object, after having set its internal GUID variable. In the
    second case, the method returns the GUID as a string (without any prefix,
    such as "guid://").

    Notice that this method will only work if the FileBlock has already been
    registered with the DLS catalog.

    The specified FileBlock is used as it is, without prepending the DLS client
    working directory to it; that may be made beforehand by the caller by means of
    the checkDlsHome method. In some implementations, the caller may also change
    the working directory in the server, in order to use a relative path directly.

    PARAMS:
      fileBlock: the FileBlock, as a string/DlsFileBlock object

    RETURN: the GUID, as a string or the DlsFileBlock object with the GUID set

    EXCEPTION:
      On error with the DLS catalog (non-existing FileBlock, etc.)
    """
    
    # Implement here...
    pass

    
  def getSURL(self, dlsEntry):
    """
    Gets the SURLs associated with the specified DlsEntry object. It querys the
    DLS and sets the SURLs (one per each location in the composing location list)
    in the specified DlsEntry object. It returns a reference to this completed
    object. 

    The specified FileBlock is used as it is, without prepending the DLS client
    working directory to it; that may be made beforehand by the caller by means of
    the checkDlsHome method. In some implementations, the caller may also change
    the working directory in the server, in order to use a relative path directly.

    PARAMS:
      dlsEntry: the DlsEntry object for which the SURLs must be retrieved

    RETURN: the DlsEntry object with the SURLs added for each location

    EXCEPTION:
      On error with the DLS catalog (non-existing FileBlock, etc.)
    """

    # Implement here...
    pass


  #########################
  # Internal methods 
  #########################

  def _checkAndCreateDir(self, dir, mode=0755):
    """
    Checks if the specified directory and all its parents exist in the DLS server.
    Otherwise, it creates the whole tree (with the specified mode).

    This method is required to support the automatic creation of parent directories
    within the add() method.

    If the specified directory exists, or if it can be created, the method returns 
    correctly. Otherwise (there is an error creating the specified dir or one of its
    parents), the method returns raises an exception.

    PARAMS: 
     dir: the directory tree to be created, as a string
     mode: the mode to be used in the directories creation
    """

    # Implement here...
    pass


  def _addFileBlock(self, dlsFileBlock):  
    """
    Add the specified FileBlock to the DLS.

    If the DlsFileBlock object includes the GUID of the FileBlock, then
    that value is used in the catalog, otherwise one is generated.

    The DlsFileBlock object may include FileBlock attributes. See the
    add method for the list of supported attributes.
    
    The method will raise an exception in the case that there is an
    error adding the FileBlock.
    
    PARAMS:
      dlsFileBlock: the dlsFileBlock object to be added to the DLS

    EXCEPTIONS:
      On error with the DLS catalog
    """
 
    # Implement here...
    pass


  def _addLocationToGuid(self, guid, dlsLocation):  
    """
    Adds the specified location to the FileBlock identified by the specified
    GUID in the DLS.
    
    The GUID should be specified as a string in the format defined for the 
    Universally Unique IDentifier (UUID). Do not include any prefix, such as
    "guid://".

    The DlsLocation object may include location attributes. See the add
    method for the list of supported attributes.
    
    The method will raise an exception in the case that the specified GUID
    does not exist or there is an error adding the location.

    PARAM:
      guid: the GUID for the FileBlock, as a string
      dlsLocationList: the list of DlsLocation objects to be added as locations

    EXCEPTIONS:
      On error with the DLS catalog
    """
 
    # Implement here...
    pass


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
    
    PARAMS:
      dlsFileBlock: the dlsFileBlock object to be added to the DLS

    EXCEPTIONS:
      On error with the DLS catalog
    """
 
    # Implement here...
    pass


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

    PARAM:
      dlsLocation: the DlsLocation object whose attributes are to be updated

    EXCEPTIONS:
      On error with the DLS catalog
    """
 
    # Implement here...
    pass


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
    
    PARAMS:
     fileBlock: the FileBlock to be removed, as a string

    EXCEPTIONS:
      On error with the DLS catalog
    """
 
    # Implement here...
    pass


  def _deleteSURL(self, surl): 
    """
    Removes the specified FileBlock location (identified by its SURL),
    
    The method will raise an exception in case the specified location does not
    exist or there is an error removing it.

    The SURL should be specified as a string with a particular format (which
    is sometimes SE dependent). It is usually something like:
    "srm://SE_host/some_string".
    
    PARAM:
      surl: the SURL to be deleted, as a string

    EXCEPTIONS:
      On error with the DLS catalog
    """
 
    # Implement here...
    pass
