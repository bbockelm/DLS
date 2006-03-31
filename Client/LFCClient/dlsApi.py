#
# $Id: dlsApi.py,v 1.2 2006/03/30 23:16:55 afanfani Exp $
#
# Dls Client v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module defines the CMS Dataset Location Service (DLS) client interface.
 Python applications interacting with a given DLS catalog implementation will
 use methods defined in the DlsApi class, defined in this module.

 This class serves as an interface definition. It should not be instantiated
 directly, but all instantiable API implementations should provide the code
 for the methods listed here (they could be derived classes).

 This module also includes an API exception class, to propagate error
 conditions when interacting with the DLS catalog.
"""

#########################################
# Imports 
#########################################
from os import environ


#########################################
# Module globals
#########################################
DLS_VERB_NONE = 0    # print nothing
DLS_VERB_INFO = 5    # print info
DLS_VERB_WARN = 10   # print only warnings (to stdout)
DLS_VERB_HIGH = 20   # print warnings (stdout) and error messages (stderr)


#########################################
# DlsApiError class
#########################################

class DlsApiError(Exception):
  """
  Exception class for the interaction with the DLS catalog using the DlsApi class.
  It normally contains a string message (empty by default), and optionally an
  error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.

  Actual (instantiable) implementations of the DLS API may extend this class to 
  define their own exceptions.
  """

  def __init__(self, message="", error_code=0):
    self.msg = message
    self.rc = error_code

  def __str__(self):
    if(self.rc):
       return repr("%i: %s" % (self.rc, self.msg))
    else:
       return repr(self.msg)

class NotImplementedError(DlsApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsApiError):
  """
  Exception class for invocations of DlsApi methods with an incorrect
  value as argument.
  """



#########################################
# DlsApì class
#########################################

class DlsApi(object):
  """
  This class serves as a DLS interface definition. It should not be instantiated
  directly, but all instantiable API implementations should provide the code
  for the methods listed here.

  Unless specified, in the instantiable implementations, all methods that can raise
  an exception will raise one derived from DlsApiError, but further information
  should be provided by those implementations documentation.
  """

  def __init__(self, dls_endpoint = None, verbosity = DLS_VERB_WARN):
    """
    This constructor is used as a general data members initialiser.
    But remember that this class should not be instantiated, since no method
    here is implemented!
 
    Notice that this method allows to have an empty DLS endpoint, but instantiable
    DLS API classes should not allow this. They should deny instantiation if no
    DLS endpoint can be retrieved from (in this order):
         - specified dls_endpoint
         - DLS_ENDPOINT environmental variable
         - DLS catalog advertised in the Information System (if implemented)
         - Possibly some default value (if defined in a given implementation)
  
    The DLS_ENDPOINT variable is checked in this constructor. Other environmental
    variables may be used in particular DLS API implementations.

    The verbosity level affects invocations of all methods in this object. See
    the setVerbosity method for information on accepted values.

    PARAM:
      dls_endpoint: the DLS server to be used, as a string with the form "hostname:port"
      verbosity: value for the verbosity level
    """

    self.setVerbosity(verbosity)

    self.server = dls_endpoint

    if(not self.server):
      self.server = environ.get("DLS_ENDPOINT")


  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Adds the specified DlsEntry object (or list of objects) to the DLS.

    For each specified DlsEntry with a non-registered FileBlock, a new entry is
    created in the DLS, and all locations listed in the DlsLocation list of the object
    are also registered. For the DlsEntry objects with an already registered FileBlock,
    only the locations are added.

    The specified FileBlock is used as it is, without prepending the DLS client working
    directory to it; that may be made beforehand by the caller by means of the
    checkDlsHome method. In some implementations, the caller may also change the
    working directory in the server, in order to use a relative path directly.

    The supported attributes of the FileBlocks and of the locations, which are not null
    are also set for all the new registrations. Unsupported attributes are just ignored.

    Check the documentation of the concrete DLS client API implementation for
    supported attributes.

    If createParent (**kwd) is set to True, then the parent directory tree is checked
    for existence and created if non existing.

    The method will not raise an exception in case there is an error adding a FileBlock
    or location, but will go on with the rest, unless errorTolerant (**kwd) is set 
    to False. This last may be useful when transactions are used. In that case an error
    may cause and automatic rollback so it is better that the method exits after 
    the first failure.

    PARAM:
      dlsEntryList: the DlsEntry object (or list of objects) to be added to the DLS
      createParent(**kwd): boolean (default False) for parent directory creation
      errorTolerant(**kwd): boolean (default True) for raising an exception after failure

    EXCEPTION: 
      On error with the DLS catalog
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg) 
 
  def update(self, dlsEntryList, **kwd):
    """
    Updates the attributes of the specified DlsEntry object (or list of objects)
    in the DLS.

    For each specified DlsEntry, the supported not null attributes of the composing
    DlsFileBlock object and the supported not null attributes of the DlsLocation
    objects of the composing locations list are updated.

    The specified FileBlock is used as it is, without prepending the DLS client working
    directory to it; that may be made beforehand by the caller by means of the
    checkDlsHome method. In some implementations, the caller may also change the
    working directory in the server, in order to use a relative path directly.

    Check the documentation of the concrete DLS client API implementation for
    supported attributes.

    The method will not raise an exception in case there is an error updating the
    attributes of a FileBlock or location, but will go on with the rest, unless
    errorTolerant (**kwd) is set to False. This last may be useful when transactions
    are used. In that case an error may cause and automatic rollback so it is better
    that the method exits after the first failure.

    PARAM:
      dlsEntryList: the DlsEntry object (or list of objects) to be updated
      errorTolerant(**kwd): boolean (default True) for raising an exception after failure

    EXCEPTION: 
      On error with the DLS catalog
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg) 

    
  def clear(self, dlsFileBlockList, **kwd):
    """
    Deletes the DLS entry correponding to the specified DlsFileBlock object
    (or list of objects) from the DLS; i.e.: the FileBlock and all of its
    locations.

    For each specified DlsFileBlock, all the associated locations and also the
    FileBlock (so the whole entry) are removed. This will not be achieved if one
    of the locations is custodial (f_type  == "P"), unless force (*kwd) is set
    to True.

    In order to remove some (or even all) of the locations associated to a
    FileBlock, but not the FileBlock itself, use the delete method instead.

    The specified FileBlock is used as it is, without prepending the DLS client
    working directory to it; that may be made beforehand by the caller by means of
    the checkDlsHome method. In some implementations, the caller may also change
    the working directory in the server, in order to use a relative path directly.

    The method will not raise an exception for every error, but will try to go on
    with all the asked deletions.

    NOTE: In some implementations it is not safe to use this method within a
    transaction. 

    PARAM:
      dlsFileBlockList: the DlsFileBlock object (or list of objects) to be deleted 
      force (**kwd): boolean (default False) for removing custodial locations 

    EXCEPTION: 
      On error with the DLS catalog
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)

    
  def delete(self, dlsEntryList, **kwd):
    """
    Deletes the locations list composing the specified DLSEntry object
    (or list of objects) from the DLS; i.e.: only the locations are removed, 
    the FileBlock itself is kept.

    For each specified DlsEntry, if the composing locations list is null, all 
    the locations associated to the composing DlsFileBlock are deleted. If the
    locations list is not null, then only those there specified will be removed.
    In any case, the FileBlock will be kept.

    In order to remove all the locations associated to a FileBlock and also the
    FileBlock itself, use the clear method instead.

    A location will not be removed if it is custodial (f_type  == "P"), unless
    force (*kwd) is set to True.

    The specified FileBlock is used as it is, without prepending the DLS client
    working directory to it; that may be made beforehand by the caller by means of
    the checkDlsHome method. In some implementations, the caller may also change
    the working directory in the server, in order to use a relative path directly.

    The method will not raise an exception for every error, but will try to go on
    with all the asked deletions.

    NOTE: In some implementations it is not safe to use this method within a
    transaction. 

    PARAM:
      dlsEntryList: the DlsEntry object (or list of objects) to be deleted 
      force (**kwd): boolean (default False) for removing custodial locations 

    EXCEPTION: 
      On error with the DLS catalog
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Returns a DlsEntry object holding the locations in which the specified FileBlock
    is stored, If a list of FileBlock is used as argument, then the method returns
    a list of DlsEntry objects; one object per location specified (in the same order).

    The returned object will have a composing DlsFileBlock object containing the
    specified FileBlock name, and a composing DlsLocation object list holding the 
    corresponding retrieved locations.

    The FileBlocks may be specified as simple strings (hostnames) or as DlsFileBlock
    objects.
    
    The specified FileBlock is used as it is, without prepending the DLS client
    working directory to it; that may be made beforehand by the caller by means of
    the checkDlsHome method. In some implementations, the caller may also change
    the working directory in the server, in order to use a relative path directly.

    If longList (**kwd) is set to true, some location attributes are also included
    in the returned DlsLocation objects. Check the documentation of the concrete
    DLS client API implementation for the list of attributes.

    NOTE: In some implementations, the long listing may be quite more expensive
    (slow) than the normal invocation.

    The method may raise an exception if an error in the DLS operation occurs.

    NOTE: Normally, it makes no sense to use this method within a transaction.

    PARAM:
      fileBlockList: the FileBlock as string/DlsFileBlock (or list of those)
      longList (**kwd): boolean (default false) for the listing of location attributes

    RETURN: a DlsEntry object (or list of objects) containing the locations

    EXCEPTION: 
      On error with the DLS catalog
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
    

  def getFileBlocks(self, locationsList):
    """
    Returns a list of DlsEntry objects holding the FileBlocks stored in the
    specified location. If a list of locations is used as argument, then the method
    returns a list of DlsEntry lists; one list per location specified (in the same
    order).

    The returned object will have a composing DlsFileBlock object containing the
    interesting FileBlock name, and a composing DlsLocation object holding the 
    corresponding specified location.

    The locations may be specified as simple strings (hostnames) or as DlsLocation
    objects.

    The method may raise an exception if an error in the DLS operation occurs.

    NOTE: In some implemenations, this method may be quite more expensive (slow)
          than the getLocations method.

    NOTE: Normally, it makes no sense to use this method within a transaction, so
          please avoid it. 

    PARAM:
      locationsList: the location as string/DlsLocation (or list of those)

    RETURN: a DlsEntry object (or list of objects) containing the FileBlocks

    EXCEPTION: 
      On error with the DLS catalog
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
   
 
  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    For all the FileBlocks registered in the DLS server in the location
    "org_location", changes them so that they no longer exist in "org_location",
    but they are now in "dest_location".

    The method may raise an exception if there is an error in the operation.

    PARAM:
      org_location: original location to be changed (hostname), as a string
      dest_lcoation: new location for FileBlocks (hostname), as a string
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)

        
  def setVerbosity(self, value = DLS_VERB_WARN):
    """
    Sets the verbosity level for all subsequent DlsApi methods.
    
    Currently admitted values are:    
      DLS_VERB_NONE => print nothing
      DLS_VERB_INFO => print info
      DLS_VERB_WARN => print only warnings (to stdout)
      DLS_VERB_HIGH => print warnings (stdout) and error messages (stderr)

    PARAMS: 
      value: the new value for the verbosity level 
    EXCEPTION:
      ValueError: if the specified value is not one of the admitted ones
    """
    admitted_vals = [DLS_VERB_NONE, DLS_VERB_INFO, DLS_VERB_WARN, DLS_VERB_HIGH]
    if(value not in admitted_vals):
      msg = "The specified value is not one of the admitted ones"
      raise ValueError(msg)

    self.verb = value

       
  def checkDlsHome(self, fileBlock, working_dir=None):
    """
    Checks if the specified FileBlock is relative (not starting by '/') and if so,
    it tries to complete it with the DLS client working directory.

    This method is required to support the use of relative FileBlocks by client 
    applications (like the CLI for example).

    The DLS client working directory should be read from (in this order):
       - specified working_dir
       - DLS_HOME environmental variable

    Other environmental variables may be used in particular DLS API implementations.

    If this DLS client working directory cannot be read, or if the specified FileBlock
    is an absolute path, the original FileBlock is returned.

    PARAMS:
      fileBlock: the FileBlock to be changed, as a string
      working_dir: the DLS client working directory (FileBlock), as a string
      
    RETURN: the FileBlock name (possibly with a path prepended) as a string
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg) 



######################
# TODO:
######################
#
# Include the transaction API. This will be e.g. a couple of extra methods 
# on the DlsApi class: "startTrans" and "closeTrans".
#
# Consider the possibility to support a "transaction" flag in the **kwd
# argument of the add and update methods, so that the whole operation of
# the method is performed within a transaction (without explicit control
# from the user). This can be enough (regarding performance), if the
# arguments of these methods are lists of DlsEntry objects.

