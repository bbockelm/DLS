#
# $Id$
#
# Dls Client v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module defines some classes that ease the use of the CMS Dataset Location
 Service (DLS) client interface. They serve as arguments and return types for
 the methods defined in the DlsApi class of the dlsApi module.

 This module also includes an exception class, to propagate error when
 manipulating objects of these classes.
"""


#########################################
# Imports 
#########################################
import dlsApi   # for the parent exception

#########################################
# Module globals
#########################################


#########################################
# DlsDataObejctError class
#########################################

class DlsDataObjectError(dlsApi.DlsApiError):
  """
  Exception class for the creation and handling of DLS data objects (classes
  defined in the dlsDataObject module).
  """
  
class TypeError(DlsDataObjectError):
  """
  Exception class for invocations of DlsApi methods with an incorrect argument type.
  """
  
class ValueError(DlsDataObjectError):
  """
  Exception class for invocations of DlsApi methods with an incorrect value as argument.
  """



#########################################
# DlsFileBlock class
#########################################

class DlsFileBlock(object):
  """
  Container of the information relative to a DLS FileBlock. It includes the
  FileBlock name in the catalog and possibly a list of attributes. Internally,
  it may also hold the GUID of the FileBlock in the catalog, but that is not
  always the case.

  The FileBlock name is accessible on the data member "name". It is a string
  that can be read and set. The form of the string should be that imposed by the
  DLS catalog; normally similar to a UNIX file: "/grid/cms/dir1/dir2/file".
  It does not make much sense that this member is set as None.

  The attribute list is accessible on the data member "attribs". It is a
  dictionary that can be read and set. Any attribute key and value can be set
  but only some of them are supported by methods manipulating DlsFileBlock 
  objects and making use of the attribute list (most methods just use the name).
  See documentation of dlsApi.DlsApi methods for details.

  The GUID of the FileBlock should normally be of no importance to the user
  application, and that is why the GUID field is internal. However, since, 
  for some implementations, there may exist situations in which it is
  convenient to be able to manually set the GUID, methods are provided for
  that purpose. Notice that this usage is normally discouraged.

  NOTE: The term GUID here refers to a unique identifier of a FileBlock, and
  not to a simple Grid file. We only use the term GUID for ease of understanding,
  in the assumption that DLS implementations will use an equivalent concept
  for a unique FileBlock identifier. If a concrete implementation has no such
  concept, then the GUID variable is of not use at all.
  """

  ############################################
  # Methods defining the public interface
  ############################################
  
  def __init__(self, name, attributes = None, guid = None):
    """
    Constructor of the class.

    PARAMS:
      name: the FileBlock name, as a string. Required.
      attributes: the FileBlock attribute list, as a dictionary. May be set.
      guid: the FileBlock GUID, as a string. Normally should not be set.
    """

    self.name = name
    self.attribs = attributes
    self.setGuid(guid)


  def setGuid(self, guid):
    """
    Sets the value of the internal variable holding the GUID of the
    FileBlock, which is meaningful only for some DLS implementations.
    This is normally discouraged, but maybe useful for certain cases
    (e.g.: inserting a FileBlock with a pre-generated GUID, or maybe
    achieving better performance for some operations).

    Notice that if this object has been used in some operation with the DLS,
    the real value of the GUID may have been set by the API, and changing it
    may lead to undesired errors and even corruption of the DLS catalog.

    The GUID should be specified as a string in the format defined for the 
    Universally Unique IDentifier (UUID). Do not include any prefix, such as
    "guid://".

    PARAM:
      guid: the GUID for the FileBlock, as a string
    """
    
    self._guid = guid

 
  def getGuid(self):
    """
    Returns the value of the internal variable holding the GUID of the
    FileBlock.
    
    Notice that this variable may not be set, and that it also might be
    (although should not be) different from the real GUID of the FileBlock
    in the DLS. In order to access the DLS and retrieve the GUID stored
    there, use the getGuid method (which is a more expensive operation,
    since it has to query the DLS catalog) of your DLS client API if it
    supports it.

    The returned GUID is a string in the format defined for the Universally
    Unique IDentifier (UUID). It does not include any prefix, such as "guid://".
    
    RETURN: the GUID of this FileBlock, as a string (or None, if not set)
    """
    
    return self._guid



#########################################
# DlsLocation class
#########################################

class DlsLocation(object):
  """
  Container of the information relative to a DLS location. It includes the
  Storage Element name in which a copy of FileBlock is stored, and possibly
  a list of attributes for this copy. Internally, it may also hold the SURL
  of the copy in the catalog, but that is not always the case.

  The name of the SE holding the copy is accessible on the data member "host".
  It is a string that can be read and set. The form of the string should be that
  of a hostname, something like "myhost.mydomain.es".
  It does not make much sense that this member is set as None.

  The attribute list is accessible on the data member "attribs". It is a
  dictionary that can be read and set. Any attribute key and value can be set
  but only some of them are supported by methods manipulating DlsLocation
  objects and making use of the attribute list (most methods just use the
  host). See documentation of dlsApi.DlsApi methods for details.

  The SURL of the FileBlock should normally be of no importance to the user
  application, and that is why the SURL field is internal. However, since 
  there may exist situations in which it is convenient to be able to manually
  set it, methods are provided for that purpose. Notice that  this usage is
  normally discouraged.
  
  NOTE: The term SURL here refers to a identifier of a FileBlock copy, and not
  to a simple Grid replica. In particular, the path part of a SURL makes no
  sense when dealing with FileBlocks. We only use the term SURL for ease of
  understanding, in the assumption that most DLS implementations will use an
  equivalent concept for a FileBlock copy identifier. If a concrete
  implementation has no such concept, then the SURL variable is of not use
  at all.
  """

  ############################################
  # Methods defining the public interface
  ############################################
  
  def __init__(self, host, attributes = None, surl = None):
    """
    Constructor of the class.

    PARAMS:
      host: the location holding a copy of a FileBlock, as a string. Required.
      attributes: the location copy attribute list, as a dictionary. May be set.
      surl: the copy SURL, as a string. Normally should not be set.
    """

    self.host = host
    self.attribs = attributes
    self.setSurl(surl)


  def setSurl(self, surl):
    """
    Sets the value of the internal variable holding the SURL of the copy,
    which is meaningful only for some DLS implementations. This is normally
    discouraged, but maybe useful for certain cases (e.g.: inserting a
    FileBlock with a pre-generated SURL, or maybe achieving better performance
    for some operations).

    Notice that if this object has been used in some operation with the DLS,
    the real value of the SURL may have been set by the API, and changing it
    may lead to undesired errors and even corruption of the DLS catalog.

    The SURL should be specified as a string with a particular format (which
    is sometimes SE dependent). It is usually something like:
    "srm://SE_host/some_string".
    
    PARAM:
      surl: the SURL for the copy, as a string
    """
    
    self._surl = surl

 
  def getSurl(self):
    """
    Returns the value of the internal variable holding the SURL of the copy.
    
    Notice that this variable may not be set, and that it also might be
    (although should not be) different from the real SURL of the copy 
    in the DLS. In order to access the DLS and retrieve the SURL stored
    there,use the getSurl method (which is a more expensive operation,
    since it has to query the DLS catalog) of your DLS client API if it
    supports it.

    The returned SURL is a string with a particular format (which is sometimes
    SE dependent). It is usually something like: "srm://SE_host/some_string".
    
    RETURN: the SURL of this FileBlock, as a string (or None, if not set)
    """
    
    return self._surl



#########################################
# DlsEntry class
#########################################

class DlsEntry(object):
  """
  Container of the information relative to a DLS entry: association between a
  FileBlock name and that FileBlock copies on different locations. It is
  composed of a DlsFileBlock object and a list of DlsLocation objects. Each of
  these objects can contain some attributes, and perhaps GUID and SURL internal
  variables, as described in the classes documentation.

  Objects of this class may be used as high level data containers for the
  methods of the dlsApi.DlsApi class (and derived classes). Some of these
  operations may accept DlsEntry objects where the list of DlsLocation objects
  is an empty list, or may act in a different way if the attribute lists of 
  the DlsFileBlock or DlsLocation objects is not empty. Please refer to the
  documentation of each method for details.
 
  The DlsFileBlock object is accessible on the data member "fileBlock". It can
  be read and set. It does not make much sense that this member is set as None.

  The DlsLocation object list is accessible on the data member "locations".
  It can be read and set. In some situations this member may be set as an
  empty list ([]).
  """

  ############################################
  # Methods defining the public interface
  ############################################
  
  def __init__(self, dlsFileBlock, dlsLocationList = []):
    """
    Constructor of the class.

    PARAMS:
      dlsFileBlock: the FileBlock, as a DlsFileBlock object. Required.
      dlsLocationList: the locations of the copys, as a list of DlsLocation objects.
      
    EXCEPTION:
      TypeError: if the arguments are not of the required type
    """
    self._setFB(dlsFileBlock)
    self._setLoc(dlsLocationList)

    
  def removeLocation(self, host):
    """
    Removes from the locations list the DlsLocation object matching the
    specified location host (equal to the SE name in the DlsLocation object).
    If no location in the list matches, no action is performed. If more than
    one matching object is found (what should not normally happen), each one
    is removed.

    PARAMS:
      host: the location (SE name), as a string.
    """

    for i in self.locations:
      if(i.host == host):
         self.locations.remove(i)


  #############################################################
  # Private: setters and getters (properties) for type checking
  #############################################################

  # Fileblock properties

  def _getFB(self): return self._fB
    
  def _setFB(self, value):
    # Check the type (accept None, although discouraged)
    if(isinstance(value, DlsFileBlock) or (value == None)):
        self._fB = value
    else:
        raise TypeError("dlsFileBlock argument should be a DlsFileBlock object")

  def _delFB(self): del self._fB

  docstr = "FileBlock (DlsFileBlock object)"
  fileBlock = property(_getFB, _setFB, _delFB, docstr)


  # Locations properties

  def _getLoc(self): return self._loc
    
  def _setLoc(self, value):
    # Check if it is a list, and its members are DlsLocation objects
    if(isinstance(value, list)):
       for i in value:
         if(not isinstance(i, DlsLocation)):
            raise TypeError("dlsLocationList should contain DlsLocation objects")
       self._loc = value 
    else:
       raise TypeError("dlsLocationList argument should be a list")
    
  def _delLoc(self): del self._loc

  docstr = "Locations of the FileBlock copies (list of DlsLocation objects)"
  locations = property(_getLoc, _setLoc, _delLoc, docstr)