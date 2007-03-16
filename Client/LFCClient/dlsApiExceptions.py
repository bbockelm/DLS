#
# $Id: dlsApi.py,v 1.20 2007/02/05 15:20:31 delgadop Exp $
#
# DLS Client. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module defines the CMS Dataset Location Service (DLS) client interface
 exception classes. This classes are used by the interface defined in the
 dlsApi module and in any back-end specific implementation of that interface.

 Summary of exceptions, error codes and meaning:

  DlsErrorWithServer        01  Generic error in server interaction.
                                 
  DlsConfigError            05  Error when setting up the interface (configuration)
                                 
  DlsArgumentError          10  Generic wrong argument for a DLS method
                                  
  DlsValueError             11  Generic wrong value as argument for a DLS method
                                 
  DlsInvalidBlockName       12  The specified fileblock is incorrect.
                                 
  DlsInvalidLocation        13  The specified location is incorrect.
                                 
  DlsTypeError              15  Generic wrong type in argument for a DLS method
                                  
  NotImplementedError       20  Exception for methods of the DlsApi that are not implemented 
                                  (and should be by a instantiable API class).
                                  
  DlsConnectionError        25  Problems in the connection with the DLS server
                                  (e.g. host unknown, service unknown).
                                  
  DlsUnexistingBlockError   30  The specified fileblock does not exist.
                                  
  DlsUnexistingReplicaError 31  The specified replica (fileblock + location) does not exist.
                                 
  DlsBlockExistsError       32  The specified fileblock exists already.
                                  
  DlsReplicaExistsError     33  The specified replica (fileblock + location) exists already.
                                  
  DlsNotAccessibleError     35  Generic errors when trying to access a FileBlock
                                  (or directory) or replica.
                                  
  DlsNotAuthorized          40  User not authorized to perform action.
                                 
  DlsAuthenticationError    45  Problems when trying to authenticate user.
                                 
  DlsDataObjectError        100  Error in the creation or handling of DLS data objects
                                  (classes defined in the dlsDataObject module).
                                                                                        
  DlsDataObjectTypeError    101  Exception for invocations of methods of the DlsDataObject
                                  module with an incorrect argument type.
                                  
  DlsDataObjectValueError   102  Exception for invocations of methods of the DlsDataObject
                                  module with an incorrect value as argument.

"""

#########################################
# DlsApiError classes
#########################################

class DlsApiError(Exception):
  """
  Base exception class for the interaction with the DLS catalog using the DlsApi
  class (or another one implemented the same interface). It normally contains a
  string message (empty by default), and optionally an error code (e.g.: if such
  is returned from the DLS server).

  The exception may be printed directly, or its data members accessed.
  Data members are:
   - msg: Error message
   - rc: DLS client API error code (see module's help for meaning)
   - server_rc: If the DLS server provided with an error code, this is included 
                here, probably containing more concrete information on the error.
 
  Other exception classes extending this one are defined to represent more
  concrete error conditions. All of them can be thus caught, by catching objects
  of this class.
  """

  def __init__(self, message="", error_code=1, server_error_code=0):
    self.msg = message
    self.rc = error_code
    self.server_rc = server_error_code

  def __str__(self):
    msg = str(self.msg)
    error = ""
    if(self.rc):
       if(self.server_rc): error = str(self.rc) + " (" + str(self.server_rc) + ")"
       else:               error = str(self.rc)
    msg = (str(self.__class__).split('.')).pop() + " [" + error + "] " + msg
    return msg


class DlsErrorWithServer(DlsApiError):
  """
  Generic error in server interaction.
  rc = 01
  """
  def __init__ (self, message="", error_code=01, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsConfigError(DlsApiError):
  """
  Error when setting up the interface (configuration)
  rc = 05
  """
  def __init__ (self, message="", error_code=05, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsArgumentError(DlsApiError):
  """
  Generic wrong argument for a DLS method 
  rc = 10
  """
  def __init__ (self, message="", error_code=10, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsValueError(DlsArgumentError):
  """
  Generic wrong value as argument for a DLS method 
  rc = 11
  """
  def __init__ (self, message="", error_code=11, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsInvalidBlockName(DlsValueError):
  """
  The specified fileblock is incorrect.
  rc = 12
  """
  def __init__ (self, message="", error_code=12, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsInvalidLocation(DlsValueError):
  """
  The specified location is incorrect.
  rc = 13
  """
  def __init__ (self, message="", error_code=13, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsTypeError(DlsArgumentError):
  """
  Generic wrong type in argument for a DLS method 
  rc = 15
  """
  def __init__ (self, message="", error_code=15, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class NotImplementedError(DlsApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  rc = 20
  """
  def __init__ (self, message="", error_code=20, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsConnectionError(DlsApiError):
  """
  Problems in the connection with the DLS server (e.g. host unknown, service unknown).
  rc = 25
  """
  def __init__ (self, message="", error_code=25, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsUnexistingBlockError(DlsApiError):
  """
  The specified fileblock does not exist.
  rc = 30
  """
  def __init__ (self, message="", error_code=30, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsUnexistingReplicaError(DlsApiError):
  """
  The specified replica (fileblock + location) does not exist.
  rc = 31
  """
  def __init__ (self, message="", error_code=31, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsBlockExistsError(DlsApiError):
  """
  The specified fileblock exists already.
  rc = 32
  """
  def __init__ (self, message="", error_code=32, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsReplicaExistsError(DlsApiError):
  """
  The specified replica (fileblock + location) exists already.
  rc = 33
  """
  def __init__ (self, message="", error_code=33, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)



class DlsNotAccessibleError(DlsApiError):
  """
  Generic errors when trying to access a FileBlock (or directory) or replica.
  rc = 35
  """
  def __init__ (self, message="", error_code=35, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsNotAuthorized(DlsApiError):
  """
  User not authorized to perform action.
  rc = 40
  """
  def __init__ (self, message="", error_code=40, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsAuthenticationError(DlsApiError):
  """
  Problems when trying to authenticate user.
  rc = 45
  """
  def __init__ (self, message="", error_code=45, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)


class DlsDataObjectError(DlsApiError):
  """
  Error in the creation or handling of DLS data objects (classes defined in
  the dlsDataObject module).
  rc = 100
  """
  def __init__ (self, message="", error_code=100, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)

  
class DlsDataObjectTypeError(DlsDataObjectError):
  """
  Exception class for invocations of methods of the DlsDataObject module with an
  incorrect argument type.
  rc = 101
  """
  def __init__ (self, message="", error_code=101, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)

  
class DlsDataObjectValueError(DlsDataObjectError):
  """
  Exception class for invocations of methods of the DlsDataObject module with an
  incorrect value as argument.
  rc = 102
  """
  def __init__ (self, message="", error_code=102, server_error_code=0):
    DlsApiError.__init__(self, message, error_code, server_error_code)

