#
# $Id$
#
# Dls Client v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements part of the CMS Dataset Location Service (DLS)
 client interface as defined by the dlsApi module. This implementation
 relies on a DLS server that supports a Data Location Interface (DLI)
 as a web service.

 The module contains the DlsDliClient class that extends the DlsApi
 class and implements the getLocations method. This reduced 
 functionality is enough for some clients that just query the DLS.
 This reduced implementation can be installed with less requirements
 than a complete DLS client API.

 To perform its function, it uses the dliClient.DliClient class.
 
 It also contains some exception classes to propagate error conditions
 when interacting with the DLS catalog.

 NOTE: This implementation does not support transactions (which is not
 a big problem, since it is only used for querys).
"""

#########################################
# Imports 
#########################################
import dlsApi
import dliClient

#########################################
# Module globals
#########################################


#########################################
# DlsLfcApiError class
#########################################

class DlsDliClientError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the
  DlsDliClient class. It normally contains a string message (empty by default),
  and optionally an error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.
  """

class SetupError(DlsDliClientError):
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
  the dlsApi.DlsApi class. This implementation relies on a DLI being
  supported by the DLS back-end.
  """

  def __init__(self, dli_endpoint = None, verbosity = dlsApi.DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLI endpoint to communicate with
    and the verbosity level.
    
    It tries to retrieve that value from several sources (in this order):
    
         - specified dli_endpoint
         - DLS_ENDPOINT environmental variable
         - DLI_ENDPOINT environmental variable
         - DLI endpoint advertised in the Information System (if implemented)

    If the DLI endpoint cannot be set in any of this ways, the instantiation is 
    denied and a SetupError is raised.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.

    PARAM:
      dli_endpoint: the DLI endpoint to be used, as a string with the form "hostname:port"
      verbosity: value for the verbosity level
      
    EXCEPTIONS:
      SetupError: if no DLI can be found.
    """

    dlsApi.DlsApi.__init__(self, dli_endpoint, verbosity)
    
    if(not self.server):
      # Do whatever... 

    if(not self.server):
       raise SetupError("Could not set the DLI endpoint to use")

  ############################################
  # Methods defining the main public interface
  ############################################

  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The longlist (**kwd) flag is ignored. No attributes can be retrieved
    from the DLI.
    """

    # Implement here...
    pass
    

