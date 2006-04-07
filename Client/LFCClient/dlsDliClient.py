#
# $Id: dlsDliClient.py,v 1.3 2006/04/07 09:29:20 delgadop Exp $
#
# DLS Client. $Name:  $.
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
 That class uses the Zolera SOAP Infrastructure (ZSI)
 (http://pywebsvcs.sourceforge.net/). ZSI requires PyXML.
 
 It also contains some exception classes to propagate error conditions
 when interacting with the DLS catalog.

 NOTE: This implementation does not support transactions (which is not
 a big problem, since it is only used for querys).
"""

#########################################
# Imports 
#########################################
import dlsApi
DLS_VERB_HIGH = dlsApi.DLS_VERB_HIGH
DLS_VERB_WARN = dlsApi.DLS_VERB_WARN
from dlsDataObjects import *
import dliClient
from os import environ

#########################################
# Module globals
#########################################


#########################################
# DlsDliClientError class
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
# DlsDliClient class
#########################################

class DlsDliClient(dlsApi.DlsApi):
  """
  This class is an implementation of the a subset of the DLS client interface,
  defined by the dlsApi.DlsApi class. This implementation relies on a DLI being
  supported by the DLS back-end.
  """

  def __init__(self, dli_endpoint = None, verbosity = dlsApi.DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLI endpoint to communicate with
    and the verbosity level, and creates the binding with the DLI interface.
    
    It tries to retrieve that value from several sources (in this order):
    
         - specified dli_endpoint
         - DLS_ENDPOINT environmental variable
         - DLI_ENDPOINT environmental variable
         - DLI endpoint advertised in the Information System (if implemented)

    If the DLI endpoint cannot be set in any of this ways, the instantiation is 
    denied and a SetupError is raised.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.
      
    @exception SetupError: if no DLI can be found.

    @param dli_endpoint: the DLI endpoint to be used, as a string with the form "hostname:port"
    @param verbosity: value for the verbosity level
    """

    # Let the parent set the server (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dli_endpoint, verbosity)
   
    # Create the binding (that tries also DLI_ENDPOINT if server not yet set)
    try:    
       if(self.verb >= DLS_VERB_HIGH):
          print "--DliClient.init(%s)" % self.server
       self.iface = dliClient.DliClient(self.server)
    except dliClient.SetupError, inst:
       raise SetupError("Error creating the binding with the DLI interface: "+str(inst))

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

    If an error occurs in the interaction with the DLI interface, an
    exception is raised. If the error is a SOAP fault, the code field 
    stores the SOAP "faultcode" element.

    @exception DlsDliClientError: On errors in the interaction with the DLI interface
    """

    result = []
    
    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList
    else:
       theList = [fileBlockList]

    # Query the DLI
    try:    
      for fB in theList:
        # Check what was passed (DlsFileBlock or string)
        if(isinstance(fB, DlsFileBlock)):
          lfn = fB.name
        else:
          lfn = fB
        entry = DlsEntry(DlsFileBlock(lfn))

        # Get the list of locations
        locList = []
        if(self.verb >= DLS_VERB_HIGH):
           print "--DliClient.listLocations(%s)" % lfn
        for host in self.iface.listLocations(lfn, fileType = "lfn"):
           locList.append(DlsLocation(host))
        entry.locations = locList
        result.append(entry)

     # Return
      return result

    except dliClient.DliClientError, inst:
      msg = inst.msg
      for i in [inst.actor, inst.detail]:
         if(i):  msg += ". " + str(i)
      e = DlsDliClientError(msg)
      if(inst.faultcode):  
         if(inst.faultstring):  e.code = inst.faultcode + ", " + inst.faultstring
         else:                  e.code = inst.faultcode    
      else:
         if(inst.faultstring):  e.code = inst.faultstring
      raise e

