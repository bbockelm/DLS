#
# $Id: dliClient.py,v 1.1 2006/03/27 11:22:19 delgadop Exp $
#
# DliClient v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module contains the Data Location Interface (DLI) client class.
 Using this class, an application may invoke the listReplica method of 
 a DLI web service.
"""

# NOTE: The exceptions must be defined before the import of dliClient_types
# because they are used there as well

#########################################
# DliClientError classes
#########################################

class DliClientError(Exception):
  """
  Exception class for the interaction with the DLI using the DliClient class.
  It normally contains a string message (empty by default).

  The exception may be printed directly, or its data members accessed.
  """
  
  def __init__(self, message=""):
    self.msg = message

  def __str__(self):
    return repr(self.msg)


class SoapError(DliClientError):
  """
  Exception class for the reception of SOAP faults in the interaction with
  the DLI using the DliClient class.

  It optionally contains some SOAP fault fields (to use if such are returned
  from the DLI):  actor, faultcode, detail, faultstring
  """
  
  def __init__(self, message="", actor=None, faultcode=None, detail=None, faultstring=None):
    self.msg = message
    self.actor = actor
    self.faultcode = faultcode
    self.detail = detail
    self.faultstring = faultstring


class ZsiLibError(DliClientError):
  """
  Exception class for errors returned by the ZSI library (other than
  the SOAP faults).
  """

class ValueError(DliClientError):
  """
  Exception class for invocations of Dli Client methods with an incorrect
  value  as argument.
  """

class TypeError(DliClientError):
  """
  Exception class for invocations of Dli Client methods with an incorrect
  argument type.
  """

class SetupError(DliClientError):
  """
  Exception class for errors when setting up the system (configuration,
  communication errors...)
  """

  
#########################################
# Imports 
#########################################
import dliClient_types
from ZSI import ZSIException as ZSIException
from ZSI import FaultException as ZSIFaultException
from ZSI import Fault as ZSIFault
from os import environ


#########################################
# Module globals
#########################################
DLI_VERB_NONE = 0    # print nothing
DLI_VERB_WARN = 10   # print only warnings (to stdout)
DLI_VERB_HIGH = 20   # print warnings (stdout) and error messages (stderr)


#########################################
# DliClient class
#########################################

class DliClient:
  """
  The DLIClient class can be used to query a Data Location Interface web service.
  It is based on the Zolera SOAP Infrastructure (http://pywebsvcs.sourceforge.net/).
  """

  def __init__(self, dli_endpoint = None, verbosity = DLI_VERB_WARN):
    """
    Constructor of the class. It sets the DLI enpoint to communicate with
    and the verbosity level.
    
    It tries to retrieve that value from several sources (in this order):
    
         - specified dli_endpoint 
         - DLI_ENDPOINT environmental variable
         - DLI endpoint advertised in the Information System (if implemented)

    If the DLI endpoit cannot be set in any of this ways, the instantiation is 
    denied and a SetupError is raised.
 
    The verbosity level affects invocations of all methods in this object. See
    the setVerbosity method for information on accepted values.

    PARAM:
      dli_endpoint: the DLI endpoint to be used, as a string of form "hostname[:port]"
      verbosity: value for the verbosity level
      
    EXCEPTIONS:
      SetupError: if no DLI endpoint can be found.
    """
  
    self.endpoint = dli_endpoint

    if (not self.endpoint):
      self.endpoint = environ.get("DLI_ENDPOINT")

    if (not self.endpoint):
       raise SetupError("Could not set the DLI endpoint to use")
       


  def listSurls(self, file, fileType = "lfn"):
    """
    Returns the list of replica SURLs for the specified Grid file/dataset.
    In order to do this, it queries the listReplica method of the DLI.

    The queried DLI may support all or only some of the following file
    types: "lfn", "guid", "dataset"
    
    PARAMS: 
      file: the LFN/GUID/DataSet Id of the file/dataset to query upon
      fileType: the type of file identifier being used ("lfn"/"guid"/"dataset")
      
    RETURN: The list of SURLs, as a list of strings

    EXCEPTIONS:
       ZsiLibError: On error with ZSI library manipulation (other than SOAP)
       SoapError: On reception of a SOAP fault in the interaction with the DLI 
    """


    try:
       # Get the SOAP binding
       iface = dliClient_types.DliSOAP(self.endpoint)       
    except ZSIException, inst:
       msg = "ZLI error when creating the SOAP binding: " + str(inst)
       raise ZsiLibError(msg)
 
    # Build the SOAP request 
    request = dliClient_types.new_listReplicasRequest(file, fileType)

    try:
       # Query (do not catch exceptions (the caller may be interested))
       response  = iface.listReplicas(request)
    except ZSIFaultException, inst:
       f = inst.fault
       msg = "Error when accessing the DLI: " + f.string
       e = SoapError(msg, f.actor, f.code, f.detail)
       raise e

    # Return
    return response.urlList


    
  def listLocations(self, file, fileType = "lfn"):
    """
    Returns the list of replica locations (SE hostnames).
    In order to do this, the listSurls method is used, and the hostnames
    extracted from the retrieved SURLs.
    
    PARAMS: Same of those of the listSurls method

    RETURN: The list of locations (hostnames), as a list of strings

    EXCEPTION: Same of those of the listSurls method
    """

    # Get the surls (what DLI really returns)
    surls = self.listSurls(file, fileType)
    
    # Build the list (transform SURLs into Locations)
    result = []
    for i in surls:
       host = (i.split("://")[1]).split('/')[0]
       result.append(host)

    # Return the list
    return result



  def setVerbosity(self, value = DLI_VERB_WARN):
    """
    Sets the verbosity level for all subsequent DliClient methods.
    
    Currently admitted values are:    
      DLI_VERB_NONE => print nothing
      DLI_VERB_WARN => print only warnings (to stdout)
      DLI_VERB_HIGH => print warnings (stdout) and error messages (stderr)

    PARAMS: 
      value: the new value for the verbosity level 

    EXCEPTION:
      ValueError: if the specified value is not one of the admitted ones
    """
    admitted_vals = [DLI_VERB_NONE, DLI_VERB_WARN, DLI_VERB_HIGH]
    if(value not in admitted_vals):
      msg = "The specified value is not one of the admitted ones"
      raise ValueError(msg)

    self.verb = value
