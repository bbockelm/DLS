#
# $Id$
#
# Dls Client v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module defines a function to retrieve a usable DLS client interface.
 
 Any DLS API implementation must provide the functionality defined by the
 methods of the dlsApi.DlsApi class, or at lest a subset of those (such
 circumstance must be clearly stated in the documentation). Each of the
 implementations will work with a given DLS back-end, and probably not
 with the others. The purpose of this module is to ease the election of
 the concrete API implementation for the client application. This election
 is automated by taking into account environment settings.
 
 The getDlsApi function, defined here, will return a usable API object.
 Refer to the documentation of the function for details.
"""
 

#########################################
# Imports 
#########################################
import dlsApi


#########################################
# Module globals
#########################################
# DlsLfcApi (complete API with LFC back-end)
DLS_TYPE_LFC = "DLS_TYPE_LFC"  

# DlsDliClient (getLocations only API with LFC back-end)
DLS_TYPE_DLI = "DLS_TYPE_DLI" 


#########################################
# getDlsApi function 
#########################################

def getDlsApi(dls_type = None, dls_host = None, verbosity = dlsApi.DLS_VERB_WARN):
  """
  Returns a usable DLS API object, implementing (some of) the methods defined
  in the dlsApi.DlsApi class.

  The election of which concrete implementation is chosen depends on (in this
  order):
    - The specified dls_type argument
    - The DlsType environmental variable
    - DLS catalog type advertised in the Information System (if implemented)
    - The default value defined in this class (currently DLS_LFC)

  If specified, the dls_type argument (or the contents of the DlsType variable)
  should be one of the supported values (defined in this module).
  
  Currently admitted values are:    
    DLS_TYPE_LFC  =>  DlsLfcApi class (complete API with LFC back-end)
    DLS_TYPE_DLI  =>  DlsDliClient class (getLocations only API with LFC back-end)

  The other arguments (dls_host and verbosity) are passed to the constructor 
  of the DLS API as they are. See the dlsApi.DlsApi documentation for details.

  PARAMS: 
    dls_type: the type of API that should be retrieved, see supported values
    dls_host: the DLS server to be used, as a string with the form "hostname:port"
    verbosity: value for the verbosity level, from the supported values
      
  RETURN: a DLS API implementation object
      
  EXCEPTION:
    dlsApi.ValueError: if the specified value is not one of the admitted ones
  """

  admitted_vals = [DLS_TYPE_LFC, DLS_TYPE_DLI]
  default = DLS_TYPE_LFC
  candidate = None
 
  # First set the candidate from the arguments or environment
  if(dls_type):
    candidate = dls_type
  else:
    # try to read the env varible here... 


  # If not set, use the default
  if(not candidate):
    candidate = default

  # Check value is supported
  if(candidate not in admitted_vals):
    msg = "The specified value (%s) is not one of the admitted ones" % candidate
    raise dlsApi.ValueError(msg)


  # If everything ok, return corresponding API
  if(candidate == DLS_TYPE_LFC):
     from dlsLfcApi import DlsLfcApi as api
  if(candidate == DLS_TYPE_DLI):
     from dlsDliClient import DlsDliClient as api

  return api(dls_host, verbosity)
