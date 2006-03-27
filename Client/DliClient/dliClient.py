#
# $Id$
#
# DliClient v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module contains the DLI client class.
"""

import dliClient_types

class DliClient:
  """
  The DLIClient class can be used to query a Data Location Interface Web Service.
  It is based on the Zolera SOAP Infrastructure (http://pywebsvcs.sourceforge.net/).
  """

  def __init__(self, dli_endpoint = None):
  
    if (dli_endpoint):
       self.endpoint = dli_endpoint
    else:
       # Here we should try to get it:
       #   1st from the DLI_HOST
       #   2nd from the Info System
       #   3rd ... complaint if not found!
       #
       # Check how lcg_utils do so with LFC...
       self.endpoint = "http://lfc-cms.cern.ch:8085"


  def listSurls(self, file, fileType = "lfn"):

    # Get the SOAP binding
    iface = dliClient_types.DliSOAP(self.endpoint)
 
    # Build the SOAP request 
    request = dliClient_types.new_listReplicasRequest(file, fileType)

    # Query (do not catch exceptions (the caller may be interested))
    response  = iface.listReplicas(request)

    # Return
    return response.urlList


    
  def listLocations(self, file, fileType = "lfn"):

    # Get the surls (what DLI really returns)
    surls = self.listSurls(file, fileType)
    
    # Build the list (transform SURLs into Locations)
    result = []
    for i in surls:
       host = (i.split("://")[1]).split('/')[0]
       result.append(host)

    # Return the list
    return result
 
