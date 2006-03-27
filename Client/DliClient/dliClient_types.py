#
# $Id$
#
# DliClient v 0.1
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 Classes Required to access the Data Location Interface (DLI) web service.

 It uses the Zolera SOAP Infrastructure (ZSI) (http://pywebsvcs.sourceforge.net/) 
 The following code is a simplification of the generated one by ZSI (based on
 the WSDL of the DLI)
"""

import ZSI
from ZSI import client
from ZSI.TCcompound import Struct
import urlparse, types

NAMESPACE = "urn:DataLocationInterface" 
METHOD_NAME = "listReplicas"


class ArrayOfstring(ZSI.TCcompound.Array):
  def __init__(self, name = None, ns = None, **kw):
     ZSI.TCcompound.Array.__init__(self, 'arrayTypeNS:string[]',\
                                   ZSI.TC.String(), pname = name, **kw)

class DliSOAP:

    def __init__(self, addr, **kw):
        # Parse the service endpoint (extract host, port, url, for the Binding)
        netloc = (urlparse.urlparse(addr)[1]).split(":") + [80,]
        if not kw.has_key("host"):
            kw["host"] = netloc[0]
        if not kw.has_key("port"):
            kw["port"] = int(netloc[1])
        if not kw.has_key("url"):
            kw["url"] =  urlparse.urlparse(addr)[2]

        # Create the Binding (connect to the web service)
        self.binding = client.Binding(**kw)


    def listReplicas(self, request):
        """
        Queries the DLI for the list of SURLs for the LFN/GUID/Dataset specified in the request.
        
        @param: request to listReplicasRequest::
          inputData: str
          inputDataType: str

        @return: response from listReplicasResponse::
          _urlList: ArrayOfstring
            _element: str
        """

        # Check correct type is passed
        if not isinstance(request, listReplicasRequest) and\
            not issubclass(listReplicasRequest, request.__class__):
            raise TypeError, "%s incorrect request type" %(request.__class__)
            
        # Query    
        kw = {}
        response = self.binding.Send(None, None, request, soapaction="", **kw)
        response = self.binding.Receive(listReplicasResponseWrapper())

        # Check correct reply was received
        if not isinstance(response, listReplicasResponse) and\
            not issubclass(listReplicasResponse, response.__class__):
            raise TypeError, "%s incorrect response type" %(response.__class__)

        # Return received value
        return response



class listReplicasRequest (ZSI.TCcompound.Struct): 
   """
   Packer of the request information for the DLI service.
   Members inputDataType and inputData must be set to "lfn"/"guid"/"dataset"
   the first, and the filename whose replicas are to be retrieved, the second.
   """

   def __init__(self, name=METHOD_NAME, ns=NAMESPACE):
   
        # Required method arguments
        self.inputDataType = None
        self.inputData = None
   
        # The output name (including namespace)
        oname = None
        if name:
            oname = name
            if ns:
                oname += ' xmlns="%s"' % ns

            ZSI.TC.Struct.__init__(self, listReplicasRequest,\
                                   [\
                                   ZSI.TC.String(pname="inputDataType",aname="inputDataType",optional=1),\
                                   ZSI.TC.String(pname="inputData",aname="inputData",optional=1),\
                                   ],\
                                   pname=name, aname="%s" % name, oname=oname )



def new_listReplicasRequest(file, fileType = "lfn"):
   """
   Helper function to create the request, setting the typecode at the same time.
   """

   listReplicasRequest.typecode = listReplicasRequest()
   aux = listReplicasRequest()
   aux.inputDataType = fileType
   aux.inputData = file
   return aux



class listReplicasResponse (ZSI.TCcompound.Struct): 
    def __init__(self, name="listReplicasResponse", ns=NAMESPACE):
        self.urlList = ArrayOfstring()

        oname = None
        if name:
            oname = name
            if ns:
                oname += ' xmlns="%s"' % ns
            ZSI.TC.Struct.__init__(self, listReplicasResponse,\
                                   [ArrayOfstring( name="urlList", ns=ns ),],\
                                   pname=name, aname="%s" % name, oname=oname )
                                   


class listReplicasResponseWrapper(listReplicasResponse):
    """
    Wrapper class around listReplicasResponse to have the typecode included
    (cannot be done inside the own class definition
    """

    typecode = listReplicasResponse()
    def __init__( self, name=None, ns=None, **kw ):
        listReplicasResponse.__init__(self)
