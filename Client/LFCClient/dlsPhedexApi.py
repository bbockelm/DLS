#
# $Id: dlsPhedexApi.py,v 1.12 2009/09/21 13:05:39 delgadop Exp $
#
# DLS Client. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server embedded in the CMS Physicx Experimental Data Export
 service (PhEDEx).

 With PhEDEx as back-end, the namespace of fileblocks is shared between
 DLS and PhEDEx, and since it makes more sense that this is handled through
 PhEDEx interfaces, some of the methods of the DLS API are not implemented
 or present a limited functionality in this implementation. For the moment,
 only read operations on the catalog are supported.
 
 The module contains the DlsPhedexApi class that implements most of the
 methods defined in dlsApi.DlsApi class and a couple of extra convenient
 (implementation specific) methods. Python applications interacting with
 a PhEDEx-embedded DLS will instantiate a DlsPhedexApi object and use its
 methods.
"""

#########################################
# Imports 
#########################################
import dlsApi
from dlsApiExceptions import *
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry, DlsDataObjectError
import warnings
warnings.filterwarnings("ignore","Python C API version mismatch for module _lfc",RuntimeWarning)
import sys
from os import environ, uname
from stat import S_IFDIR
from dlsXmlParser import DlsXmlParser
from xml.sax import SAXException, SAXParseException
from urllib2 import (HTTPError, URLError, urlopen, build_opener, 
                     Request, HTTPRedirectHandler)
from urllib2 import __version__ as urllibversion
from urllib import urlencode
from dlsDefaults import DLS_PHEDEX_MAX_BLOCKS_PER_QUERY, DLS_PHEDEX_MAX_SES_PER_QUERY, \
                        DLS_PHEDEX_MAX_BLOCKS_PER_FILE_QUERY, getApiVersion

#########################################
# Module globals
#########################################
DLS_PHEDEX_BLOCKS = "DLS_PHEDEX_BLOCKS"
DLS_PHEDEX_FILES = "DLS_PHEDEX_FILES"
DLS_PHEDEX_ALL_LOCS = "DLS_PHEDEX_ALL_LOCS"

#unamev = uname()
#USERAGENT = {'User-Agent': 'dls-client/%s (CMS) urllib2/%s %s/%s (%s)' %\
#             (getApiVersion(), urllibversion, unamev[0], unamev[2], unamev[4])}



#########################################
# Helper classes
#########################################
class DlsRedirectHandler(HTTPRedirectHandler):
    """
    Redirector that keeps POSTs as POSTs (including all data that
    belonged originally to the request) unless we are changing
    the last part of the URL (data service API call). Infinite loops 
    are also avoided but that's done by our parent (implicit here).

    Notice that GET redirections go the new URL indicated by the server
    without adding any URL arguments ('?') originally present (they should
    have been preserved by the server).
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """
        Method that will be called by urllib2 when a redirection occur, to 
        see what to do.
        """
        m = req.get_method()
        if ((code in (301, 302, 303, 307) and m in ("GET", "HEAD")) or
            (code in (300, 302, 303) and m == "POST")):

            newurl = newurl.replace(' ', '%20')            
            oldurl = req.get_full_url()
            newApiCall = newurl.split('/')[-1].split('?')[0]
            oldApiCall = oldurl.split('/')[-1].split('?')[0]
#            #
#            print 'newurl:',newurl
#            print 'req.data:',req.data
#            print 'req.get_full_url():', req.get_full_url()
#            print
#            #
            if oldApiCall != newApiCall:
                msg = "Not allowed redirection: trying to change data service"
                msg += " API call from '%s' to '%s'" % (oldApiCall, newApiCall)
                raise HTTPError(req.get_full_url(), code, msg, headers, fp)

            return Request(newurl, req.data, headers = req.headers,
                           origin_req_host = req.get_origin_req_host(),
                           unverifiable = True)
        else:
            raise HTTPError(req.get_full_url(), code, msg, headers, fp)


#########################################
# DlsPhedexApi class
#########################################

class DlsPhedexApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on DLS information
  being embedded in the CMS Physicx Experimental Data Export service (PhEDEx).

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsApiError.
  """

  def __init__(self, dls_endpoint=None, verbosity=dlsApi.DLS_VERB_WARN, **kwd):
    """
    Constructor of the class. It creates a DlsXmlParser object using the
    specified dls_endpoint and optionally additional parameters in **kwd. 
    It also sets the verbosity of the DLS API.
    
    At the time of writing, the minimum argument required to build the DBS
    interface is the DBS server endpoint. For others, please check DBS
    client documentation.
    
    The server endpoint is got from a string in the URL form, usually:
    "http[s]://hname[:port]/path/to/DLS". This API tries to retrieve that
    value from several sources (in this order):   
         - specified dls_endpoint 
         - DLS_ENDPOINT environmental variable
         - DLS_PHEDEX_ENDPOINT environmental variable
         - DLS catalog advertised in the Information System (if implemented)
         
    If the necessary arguments cannot be obtained in any of these ways, the
    instantiation is denied and a DlsConfigError is raised.
 
    The verbosity level affects invocations of all methods in this object.
    See the dlsApi.DlsApi.setVerbosity method for information on accepted values.

    If the checkEndpoint (**kwd) is set to True, the provided endpoint is
    checked. This makes sense where more than one query are to be made next.
    For simple queries, any error in the endpoint will be noticed in the query
    itself, so the check would be redundant and not efficient.

    The uaClientsList and uaFlexString arguments (**kwd) may be respectively used 
    to specify a list of client,version pairs of strings identifying clients of DLS 
    (e.g: [['ProdAgent', '3.45']], and a string of free format (e.g. 'Operator John').
    Both will be included in the UserAgent string used in identification with the 
    PhEDEx data service, as described in the _userAgent method's docstring.
      
    @exception DlsConfigError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string "hname[:port]/path/to/DLS"
    @param verbosity: value for the verbosity level
    @param kwd: 
       - checkEndpoint: Boolean (default False) for testing of the DLS endpoint
       - uaClientsList: List of client,version pairs of strings for the UserAgent string
       - uaFlexString: String, flexible (format-free) part of the UserAgent string
    """

    # Keywords
    checkEndpoint = False
    if(kwd.has_key("checkEndpoint")):
       checkEndpoint = kwd.get("checkEndpoint")

    uaFlexString = ''
    if(kwd.has_key("uaFlexString")):
       uaFlexString = kwd.get("uaFlexString")

    uaClientsList = []
    if(kwd.has_key("uaClientsList")):
       uaClientsList = kwd.get("uaClientsList")

    # Let the parent set the server endpoint (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)

    # If the server is not there yet, try from DLS_PHEDEX_ENDPOINT
    if(not self.server):
      self.server = environ.get("DLS_PHEDEX_ENDPOINT")

    # If still not there, give up 
    if(not self.server):
       raise DlsConfigError("Could not set the DLS server to use")

    self.server = self.server.strip('/')
  
    # Create the parser
    self.parser = DlsXmlParser()

    # Set the default number of elements for bulk queries
    self.setBlocksPerQuery ( DLS_PHEDEX_MAX_BLOCKS_PER_QUERY )
    self.setBlocksPerFileQuery ( DLS_PHEDEX_MAX_BLOCKS_PER_FILE_QUERY )
    self.setLocsPerQuery ( DLS_PHEDEX_MAX_SES_PER_QUERY )

    # Store the internal UserAgent variables
#    import pdb; pdb.set_trace()

    self.setUserAgentFlexString(uaFlexString)
    self._uaVersions = []
    self.addUserAgentClient('Python', sys.version.split()[0])
    self.addUserAgentClient('urllib2', urllibversion)
    self.addUserAgentClient('dls-client', getApiVersion())
    for client, version in uaClientsList:
        self.addUserAgentClient(client, version)
    unamev = uname()
    self._uaArch = ' (CMS) %s/%s (%s)' % (unamev[0], unamev[2], unamev[4])

    # Check that the provided URL is OK (by listing an inexisting fileblock)
    if(checkEndpoint):
      try:
#         self._debug("Checking endpoint %s..." % self.server)
         urlv = self._buildXmlUrl(self.server, DLS_PHEDEX_BLOCKS, ["-"])
#         req = Request(urlv[0] + '?' + urlencode(urlv[1]), None, USERAGENT)
         req = Request(urlv[0] + '?' + urlencode(urlv[1]), None, self._userAgent())
         opener = build_opener(DlsRedirectHandler())
         url = opener.open(req)
         #
         self.parser.xmlToEntries(url)
      except Exception, inst:
         msg = "Could not set the interface to the DLS server. "
         msg += "Error when accessing provided PhEDEx URL: %s" %   (self.server)
         self._mapException(inst, msg, msg, errorTolerant = False)

    

  ############################################
  # Methods defining the main public interface
  ############################################

  
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If longList (**kwd) is set to True, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
     - bytes
     - files
     - node
     - node_id
     - time_create
     - time_update
     - complete

    If instead of a FileBlock name, a pattern is provided (with '%' or '*' as
    wildcards), the method includes in the returned list the DlsEntry objects
    for all the matching FileBlocks.

    In the current implementation the cost of doing a long listing
    is the same as doing a normal one.

    The showProd and showCAF flags are taken into account and if not set
    to True some FileBlock replicas are filtered out. Likewise, if the
    subscribed or custodial flags are set to True, some replicas are not shown.

    The following keyword flags are ignored: session.
    """
    # Keywords
    longList = False 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    errorTolerant = False
    if(kwd.has_key("errorTolerant")):   errorTolerant = kwd.get("errorTolerant")

    subscribed = False
    if(kwd.has_key("subscribed")):   subscribed = kwd.get("subscribed")
    custodial = False
    if(kwd.has_key("custodial")):   custodial = kwd.get("custodial")

    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")
    showCAF= False
    if(kwd.has_key("showCAF")):   showCAF = kwd.get("showCAF")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]

    # Loop on the entries to build a list of blocks to ask for
    lfnList = []
    for fB in theList:
       # Check what was passed (DlsFileBlock or string)
       if(isinstance(fB, DlsFileBlock)):
         lfn = fB.name
       else:
         lfn = fB

       # If '/' or '*' or '%' is given, we want all blocks back
       if (lfn=='/') or (lfn == '*') or (lfn == '%'):
          lfn = '/%'

       lfnList.append(('block', lfn)) 
       
    multiList = self._toMultiList(lfnList, DLS_PHEDEX_MAX_BLOCKS_PER_QUERY)
    urlbase = self.server + '/blockReplicas'

    
    msg = "Number of arguments per bulk query: "
    for i in multiList: msg += str(len(i)) + ' '
    self._debug(msg)

    arglist2 = []
    # flags that could be added: incomplete, updated_since, created_since
    arglist2.append(('complete', 'y'))
    if subscribed:
       arglist2 += [('subscribed','y')]
    if custodial:
       arglist2 += [('custodial','y')]
    if not (showProd and showCAF):
       arglist2 += [('op','node:and')]
    if not showProd:
       arglist2 += [('node','!T0*'), ('node','!T1*')]
    if not showCAF:
       arglist2 += [('node','!T2_CH_CAF')]
    self._debug("Using PhEDex xml url: " + urlbase + ' ' + str(arglist2))
  
    eList = []
    partList = []
    for arglist in multiList:
      if not arglist: continue
      # Get the locations
      try:  
         # Build the xml query to use
#         req = Request(urlbase, urlencode(arglist + arglist2), USERAGENT)
         req = Request(urlbase, urlencode(arglist + arglist2), self._userAgent())
         opener = build_opener(DlsRedirectHandler())
         url = opener.open(req)
         partList = self.parser.xmlToEntries(url)
      except Exception, inst:
         msg = "Error retrieving locations"
         msg_w = msg + ". Skipping"
         self._mapException(inst, msg, msg_w, errorTolerant)
      # Add to the total 
      eList += partList

    # Check if the list was empty
    if(not eList):
       msg = "No existing fileblock matching %s" % (str(lfnList))
       self._warn(msg)

    # Return what we got
    return eList



  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The showProd and showCAF flags are taken into account and if not set 
    to True some FileBlock replicas are filtered out. Likewise, if the
    subscribed or custodial flags are set to True, some replicas are not shown.

    The cmsSite flag is taken into account, and if included locations
    are considered to be CMS site names rather than SE hostnames (in the
    case of arguments being DlsLocation objects, the 'node' field in the
    attribs dictionary is used as location).

    The following keyword flags are ignored: session.
    """

    # Keywords
    cmsSite = False
    if(kwd.has_key("cmsSite")):   cmsSite = kwd.get("cmsSite")

    subscribed = False
    if(kwd.has_key("subscribed")):   subscribed = kwd.get("subscribed")
    custodial = False
    if(kwd.has_key("custodial")):   custodial = kwd.get("custodial")

    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")
    showCAF= False
    if(kwd.has_key("showCAF")):   showCAF = kwd.get("showCAF")

    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList 
    else:
       theList = [locationList]

    
    # Loop on the entries to build a list of locations to ask for
    locList = []
    for loc in theList:
       
       # Check what was passed (DlsLocation or string)
       if(isinstance(loc, DlsLocation)):
          if cmsSite:
             host = loc.attribs['node']
          else:
             host = loc.host
       else:
          host = loc

       if cmsSite:
          locList.append(('node',host)) 
       else:
          locList.append(('se',host)) 


    multiList = self._toMultiList(locList, DLS_PHEDEX_MAX_SES_PER_QUERY)
    urlbase = self.server + '/blockReplicas'

    msg = "Number of arguments per bulk query: "
    for i in multiList: msg += str(len(i)) + ' '
    self._debug(msg)

    arglist2 = []
    # flags that could be added: incomplete, updated_since, created_since
    arglist2.append(('complete', 'y'))
    if subscribed:
       arglist2 += [('subscribed','y')]
    if custodial:
       arglist2 += [('custodial','y')]
    if not cmsSite:
        if not (showProd and showCAF):
           arglist2 += [('op','node:and')]
        if not showProd:
           arglist2 += [('node','!T0*'), ('node','!T1*')]
        if not showCAF:
           arglist2 += [('node','!T2_CH_CAF')]
    self._debug("Using PhEDex xml url: " + urlbase + ' ' + str(arglist2))

    eList = []
    partList = []
    for arglist in multiList:
      if not arglist: continue
      # Get the blocks
      try:  
         # Build the xml query to use
#         req = Request(urlbase, urlencode(arglist + arglist2), USERAGENT)
         req = Request(urlbase, urlencode(arglist + arglist2), self._userAgent())
         opener = build_opener(DlsRedirectHandler())
         url = opener.open(req)
         partList = self.parser.xmlToEntries(url)
      except Exception, inst:
         msg = "Error retrieving FileBlocks for %s" % (locList)
         self._mapException(inst, msg, msg, False)
      # Add to the total 
      eList += partList
       
    # Return what we got
    return eList



  def listFileBlocks(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.listFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx FileBlock namespace is not hierarchical, there is no concept
    of FileBlock directory, and recursive listing makes no sense.

    If instead of a FileBlock name, a pattern (with '%' as wildcard) is provided,
    the method includes in the returned list the DlsEntry objects for all the
    matching FileBlocks.

    If longList (**kwd) is set to True, the attributes returned with
    the FileBlock are the following:
      - bytes
      - files 
      - is_open
      - id

    The following keyword flags are ignored: session, recursive.
    """
    # Keywords
    longList = False 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]


    # Loop on the entries to build a list of blocks to ask for
    lfnList = []
    for fB in theList:
       # Check what was passed (DlsFileBlock or string)
       if(isinstance(fB, DlsFileBlock)):
         lfn = fB.name
       else:
         lfn = fB

       # If '/' or '*' or '%' is given, we want all blocks back
       if (lfn=='/') or (lfn == '*') or (lfn == '%'):
          lfn = '/%'

       lfnList.append(('block',lfn))

    multiList = self._toMultiList(lfnList, DLS_PHEDEX_MAX_BLOCKS_PER_QUERY)
    urlbase = self.server + '/blockReplicas'

    msg = "Number of arguments per bulk query: "
    for i in multiList: msg += str(len(i)) + ' '
    self._debug(msg)

    arglist2 = []
    # flags that could be added: incomplete, updated_since, created_since
    
    # We list incomplete blocks as well, don't we?
    #arglist2.append(('complete', 'y'))
    self._debug("Using PhEDex xml url: " + urlbase + ' ' + str(arglist2))

    bList = []
    partList = []
    for arglist in multiList:
      if not arglist: continue
      # Get the blocks
      try:  
#         req = Request(urlbase, urlencode(arglist + arglist2), USERAGENT)
         req = Request(urlbase, urlencode(arglist + arglist2), self._userAgent())
         opener = build_opener(DlsRedirectHandler())
         url = opener.open(req)
         partList = self.parser.xmlToBlocks(url)
      except Exception, inst:
         msg = "Error retrieving fileblock information"
         msg_w = msg + ". Skipping"
         self._mapException(inst, msg, msg_w, errorTolerant = True)
      # Add to the total 
      bList += partList
    
    # Check if the list was empty
    if(not bList):
       msg = "No existing fileblock matching %s" % (str(lfnList))
       msg_w = msg + ". Skipping"
       self._warn(msg_w)

    # Return what we got
    return bList


  def getFileLocs(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileLocs method.
    Refer to that method's documentation.

    Implementation specific remarks:

    No wildcards ('*' or '%') are accepted in the FileBlock names.
    
    The showProd and showCAF flags are taken into account and if not set
    to True some FileBlock replicas are filtered out. Likewise, if the
    subscribed or custodial flags are set to True, some replicas are not shown.

    The following keyword flags are ignored: session.
    """
    
    # Keywords
    errorTolerant = False
    if(kwd.has_key("errorTolerant")):   errorTolerant = kwd.get("errorTolerant")
    
    subscribed = False
    if(kwd.has_key("subscribed")):   subscribed = kwd.get("subscribed")
    custodial = False
    if(kwd.has_key("custodial")):   custodial = kwd.get("custodial")

    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")
    showCAF= False
    if(kwd.has_key("showCAF")):   showCAF = kwd.get("showCAF")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]

    # Loop on the entries to build a list of blocks to ask for
    lfnList = []
    for fB in theList:
       # Check what was passed (DlsFileBlock or string)
       if(isinstance(fB, DlsFileBlock)):
         lfn = fB.name
       else:
         lfn = fB

       # Check that the passed FileBlock is not a pattern
       if (lfn.find('*') != -1) or (lfn.find('%') != -1):
         msg = "FileBlock patterns (containing '*' or '%%' wildcards) are not acceptable: "+lfn
         raise DlsInvalidBlockName(msg)
         
       if lfn: lfnList.append(('block', lfn)) 
       
    if not lfnList: 
       msg = "Error querying for file replicas. A FileBlock must be specified"
       raise DlsArgumentError(msg)

    multiList = self._toMultiList(lfnList, DLS_PHEDEX_MAX_BLOCKS_PER_FILE_QUERY)
    urlbase = self.server + '/fileReplicas'

    msg = "Number of arguments per bulk query: "
    for i in multiList: msg += str(len(i)) + ' '
    self._debug(msg)

    arglist2 = []
    # flags that could be added: incomplete, updated_since, created_since
    arglist2.append(('dist_complete', 'y'))
    if subscribed:
       arglist2 += [('subscribed','y')]
    if custodial:
       arglist2 += [('custodial','y')]
    if not (showProd and showCAF):
       arglist2 += [('op','node:and')]
    if not showProd:
       arglist2 += [('node','!T0*'), ('node','!T1*')]
    if not showCAF:
       arglist2 += [('node','!T2_CH_CAF')]
    self._debug("Using PhEDex xml url: " + urlbase + ' ' + str(arglist2))

    flList = []
    partList = []
    for arglist in multiList:
      if not arglist: continue
      # Get the file replicas
      try:  
         # Build the xml query to use
#         req = Request(urlbase, urlencode(arglist + arglist2), USERAGENT)
         req = Request(urlbase, urlencode(arglist + arglist2), self._userAgent())
#         url = urlopen(urlbase, urlencode(arglist + arglist2))
#         url = urlopen(req)
         opener = build_opener(DlsRedirectHandler())
         url = opener.open(req)
         #
         partList = self.parser.xmlToFileLocs(url)
      except Exception, inst:
         msg = "Error getting files for FileBlock in DLS"
         msg_w = msg + ". Skipping"
         self._mapException(inst, msg, msg_w, errorTolerant)
      # Add to the total
      flList += partList
    
    # Return what we got
    return flList


  def getAllLocations(self, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getAllLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The following keyword flags are ignored: session.
    """
 

    # showEmpty
    urlbase = self.server + "/nodes"
    urlargs = []
    urlargs.append(('noempty','y'))
 
    # Get the locations
    try:  
       self._debug("Using PhEDex xml url: " + urlbase + str(urlargs))
       urlv = self._buildXmlUrl(self.server, DLS_PHEDEX_BLOCKS, ["-"])
#       req = Request(urlbase + '?' + urlencode(urlargs), None, USERAGENT)
       req = Request(urlbase + '?' + urlencode(urlargs), None, self._userAgent())
       opener = build_opener(DlsRedirectHandler())
       url = opener.open(req)
       locList = self.parser.xmlToLocations(url)
    except Exception, inst:
       msg = "Error getting all locations in DLS"
       self._mapException(inst, msg, msg, errorTolerant = False)

    # Return what we got
    return locList


  def dumpEntries(self, dir = "/", **kwd):
    """
    Implementation of the dlsApi.DlsApi.dumpEntries method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx FileBlock namespace is not hierarchical, there is no concept
    of FileBlock directory, and recursive listing makes no sense. The dir
    argument is interpreted as representing a FileBlock name pattern (with
    '%' as wildcard), and the matching FileBlocks and associated locations
    are dumped.

    The showProd and showCAF flags are taken into account and if not set 
    to True some FileBlock replicas are filtered out. Likewise, if the
    subscribed or custodial flags are set to True, some replicas are not shown.

    The following keyword flags are ignored: session, recursive.
    """

    # Keywords
    subscribed = False
    if(kwd.has_key("subscribed")):   subscribed = kwd.get("subscribed")
    custodial = False
    if(kwd.has_key("custodial")):   custodial = kwd.get("custodial")

    showProd = False
    if(kwd.has_key("showProd")):   showProd = kwd.get("showProd")
    showCAF= False
    if(kwd.has_key("showCAF")):   showCAF = kwd.get("showCAF")

    # This can be achieved by listing the fBs and associated locations
    result = self.getLocations(dir, longList=False, errorTolerant=True, \
                               subscribed=subscribed, custodial=custodial, \
                               showProd=showProd, showCAF=showCAF)

    # Return what we got
    return result



  def startSession(self):
    """
    Implementation of the dlsApi.DlsApi.startSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    Since PhEDEx does not support sessions, this method does nothing.
    """
    self._debug("Starting session with %s (no action)" % (self.server))

 
  def endSession(self):
    """
    Implementation of the dlsApi.DlsApi.endSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    Since PhEDEx does not support sessions, this method does nothing.
    """
    self._debug("Ending session with %s (no action)" % (self.server))
  
 
  def startTrans(self):
    """
    Implementation of the dlsApi.DlsApi.startTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx does not support transactions, this method does nothing.
    """
    self._debug("Starting transaction with %s (no action)" % (self.server))


  def endTrans(self):
    """
    Implementation of the dlsApi.DlsApi.endTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx does not support transactions, this method does nothing.
    """
    self._debug("Ending transaction with %s (no action)" % (self.server))
  
  
  def abortTrans(self):
    """
    Implementation of the dlsApi.DlsApi.abortTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Since PhEDEx does not support transactions, this method does nothing.
    """
    self._debug("Aborting transaction with %s (no action)" % (self.server))
 

  def setBlocksPerQuery(self, nblocks):
    """
    Sets the number of blocks to query for in each bulk getLocations query
    
    @param nblocks: number of blocks to query for in each bulk query 

    @exception: raises ValueError, if nblocks is not a positive integer
    """
    if (not type(nblocks) == int) and (nblocks > 0):
       raise DlsValueError("Argument of setBlocksPerQuery must be a positive integer")
    self.blocksPerQuery = nblocks


  def setBlocksPerFileQuery(self, nblocks):
    """
    Sets the number of blocks to query for in each bulk getFileLocs query
    
    @param nblocks: number of blocks to query for in each bulk query 

    @exception: raises ValueError, if nblocks is not a positive integer
    """
    if (not type(nblocks) == int) and (nblocks > 0):
       raise DlsValueError("Argument of setBlocksPerFileQuery must be a positive integer")
    self.blocksPerFileQuery = nblocks


  def setLocsPerQuery(self, nlocs):
    """
    Sets the number of locations to query for in each bulk getFileBlocks query
    
    @param nlocs: number of blocks to query for in each bulk query 

    @exception: raises ValueError, if nlocs is not a positive integer
    """
    if not ((type(nlocs) == int) and (nlocs > 0)):
       raise DlsValueError("Argument of setBlocksPerQuery must be a positive integer")
    self.locsPerQuery = nlocs

 
  def addUserAgentClient(self, client, version):
    """
    Adds a client/version pair to the list that will be included in the 
    UserAgent string to be used as identifier when contacting the PhEDEx
    data service.

    @param client: the client name (string)
    @param version: a string (usually of the form 'x.y') with version of the client
    """
    self._uaVersions.insert(0, [client, version])


  def setUserAgentFlexString(self, text):
    """
    Sets the client-provided flexible (format-free) part of the UserAgent 
    string to be used as identifier when contacting the PhEDEx data service.
    """
    self._uaFlex = text


  ##################################
  # Private methods
  ##################################

  def _userAgent(self):
    """
    Builds a UserAgent string to be used as identifier when contacting the PhEDEx
    data service. It will be built by concatenating the following elements:
     - Flexible client-provided string
     - Series of client/version pairs (client-provided, DLS, urllib, python)
     - Architecture string (preceded by the string '(CMS)')

    The client-provided parts can be set at DlsPhedexApi instantiation time or
    with the 'addUserAgentClient' and 'setUserAgentFlexString' methods.
    """
    result = ''
    if self._uaFlex:
        result += self._uaFlex + ', '
    result += ' '.join('/'.join(x) for x in self._uaVersions) + self._uaArch

    return  {'User-Agent': result}


  def _toMultiList(self, list, num):
    """
    Gets a list of elements and returns a list of lists. These are the result
    of dividing the original list elements into lists of not more than <num> 
    elements.
    """
    multi = []
    l = len(list)
    lmulti = (l-1) / num + 1
    for i in xrange(lmulti):
       multi.append(list[i*num : (i+1)*num])
    return multi


  # The following function is really only used by the constructor
  # For the other functions (that use lists) it is more efficient
  # to inline it.
  def _buildXmlUrl(self, xml_base, type, blocks=None, ses=None, **kwd):
    """
    Returns an appropriate URL base and arguments for a query to PhEDEx with the 
    specified parameters: [urlbase, urlargs]. This can be used with urlopen.

    @param xml_base: base URL for the PhEDEx location query service
    @param type: the type of query to make: DLS_PHEDEX_BLOCKS, DLS_PHEDEX_FILES, DLS_PHEDEX_ALL_LOCS
    @param blocks: list of FileBlock names (as str tuple) to query; wildcard '%' or '*' allowed
    @param ses:  list of location names (as str tuple) to query; use None for 'any'
    @param **kwd: Flags:
      - incomplete: boolean (default False) for getting incomplete blocks also returned 
      - updated_since: unix timestamp, for replicas updated since specified time
      - created_since: unix timestamp, for replicas cretated since specified time
      - showProd: boolean (default False) for turning off the filtering of prod-only replicas
      - showCAF: boolean (default False) for turning off the filtering of CAF replicas
      - showEmpty: boolean (default False) for showing empty locations (DLS_PHEDEX_ALL_LOCS)
    """

    admitted_vals = [DLS_PHEDEX_BLOCKS, DLS_PHEDEX_FILES, DLS_PHEDEX_ALL_LOCS]
    if(type not in admitted_vals):
      msg = "Error building the PhEDEx xml url."
      msg += "The specified type of query is not one of the admitted values"
      raise DlsValueError(msg)

    urlbase = xml_base
    
    # Most common case (querying blocks or locations)
    if(type == DLS_PHEDEX_BLOCKS):
    
       urlbase += "/blockReplicas"
       urlargs = []
       
       if(not (blocks or ses)): 
         msg = "Error building the PhEDEx xml url. A FileBlock or a location must be specified"
         raise DlsArgumentError(msg)
       
       if(blocks):
         for block in blocks:
           urlargs.append(('block', block.replace('*','%')))
       
       if(ses):
          for se in ses:
            urlargs.append(('se', se))
   
       if not (kwd.has_key("incomplete") and kwd.get("incomplete")):
          urlargs.append(('complete', 'y'))
   
       if(kwd.has_key("updated_since")):
          urlargs.append(('updated_since', kwd.get("updated_since")))
       
       if(kwd.has_key("created_since")):
          urlargs.append(('created_since'+kwd.get("created_since")))

       showProd = (kwd.has_key("showProd") and kwd.get("showProd"))
       showCAF = (kwd.has_key("showCAF") and kwd.get("showCAF"))
       if not (showProd and showCAF):
          urlargs.append(('op','node:and'))
       if not showProd:
          urlargs.append(('node','!T0*'))
          urlargs.append(('node','!T1*'))
       if not showCAF:
          urlargs.append(('node','!T2_CH_CAF'))
  
    # Query for individual files in a given block 
    if(type == DLS_PHEDEX_FILES):
       urlbase += "/fileReplicas?"
       urlargs = []

       if(not (blocks)): 
         msg = "Error building the PhEDEx xml url. A FileBlock must be specified"
         raise DlsArgumentError(msg)
       else:
         for block in blocks:
           urlargs.append(('block', block.replace('*','%')))
       
       if(ses):
          for se in ses:
            urlargs.append(('se', se))
   
       if not (kwd.has_key("incomplete") and kwd.get("incomplete")):
          urlargs.append(('dist_complete', 'y'))
   
       if(kwd.has_key("updated_since")):
          urlargs.append(('updated_since', kwd.get("updated_since")))
       
       if(kwd.has_key("created_since")):
          urlargs.append(('created_since'+kwd.get("created_since")))
 
       showProd = (kwd.has_key("showProd") and kwd.get("showProd"))
       showCAF = (kwd.has_key("showCAF") and kwd.get("showCAF"))
       if not (showProd and showCAF):
          urlargs.append(('op','node:and'))
       if not showProd:
          urlargs.append(('node','!T0*'))
          urlargs.append(('node','!T1*'))
       if not showCAF:
          urlargs.append(('node','!T2_CH_CAF'))

    # Query for all locations
    if(type == DLS_PHEDEX_ALL_LOCS):

       urlbase += "/nodes"
       urlargs = []

       if not (kwd.has_key("showEmpty") and kwd.get("showEmpty")):
          urlargs.append(('noempty','y'))
 
    return [urlbase, urlargs]
       
   
  def _mapException(self, inst, excp_msg, warn_msg, errorTolerant=False):
    """
    If errorTolerant==False, analyzes the passed exception and raises the 
    appropriate corresponding DlsApiError exception including the message
    excp_msg. If errorTolerant==True, just prints the passed warn_msg using
    the _warn function.

    @param inst: the Exception object to be analyzed
    @param excp_msg: the message passed to the exception in creation
    @param warn_msg: the message to print as warning
    @param errorTolerant: boolean to control operation 

    @exception: raises the appropriate DlsApiError, if errorTolerant==False
    """

    caught_msg = "%s: %s" % (str(inst.__class__), str(inst))

    if(not errorTolerant):
       
       # If DlsErrorWithServer, do not repeat the exception name
       if (isinstance(inst, DlsErrorWithServer)):
         excp_msg = excp_msg + '. %s' % (inst.msg)
         raise DlsErrorWithServer(excp_msg)
       
       # Otherwise, include all information
       excp_msg = excp_msg + '. Caught: %s' % (caught_msg)
      
       if (isinstance(inst, SAXException) or (isinstance(inst, SAXParseException))):
         excp_msg = "Error parsing server reply. " + excp_msg
         raise DlsErrorWithServer(excp_msg)
         
       if (isinstance(inst, HTTPError) or (isinstance(inst, URLError))):
         excp_msg = excp_msg + ' Server endpoint: ' + self.server
         raise DlsConnectionError(excp_msg)
         
       # If unexpected exception, raise the default exception
       raise DlsErrorWithServer(excp_msg)
       
    else:
       warn_msg = warn_msg + '. Caught: %s' % (caught_msg)
       self._warn(warn_msg)
