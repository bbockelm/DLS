#
# $Id: dlsMySQLApi.py,v 1.4 2006/04/07 09:02:10 delgadop Exp $
#
# Dls Client. $Name$. 
# Antonio Delgado Peris. CIEMAT. CMS.
# client for MySQL prototype : A. Fanfani  

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server using a LCG File Catalog (LFC) as back-end.

 The module contains the DlsMySQLApi class that implements all the methods
 defined in dlsApi.DlsApi class and a couple of extra convenient
 (implementation specific) methods. Python applications interacting with
 a LFC-based DLS will instantiate a DlsLFCApi object and use its methods.

 It also contains some exception classes to propagate error conditions
 when interacting with the DLS catalog.
"""

#########################################
# Imports 
#########################################
import dlsApi
#AF adding:
import dlsDataObjects
import socket
#########################################
# Module globals
#########################################
#DLS_VERB_NONE = 0    # print nothing
#DLS_VERB_INFO = 5    # print info
#DLS_VERB_WARN = 10   # print only warnings (to stdout)
#DLS_VERB_HIGH = 20   # print warnings (stdout) and error messages (stderr)

#########################################
# DlsMySQLApiError class
#########################################

class DlsMySQLApiError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the DlsMySQLApi
  class. It normally contains a string message (empty by default), and optionally
  an  error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.
  """

class NotImplementedError(DlsMySQLApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsMySQLApiError):
  """
  Exception class for invocations of DlsApi methods with an incorrect value
  as argument.
  """
  
class SetupError(DlsMySQLApiError):
  """
  Exception class for errors when setting up the system (configuration,
  communication errors...)
  """



#########################################
# DlsApi class
#########################################

class DlsMySQLApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on a Lcg File Catalog
  (LFC) as DLS back-end.

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsMySQLApiError.
  """

  def __init__(self, dls_endpoint= None, verbosity = dlsApi.DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLS (MySQL proto) server to communicate with
    and the verbosity level.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.
      
    @exception SetupError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string of form "hostname[:port]"
    @param verbosity: value for the verbosity level
    """

    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)    
    
    if(self.server):
      # Do whatever... 
      print " Using server %s"%self.server

    if(not self.server):
       raise SetupError("Could not set the DLS server to use")

  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.add method.
    Refer to that method's documentation.

    Implementation specific remarks:

    No attribute are supported in the MySQL prototype
    """
    for entry in dlsEntryList:
      fb=entry.fileBlock.name
      for location in entry.locations:
            se=location.host
            #print "fb %s"%fb
            #print "ses %s"%se
            self.dls_connect()
            msg='add_replica#%s#%s'%(fb,se)
            if ( self.verb >= 5 ) :
                print "Send:%s"%(msg)
            self.dls_send(msg)           
            msg=self.dls_receive()
            
            if ( self.verb >= 5 ):
                if msg=="0":
                    print "Replica Registered"
                elif msg=="1":
                    print "Replica already stored"
                else:
                    msg="2"
                    print "Replica not registered"
            self.__client.close()
            #return int(msg)
#TODO : error code
    return

#not in DLS proto    
#  def delete(self, dlsFileBlockList, **kwd):
#

  def delete(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.delete method.
    Refer to that method's documentation.

    Implementation specific remarks:

    """
    for entry in dlsEntryList:
      fb=entry.fileBlock.name
      for location in entry.locations:
            se=location.host
            #print "fb %s"%fb
            #print "ses %s"%se
            self.dls_connect()
            msg='remove_replica#%s#%s'%(fb,se)
            if ( self.verb >= 5 ):
                print "Send:%s"%(msg)
            self.dls_send(msg)
            msg=self.dls_receive()
            
            if ( self.verb >= 5 ):
                if msg=="0":
                    print "Replica Deleted"
                elif msg=="1":
                    print "Replica Not present"
                else:
                    print "error: %s not Stored"%(msg) 
                    msg="2"
            
            self.__client.close()
            #return int(msg)
    return

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:
    
    No attribute are supported in the MySQL prototype
    """

    for fb in fileBlockList:
            self.dls_connect()
            msg='show_replica_by_db#%s'%(fb)
            self.dls_send(msg)
            if ( self.verb >= 5 ):
                print "Send: %s"%(msg)
            msg=self.dls_receive()
            if ( self.verb >= 5 ):
                print "Received from server:"
            print msg
            self.__client.close()
            #return 0
    return
    

  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:
    
    No attribute are supported in the MySQL prototype
    """ 
    for se in locationList: 
            self.dls_connect()
            msg='show_replica_by_se#%s'%(se)
            self.dls_send(msg)
            if ( self.verb > 5 ):
                print "send: %s"%(msg) 
            msg=self.dls_receive()
            if ( self.verb > 5 ):
                print "Received from server:"
            print msg
            self.__client.close()
            #return 0
    return
 
  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
    Refer to that method's documentation.
    """

    # Implement here...
    pass

        
## not in DLS proto
#  def checkDlsHome(self, fileBlock, working_dir=None):
#    """
#    Implementation of the dlsApi.DlsApi.checkDlsHome method.
#    Refer to that method's documentation.
#
#    Implementation specific remarks:
#
#    The DLS client working directory should be read from (in this order):
#       - specified working_dir
#       - DLS_HOME environmental variable
#       - LFC_HOME environmental variable
#    """
#
#    # Implement here...
#    pass


  #########################
  # Internal methods
  #########################

  def clientsocket(self):
        """
        """
        self.__client=socket.socket ( socket.AF_INET, socket.SOCK_STREAM )

  def dls_connect(self):
        """
        """       
        host=self.server.split(':')[0]
        port=self.server.split(':')[1]
        if port==None:
           port=18080

        if ( self.verb >= 5 ):
            print "Connecting to host: %s port: %d"%(host,int(port))

        self.clientsocket()
        
        try:
            self.__client.connect ( (host, int(port)) )
        except:
            print "DLS Server don't respond"
            return 3

  def dls_send(self,msg):
        """
        """
        totalsent=0
        MSGLEN=len(msg)
        sent=  sent=self.__client.send(str(MSGLEN).zfill(16))
        #print "Sent %s"%(str(MSGLEN).zfill(16))
        if sent == 0:
            raise RuntimeError,"Socket connection broken"
        else:
            while totalsent < MSGLEN:
                sent = self.__client.send(msg[totalsent:])
                #print "Sent %s"%(msg[totalsent:])
                if sent == 0:
                    raise RuntimeError,"Socket connection broken"
                totalsent = totalsent + sent


  def dls_receive(self):
        """
        """
        chunk =  self.__client.recv(16)
        #print "Received %s"%(chunk)
        if chunk == '':
            raise RuntimeError,"Socket connection broken"
        MSGLEN= int(chunk)
        msg = ''
        while len(msg) < MSGLEN:
            chunk =  self.__client.recv(MSGLEN-len(msg))
            #print "Received %s"%(chunk)
            if chunk == '':
                raise RuntimeError,"Socket connection broken"
            msg = msg + chunk
        return msg


##################################################333
# Unit testing

if __name__ == "__main__":

   import dlsClient
   from dlsDataObjects import *

## use DLS server
   type="DLS_TYPE_MYSQL"
   server ="lxgate10.cern.ch:18081"
   api = dlsClient.getDlsApi(dls_type=type,dls_host=server, verbosity=5)
                                                                                                                 
## get FileBlocks given a location
   se="cmsboce.bo.infn.it"
   api.getFileBlocks([se])
                                                                                                                 
## get Locations given a fileblock
   fb="bt_DST871_2x1033PU_g133_CMS/bt03_tt_2tauj"
   #fb="testblock"
   api.getLocations([fb])
                                                                                                                 
## add a DLS entry
   fileblock=DlsFileBlock("testblock")
   location=DlsLocation("testSE")
   entry=DlsEntry(fileblock,[location])
   api.add([entry])
                                                                                                                 
## check the inserted entry
   api.getLocations([fb])
                                                                                                                 
## delete a DLS entry
   loc=DlsLocation("testSE")
   entry=DlsEntry(fileblock,[loc])
   api.delete([entry])


