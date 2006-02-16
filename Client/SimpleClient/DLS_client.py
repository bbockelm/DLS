#!/usr/bin/python

# $Id: dls-get-se,v 1.3 2006/01/06 17:46:30 afanfani Exp $ 

import socket
import getopt
import sys
class DLS_client:
    def __init__(self):
        self.__client=socket.socket ( socket.AF_INET, socket.SOCK_STREAM )

    def getReplicas(self, fileblock, server, port=18080):
        try:
            self.__client.connect ( (server, int(port)) )

            msg='show_replica_by_db#%s'%(fileblock)
            self.dls_send(msg)
            msg=self.dls_receive()
            self.__client.close()
            return msg
        except:
            print "DLS Server doesn't respond"
            return 3

    def connect(self):
        long_options=["help","verbose","datablock=","host=","port="]
        short_options="hvd:o:p:"
        
        try:
            opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)

        if len(opts)<1:
            self.usage()
            sys.exit(2)
            
        host = None
        db = None
        port = None
        verbose = False
        
        for o, a in opts:
            if o in ("-v", "--verbose"):
                verbose = True
            if o in ("-h", "--help"):
                self.help()
                sys.exit(2)
            if o in ("-o", "--host"):
                host=a
            if o in ("-p", "--port"):
                port=a
            if o in ("-d", "--datablock"):
                db=a
        
        if host==None:
            self.usage()
            print "error: --host <Hostname> is required"
            sys.exit(2)
          
        if db==None:
            self.usage()
            print "error: --datablock <datablock> is required"
            sys.exit(2)
          
        if port==None:
            port=18080
            
        try:
            if verbose:
                print "Connectiong to %s:%d"%(host,int(port))

            self.__client.connect ( (host, int(port)) )

            msg='show_replica_by_db#%s'%(db)
            self.dls_send(msg)
            if verbose:
                print "send: %s"%(msg) 
            msg=self.dls_receive()
            if verbose:
                print "Received from server:"
            print msg
            self.__client.close()
            return 0
        except:
            print "DLS Server don't respond"
            return 3
    
    def dls_send(self,msg):
        totalsent=0
        #pdb.set_trace()
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

    def usage(self):
        
        print "dls-get-se version 0 2005 Giovanni Ciraolo"
        print "usage dls-get-se <options>"
        print "try --help for more options"
        

    def help(self):
        print "dls-get-se version 0 2005 Giovanni Ciraolo"
        print "usage dls-get-se <options>"
        print "Options"
        print "-h,--help \t\t\t Show this help"
        print "-v,--verbose \t\t\t Show output of procedures"
        print "-d,--datablock <datablock> \t Name of DataBlock"
        print "-o,--host <hostname> \t\t DLS server remote host name"
        print "-p,--port <port> \t\t DLS server remote host port\n"

    def dls_receive(self):

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
