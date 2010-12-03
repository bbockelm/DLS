#!/usr/bin/env python


from threading import Thread
import os, time

import cherrypy
from cherrypy import expose, HTTPError, HTTPRedirect
from cherrypy.lib.static import serve_file

PORT = 4444
BLOCK1 = "/Wmunu/Spring10-START3X_V26_S09-v1/GEN-SIM-RECO#d1fd2344-2590-42b0-a94c-ab837fd6b07b"
BLOCK2 = "/Mu/Run2010A-PromptReco-v4/RECO#d36c7ef5-46f2-422a-90b5-37941d6c1f03"
XML1 = os.getcwd() + "/blockReplicas_1.xml"
XML2 = os.getcwd() + "/blockReplicas_2.xml"
XML_empty = os.getcwd() + "/empty.xml"


class FakePhedexServer(Thread):

    def run(self):
        """
        Main thread method (called when 'thread.start') is invoked).
        """
        opts = {'server.socket_port': PORT,
                'server.socket_host': '0.0.0.0'
               }
        cherrypy.config.update(opts)
        cherrypy.tree.mount(FakePhedexServer())
        cherrypy.server.start()


    def stop(self):
        """
        Stop the cherrypy server.
        """
        cherrypy.server.stop()


    def log(self, msg):
        """
        Log msg to where it corresponds.
        """
        print "FakePhedexServer-LOG: %s" % msg


    @expose
    def index(self):
        """
        Default method (when no path is specified in the server).
        """
        return "Hello world!"


    @expose
    def normal(self, method, block, **kwargs):
        if method == 'blockReplicas':
            return self._blockReplicas(block, **kwargs)

    @expose
    def ssl(self, method, block, **kwargs):
        if method == 'blockReplicas':
            return self._blockReplicasSSL(block, **kwargs)

    @expose
    def useragent(self, method, block = 'NULL', **kwargs):
        if method == 'blockReplicas':
            useragent = cherrypy.request.headers
            text = """<xml>
                        <phedex>
                        <block name="%s" ></block>
                        </phedex>
                      </xml>
                   """ % useragent['User-Agent']
            return text

    @expose
    def change(self, method, block = None, **kwargs):
        if method == 'blockReplicas':
            return self._blockReplicasChangeA(block, **kwargs)
        if method == 'blockReplicasB':
            return self._blockReplicasChangeB(block, **kwargs)

    @expose
    def inf(self, method, block = None, **kwargs):
        if method == 'blockReplicas':
            return self._blockReplicasInf(block, **kwargs)

    def _blockReplicas(self, block, **kwargs):
        """
        Method to simulates the non-SSL blockReplicas method of the PhEDEx 
        data service. It just redirects to the SSL version: 'blockReplicasSSL'.
        """
        if cherrypy.request.method == 'GET':
            newpath = '/ssl/blockReplicas?block=%s&' % block
            for k, v in kwargs.items():
                if type(v) == list:
                    newpath += '&'.join(['%s=%s' % (k,x) for x in v])
                else:
                    newpath += '&%s=%s' % (k,v)
        else:
            newpath = '/ssl/blockReplicas'

        raise HTTPRedirect(newpath, 303)


    def _blockReplicasSSL(self, block, **kwargs):
        """
        Method to simulate the SSL blockReplicas method of the PhEDEx 
        data service. It just return some XML for a few predefined blocks.
        """
        myfile = None

        self.log('blockReplicas: %s, %s' % (block, kwargs))

        if block == BLOCK1:
            myfile = XML1
        if block == BLOCK2:
            myfile = XML2
        if block == '-':
            myfile = XML_empty

        if myfile:
            return serve_file(myfile, "text/xml")
        else:
            raise HTTPError(400, 'Non appropriate block!')


    def _blockReplicasChangeA(self, block, **kwargs):
        """
        Wrong method that will redirect to _blockReplicasBadC: used to test 
        API call change in redirection.
        """
        raise HTTPRedirect("/change/blockReplicasB", 303)

    def _blockReplicasChangeB(self, block, **kwargs):
        """
        Wrong method that will redirect to _blockReplicasBadA: used to test 
        API call change in redirection.
        """
        raise HTTPRedirect("/change/blockReplicas", 303)
    
    def _blockReplicasInf(self, block, **kwargs):
        """
        Wrong method that will redirect to himself: used to test 
        infinite loop redirections.
        """
        raise HTTPRedirect("/inf/blockReplicas", 303)


##
# Main, run the cherrypy server with the FakePhedexServer tree
##
if __name__ == "__main__":
    mythread = FakePhedexServer()
    mythread.start()
    try:
        while True: 
            time.sleep(3)
    except:
        mythread.stop()
        mythread.join()

