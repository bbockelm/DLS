#!/usr/bin/env python

"""
DLS-phedex redirection tests
"""

import unittest
import time, re
import cherrypy
from cherrypy import HTTPError

import FakePhedexServer, dlsPhedexApi
from dlsApiExceptions import DlsApiError

PORT = 4444
BASEURL = "http://localhost:%s" % PORT

class DlsPhedexAgainstServerTest(unittest.TestCase):

    """
    TestCase for DLS-phedex in redirection cases.
    """

    _started = False

    def setUp(self):
        """
        Code to execute to in preparation for the test
        """
        self.C = self.__class__
        if not self.C._started:
            self.C.server = FakePhedexServer.FakePhedexServer()
            self.C.server.start()
            time.sleep(1)
            self.C._started = True

    def tearDown(self):
        """
        code to execute to clean up after tests
        """
        pass

    def testA_NormalRedirection(self):
        print "\n\n########## Testing normal redirection works"
        url = '%s/normal' % BASEURL
        api = dlsPhedexApi.DlsPhedexApi(url)

        res = api.getLocations(FakePhedexServer.BLOCK1)
#        print 'RES:',res[0]
        self.assert_(len(res) == 1)
        self.assert_(len(res[0].locations) > 1)


    def testB_ChangingRedirection(self):
        print "\n\n########## Testing changing redirection does not work"
        url = '%s/change' % BASEURL
        api = dlsPhedexApi.DlsPhedexApi(url)

        try:
            res = api.getLocations(FakePhedexServer.BLOCK1)
#            print 'RES:',res
            self.assert_(False)
        except DlsApiError as ex:
            self.assert_('trying to change' in str(ex))


    def testC_InfiniteRedirection(self):
        print "\n\n########## Testing infinite redirection does not work"
        url = '%s/inf' % BASEURL
        api = dlsPhedexApi.DlsPhedexApi(url)

        try:
            res = api.getLocations(FakePhedexServer.BLOCK1)
#            print 'RES:',res
            self.assert_(False)
        except DlsApiError as ex:
            self.assert_('infinite loop' in str(ex))


    def testD_GetRequest(self):
        print "\n\n########## Testing GET request (API creation check) works"
        url = '%s/normal' % BASEURL

        api = dlsPhedexApi.DlsPhedexApi(url, checkEndpoint = True)
        self.assert_(True)


    def testE_NoRedirection(self):
        print "\n\n########## Testing normal redirection works"
        url = '%s/ssl' % BASEURL
        api = dlsPhedexApi.DlsPhedexApi(url)

        res = api.getLocations(FakePhedexServer.BLOCK2)
#        print 'RES:',res[0]
        self.assert_(len(res) == 1)
        self.assert_(len(res[0].locations) > 1)


    def testF_DefaultUserAgent(self):
        print "\n\n########## Testing default user agent works"
        url = '%s/useragent' % BASEURL
        api = dlsPhedexApi.DlsPhedexApi(url)

        res = (api.getLocations(FakePhedexServer.BLOCK2))[0].fileBlock.name
        print 'RES:',res

#       E.g.: 'dls-client/DLS_1_1_2 urllib2/2.6 Python/2.6.4 (CMS) Linux/2.6.18 (x86_64)
        self.C.basicPat = r'dls-client/[^ ]* urllib2/[^ ]* Python/[^ ]* '
        self.C.basicPat += '\(CMS\) Linux/[^ ]* \([^ ]*\)'
        pattern = re.compile(self.C.basicPat)
        self.assert_(pattern.match(res))


    def testG_ExtendedUserAgent(self):
        print "\n\n########## Testing extended user agent works"
        url = '%s/useragent' % BASEURL
        api = dlsPhedexApi.DlsPhedexApi(url, uaFlexString = 'This is a test', 
                    uaClientsList = [['Tester', '1.1'], ['Sth', '2_4']], )

        res = (api.getLocations(FakePhedexServer.BLOCK2))[0].fileBlock.name
        print 'RES:',res

        extPat = r'This is a test, Sth/2_4 Tester/1.1 ' + self.C.basicPat
        pattern = re.compile(extPat)
        self.assert_(pattern.match(res))

    def testZ_Shutdown(self):
        print "\n\n########## Testing shutdown"
        self.C.server.stop()
        self.C.server.join()
        self.assert_(True)

if __name__ == '__main__':
    unittest.main()
