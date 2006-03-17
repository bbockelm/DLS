#!/usr/bin/env python
#
# 16-February-2006 Fabio Farina
# modif 17, 20
#

from dls_stress_test import *
import getopt
import os, sys

class NoLFCHOSTError:
    def __init__(self):
        print '\nERROR : LFC_HOST environment variable is not defined\n'
        pass

def usage():
  print " "
  print " DLS/LFC Test Suite "
  print " "
  print 'usage: ',sys.argv[0],' <options>'
  print "Options"
  print "-h,--help \t\t Show this usage"
  print " "
  print "-w,--whole \t\t Perform the whole test suite"
  print "-r,--repetition \t Perform the repetition test only"
  print "-c,--concurrency \t Perform the concurrency test only"
  print "-m,--magnitude \t\t Perform the magnitude test only"
  print "-a,--random \t\t Perform the random variation test only"
  print "-u,--user \t\t Perform the user simulation test only"
  print " "
  print "--distribution= \t Enable distribution of clients arrivals. The allowed values are"
  print "\t\t\t\t poisson (defaul)"
  print "\t\t\t\t burst"
  print "\t\t\t\t pareto"
  print ""
  

if __name__ == '__main__':
     long_options=["help","whole","repetition","concurrency","magnitude","random","user","distribution="]
     short_options="hwrcmaui:d="
     try:
         opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
     except getopt.GetoptError:
         usage()
         print "err"
         sys.exit(1)

     if len(opts)<1:
         usage()
         sys.exit(1)
     
     try:
         lfc = os.environ['LFC_HOST']
     except:
         raise NoLFCHOSTError
     
     # implicit -w management
     repetition = concurrency = magnitude = random = user = 1
     distro = "poisson"
     
     for o, a in opts:
            if o in ("-h", "--help"):
                usage()
                sys.exit(1)
            if o in ("-r", "--repetition"):
                 concurrency = magnitude = random = user = 0
            if o in ("-c", "--concurrency"):
                 repetition = magnitude = random = user = 0
            if o in ("-m", "--magnitude"):
                 repetition = concurrency = random = user = 0
            if o in ("-a", "--random"):
                 repetition = concurrency = magnitude = user = 0
            if o in ("-u", "--user"):
                 repetition = concurrency = magnitude = random = 0
            if o in ("-d=", "--distribution"):
                 distro = a
     
     if (distro != "poisson") & (distro != "burst") & (distro != "pareto"):
         print "WARNING: unrecognized thread arrival model. Poisson distribution assumed."
         distro = "poisson"
     

     if repetition == 1:
         repetitionTest(distro)
     
     if concurrency == 1:
         concurrencyTest(distro)
     
     if magnitude == 1:
         magnitudeTest(distro)
     
     if random == 1 :
         randomVariationTest(distro)
     
     if user == 1:
         userSimulationTest(distro)
     
     sys.exit(0)


     
