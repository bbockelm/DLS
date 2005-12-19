#!/usr/bin/python

# 
# 11/2005.
# Antonio Delgado Peris. CERN, LCG.
#

import lfc
import sys
import commands
import getopt
import os
import time


######################## CONSTANTS ########################
THIS_YEAR = time.localtime()[0]


######################## FUNCTIONS ########################
def usage():
   """
    Provides usage information
   """

   print "Usage: dlslfc-get-se [-v] [-l] <fileblock>"
   print "       dlslfc-get-se [-v] [-l] -f <listing_file>"
   print "       dlslfc-get-se -u"
   print "       dlslfc-get-se -h"

def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -v, --verbose
   -l, --long
   -f, --from-file
   """

def help():
   """
    Provides some help information
   """
   print """This script will print the SEs for which there is a replica of the specified
fileblock registered. 

If "-l" is specified, some replica attributes are printed after the SE name.
Currently, printed attributes are: accees time, pin time, file type ('P' for permanent 
or 'V' for volatile) and SFN.

The "-f" option can be used to retrieve fileblock names from a file rather than from the
given arguments. The file must contain one line per fileblock to be listed.

If "-u" is specified, usage information is displayed.

If "-h" is specified, help information is displayed.
   """
   options()
   usage()


def checkHome(pLfn):
  """
   Checks if the specified LFN is relative (not starting by '/') and if so, it
   prepends the contents of the env var LFC_HOME to it.
  """

  lfn=pLfn

  if(not lfn.startswith('/')):
     lfc_home = os.environ.get("LFC_HOME")
     if(lfc_home):
        lfn = lfc_home + '/' + lfn

  return lfn



def showSEsForOneLFN(pLfn, pLonglist, pVerbose):
   """
    Prints the SEs which hold a replica of the specified fileblock.

    If pLonglist is set to true, some replica attributes are also printed before the SE.

    Currently printed attributes are: atime, ptime, f_type.
   """
   lfn = pLfn
   longlist = pLonglist
   verbose = pVerbose
   
   lfn=checkHome(lfn)

 # Retrieve list of replicas

#   list = lfc.lfc_list()
#   flags = lfc.CNS_LIST_BEGIN 
#
#   # First check we can access the entry (cause next method's error are difficult to get)
#   R_OK=4
#   if(lfc.lfc_access(lfn, R_OK)<0):
#      sys.stderr.write("Error when accessing provided lfn: "+lfn+
#                       ": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
#      return -1
#
#   # Call the retrieval in a loop
#   filerep=lfc.lfc_listreplica(lfn, "", flags, list)
#   while(filerep):
#      if(verbose):
#         print "--lfc.lfc_listreplica(\""+lfn+"\", \"\",",flags,",list)"   
#

   if(verbose):
         print "--lfc.lfc_getreplica(\""+lfn+"\", \"\", \"\")"   
         
   err, list = lfc.lfc_getreplica(lfn, "", "")
   
   if(err):
      sys.stderr.write("Error retrieving replicas for: "+lfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
      return -1
      
   for filerep in list:
      print filerep.host,
      if(longlist):
         time_tuple = time.localtime(filerep.atime)
         if(time_tuple[0] != THIS_YEAR):
            fmt = "%b %d %Y"
         else:
            fmt = "%b %d %H:%M"
         print '\t',time.strftime(fmt, time_tuple), "\t",
         print filerep.ptime, "\t",
         print filerep.f_type, "\t",
         print filerep.sfn,
      print

#      flags=lfc.CNS_LIST_CONTINUE
#      filerep=lfc.lfc_listreplica(lfn, "", flags, list)
#  
#   flags=lfc.CNS_LIST_END
#   lfc.lfc_listreplica(lfn, "", flags, list)

 # Finally, if no error exited before, exit succesfully
   return 0



def showSEsForLFNs(pLfnList, pLonglist, pVerbose):
   """
    Prints the SEs which hold a replica of the specified fileblock.
   """

   lfnList = pLfnList
   longlist = pLonglist
   verbose = pVerbose

   for lfn in lfnList:
      lfn = lfn.strip()
      print "  LFN: "+lfn
      showSEsForOneLFN(lfn, longlist, verbose)

 # Finally, if no error exited before, exit succesfully
   return 0



###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Options and args... 
 
   longoptions=["help", "usage", "verbose", "long", "from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'hutvlf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)

   err=0
   verbose = False
   longlist = False
   fromFile = False
   fname=""
   for opt, val in optlist:
       if opt in ("-h", "--help"):
           help()
           return -1

       elif opt in ("-u", "--usage"):
           usage()
           return -1
           
       elif opt in ("-v", "--verbose"):
           verbose = True

       elif opt in ("-l", "--long"):
           longlist = True

       elif opt in ("-f","--from-file"):
           fromFile = True
           fname = val
           
 # Perform the listing

   # From file
   if(fromFile):
      #Open File      
      try:
         file=open(fname, 'r')
      except IOError, inst:
         msg="The file "+fname+" could not be opened: "+str(inst)+"\n"
         sys.stderr.write(msg)
         return -1
         
      # Read lines
      lfnList = file.readlines()
      
      # Show loop (under session)
      lfc.lfc_startsess("", "")
      err = showSEsForLFNs(lfnList, longlist, verbose)
      lfc.lfc_endsess()

   # From command line options
   else:
      if(len(args)<1):
        print "Not enough input arguments"
        usage()
        return(-1)
      
      # Show (under session)
      else:
         lfn = args[0]
         if(verbose):
            print "--LFN: "+lfn
         lfc.lfc_startsess("", "")
         err = showSEsForOneLFN(lfn, longlist, verbose)
         lfc.lfc_endsess()


 # Finally, if no error exited before, exit succesfully
   return err

     

######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))

