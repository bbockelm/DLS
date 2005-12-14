#!/usr/bin/python

# 
# 11/2005. dlslfc v 0.7
# Antonio Delgado Peris. CERN, LCG. $Id
#

import lfc
import sys
import getopt


######################## FUNCTIONS ########################

def usage():
   """
    Provides usage information
   """
   print "Usage: dlslfc-get-datablock [-v] <SE_name>"
   print "       dlslfc-get-datablock [-v] -f <listing_file>"
   print "       dlslfc-get-datablock -u"
   print "       dlslfc-get-datablock -h"


def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -v, --verbose
   -f, --from-file
   """


def help():
   """
    Provides some help information
   """
   print """This script will print the fileblock entries that are associated with the
specified SE (i.e., there is a replica of that fileblock in that SE).

The "-f" option can be used to retrieve SE names from a file rather than from the
given arguments. The file must contain one line per SE to be listed.

If "-u" is specified, usage information is displayed.

If "-h" is specified, help information is displayed.
   """
   options()
   usage()


def showEntriesForOneSE(pSe, pVerbose):
   """
    Prints the fileblocks that are stored in the specified SE.
   """

   se = pSe
   verbose = pVerbose

 # Retrieve list of replicas
   list_rep=lfc.lfc_list()
   flags_rep=lfc.CNS_LIST_BEGIN 

   # Call the retrieval in a loop
   if(verbose):
      print "--lfc.lfc_listreplicax(\"\", \""+se+"\", \"\",",flags_rep,",list_rep)"   
   filerep=lfc.lfc_listreplicax("", se, "", flags_rep, list_rep)
   
   while(filerep):
      path = ' ' * (lfc.CA_MAXPATHLEN+1)
      if(verbose):
         print "--lfc.lfc_getpath(\"\",",filerep.fileid,", path)  (("+filerep.sfn+"))"   
      if(lfc.lfc_getpath("", filerep.fileid, path)<0):
         print "Error while retrieving the path for "+filerep.sfn+":",lfc.sstrerror(lfc.cvar.serrno)
      print path.split('\x00')[0]

     # Uncomment this to get the sfn printed
#      print filerep.sfn
#      print (filerep.sfn).split('/')[3]

      flags_rep=lfc.CNS_LIST_CONTINUE
      if(verbose):
         print "--lfc.lfc_listreplicax(\"\", \""+se+"\", \"\",",flags_rep,",list_rep)"   
      filerep=lfc.lfc_listreplicax("", se, "", flags_rep, list_rep)

   # Finish the replica retriving
   flags_rep=lfc.CNS_LIST_END
   filerep=lfc.lfc_listreplicax("", se, "", flags_rep, list_rep)


def showEntriesForSEs(pSeList, pVerbose):
   """
    Prints the fileblocks that are stored in the specified SE.
   """

   seList = pSeList
   verbose = pVerbose
   for se in seList:
      se = se.strip()
      print "  SE: "+se
      showEntriesForOneSE(se, verbose)
   

###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Options and args... 
 
   longoptions=["help", "usage", "verbose","from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'hutvf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)

   err=0
   verbose = False
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

       elif opt in ("-f","--from-file"):
           fromFile = True
           fname = val
          
          
 # Perform the listing

   # From file
   if(fromFile):
      try:
         file=open(fname, 'r')
      except IOError, inst:
         msg="The file "+fname+" could not be opened: "+str(inst)+"\n"
         sys.stderr.write(msg)
         return -1
      seList=file.readlines()
      
      # Show (under session)
      lfc.lfc_startsess("", "")
      lfc.lfc_chdir("/")
      err = showEntriesForSEs(seList, verbose)
      lfc.lfc_endsess()

   # From command line options
   else:
      if(len(args)<1):
        print "Not enough input arguments"
        usage()
        return(-1)
      
      # Show (under session)
      else:
         se = args[0]
         if(verbose):
            print "--SE: "+se
         lfc.lfc_startsess("", "")
         lfc.lfc_chdir("/")
         err = showEntriesForOneSE(se, verbose)
         lfc.lfc_endsess()


 # Finally, if no error exited before, exit succesfully
   return err


      

######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))

