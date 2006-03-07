#!/usr/bin/python

# 
# 11/2005. dlslfc v 0.7
# Antonio Delgado Peris. CERN, LCG. $Id
#

import lfc
import sys
import commands
import getopt
import os


######################## FUNCTIONS ########################

def usage():
   """
    Provides usage information
   """
   print "Usage: dlslfc-delete [-v] [-k]      [-x] <fileblock> [<SE_1> <SE_2> ..]"
   print "       dlslfc-delete [-v] [-k | -l] [-x] -a <fileblock>"
   print "       dlslfc-delete [-v] [-k | -l] [-x] -f <listing_file>"
   print "       dlslfc-delete -u"
   print "       dlslfc-delete -h"

def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -v, --verbose
   -a, --all
   -l, --remove-links
   -k, --keep-lfn
   -x, --force
   -f, --from-file
   """

def help():
   """
    Provides some help information
   """
   print """This script will remove the specified non-custodial replicas (located in
<SE_1> <SE_2> ..) for the given <fileblock> entry in the DLS server.

Custodial replicas (f_type = "P") are not deleted unless "--force" is specified.
If any (or all) of the SEs is  specified, only the replicas and not the fileblock
entry itself are removed.

If the "-a" option is specified (no SE must then be used), all the replicas are removed
and also the specified fileblock's LFN (or sym link) is deleted. If one of the replicas
was custodial, though, that one cannot be removed unless "--force" is used, and thus
the entry itself cannot be removed either.

If the "-l" option is used, also the rest of sym links (including the main LFN) are
removed. This option makes sense only if specified together with "-a" (or "-f" for
lines with no SE).

If the "-k" option is given, then only the SFNs are removed, but not the LFN or
symlinks, no matter if "-a" is used or not.

The "-k' and "-l" options cannot be specified together.

NOTE: If you plan to delete a whole DLS directory (including any custodial replicas),
rather use lfc-del-dir. If you want to delete a sym link but not any replica, rather
use lfc-rm.

The "-f" option can be used to specify fileblocks and SEs in a file rather than in
the arguments. The file must contain one fileblock per line, with the list of the SE
of the replicas to be removed in the same line and separated by whitespaces. In this
case, the "-k", "-l" and "--force" options have the same meaning as before and affects
all fileblocks in <listfile>.

ATTENTION: The "-a" option cannot be specified together with "-f", but if in a line
of <listfile> no replica is specified with the fileblock, then the functionality of
the"-a" option is asumed for that line and all replicas are removed. 

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
 

def deleteOneReplica(pSfn, pVerbose):
   """
    Tries to delete the replica with the SFN and for the fileid specified, 
   """

   sfn = pSfn
   verbose = pVerbose

   if(verbose):
      print "--lfc.lfc_delreplica(\"\", None, \""+sfn+"\")"   
   if(lfc.lfc_delreplica("", None, sfn) < 0):   
      sys.stderr.write("Error deleting replica: "+sfn+": "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return (-1)


def deleteOneLFN(pLfn, pVerbose):
   
   """
    Tries to delete the LFN (or sym link) specified
   """

   lfn=pLfn 
   verbose = pVerbose
   
   if(verbose):
      print "--lfc.lfc_unlink(\""+lfn+"\")"
   if(lfc.lfc_unlink(lfn)<0):
      sys.stderr.write("Error removing LFN:"+lfn+": "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return -1



def deleteOneEntry(pLfn, pSeList, pKeepLfn, pRemoveLinks, pForce, pVerbose):
   """
    Tries to delete the replicas of the specified LFN (or  symlink) that are
    kept in the specified SEs (or all if se=[]).

    A custodial replica (f_type = "P") is not deleted, unless pForce == true.
    
    If pSeList was empty, then all the existing replicas are removed. This will
    not be achieved if one of the replicas is custodial and pForce == false. 
    
    If this removal of all the replicas succeeds then the given LFN (or symlink)
    is also removed, unless pKeepLfn is true. Additionally, if pRemoveLinks == True,
    then all the other links (including the main LFN) are also removed.

    pKeepLfn and pRemoveLinks should not be true at the same time. In such a
    case, results are uncertain.

    The specified LFN is used as it is, without using the LFC_HOME env
    variable (that should be pre-pended by the caller, or the LFC directory
    changed to it beforehand in order to use the relative path).
   """

   lfn = pLfn
   seList = pSeList
   keepLfn = pKeepLfn
   removeLinks = pRemoveLinks
   force = pForce
   verbose = pVerbose
   
   rc = 0

#   lfn = checkHome(lfn)

   if(verbose):
      print "--Deleting LFN: "+lfn+" for SEs:",seList
  
   all = False
   if(not seList):
      all = True

   custodialLeft = False

#   # Look for specified SE within the list of replicas
#   listrep=lfc.lfc_list()
#   flagsrep=lfc.CNS_LIST_BEGIN 
   
#   # Call the retrieval in a loop
#   found=False
#   filerep=lfc.lfc_listreplica(lfn, "", flagsrep, listrep)
   
#   while(filerep):
#  
#      if(verbose):
#         print "--lfc.lfc_listreplica(\""+lfn+"\", \"\",",flagsrep,",listrep)"

   if(verbose):
      print "--lfc.lfc_getreplica(\""+lfn+"\", \"\",\"\")"
   err, list = lfc.lfc_getreplica(lfn, "", "")
   if(err):
      sys.stderr.write("Error retrieving replicas for: "+lfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
      return -1
      
   for filerep in list:              
      # Check if it is custodial before deleting (unless force was specified)
      if ((filerep.f_type == 'P') and (not force)):
         print "Warning: Not deleting custodial replica in",filerep.host,"of",lfn
         custodialLeft = True
         continue
      # If all are to be deleted, just do it
      if(all):
         err = deleteOneReplica(filerep.sfn, verbose)
         if(err): rc = err
      
      # Otherwise, check if this host was specified
      else:
         if(filerep.host in seList):
            # Don't look for this SE further
            seList.remove(filerep.host)

            # Delete
            err = deleteOneReplica(filerep.sfn, verbose)
            if(err): rc = err

            # And if no more SEs, exit
            if(not seList):
               break

#      flagsrep=lfc.CNS_LIST_CONTINUE
#      filerep=lfc.lfc_listreplica(lfn, "", flagsrep, listrep)

#   flagsrep=lfc.CNS_LIST_END
#   lfc.lfc_listreplica(lfn, "", flagsrep, listrep)

   # For the SEs specified, warn if they were not all removed
   if(seList and verbose):
      print "Warning: Not all SFNs (for the specified SEs) could be found and removed"


   # If all the replicas (even custodial) were deleted go on to LFNs (and links)
   if((all) and (not custodialLeft)):

      # If -l was specified, delete all links (even main LFN)
      if(removeLinks):
#         list=lfc.lfc_list()
#         flags=lfc.CNS_LIST_BEGIN 
#         link=lfc.lfc_listlinks(lfn, "", flags, list)

#         while(link):

#            if(verbose):
#               print "--lfc.lfc_listlinks(\""+lfn+"\", \"\",",flags,",list)"

         if(verbose):
            print "--lfc.lfc_getlinks(\""+lfn+"\", \"\",\"\")"
         err, list = lfc.lfc_getlinks(lfn, "", "")

         if(err):
            sys.stderr.write("Error retrieving links for: "+lfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
            return -1

         for link in list:
            err = deleteOneLFN(link.path, verbose)
            if(err): rc=err          
            
#            flags=lfc.CNS_LIST_CONTINUE
#            link=lfc.lfc_listlinks(lfn, "", flags, list)

#         flags=lfc.CNS_LIST_END
#         lfc.lfc_listlinks(lfn, "", flags, list)
         
      # If no "-l", but no "-k" either, remove the specified LFN (or sym link)
      else:
         if(not keepLfn):
            err = deleteOneLFN(lfn, verbose)
            if(err): rc = err

 # Return the error code
   return rc





def deleteEntries(pLineList, pKeepLfn, pRemoveLinks, pForce, pVerbose):
   """
    Tries to delete the replicas of the specified LFNs that are kept in the corresponding
    specified SEs (or all if for that LFN, se=[]).
    
    pLineList is a list of strings. In each string the first element is an LFN and the
    rest (separated by whitespaces) are the corresponding SEs for that LFN.

    For each LFN in pLineList, the contents of LFC_HOME env var are prepended
    for the relative paths (absoulte paths are used as they are), and
    deleteOneEntry is invoked.

    Please check deleteOneEntry for the meaning of the other arguments.
   """

   lineList = pLineList
   keepLfn = pKeepLfn
   removeLinks = pRemoveLinks
   force = pForce
   verbose = pVerbose

   rc = 0
   
   for line in lineList:
      tokens=(line.strip()).split()
      lfn = tokens[0]
      lfn = checkHome(lfn)
      seList = tokens[1:]
      err = deleteOneEntry(lfn, seList, keepLfn, removeLinks, force, verbose)

      if(err): rc = err
      
   # Return the error code
   return rc



###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Options and args... 
 
   longoptions=["help", "usage", "verbose", "all", "remove-links", "keep-lfn", "force", "from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'huvalkxf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)

   err=0
   all = False
   removeLinks = False
   keepLfn = False
   force = False
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
           
       elif opt in ("-a", "--all"):
           if(fromFile):
              sys.stderr.write("Bad usage: The -a and -f options are incompatible"+'\n')
              usage()
              return -1
           all = True
           
       elif opt in ("-l", "--remove-links"):       
           if(keepLfn):
              sys.stderr.write("Bad usage: The -k and -l options are incompatible"+'\n')
              usage()
              return -1
           removeLinks = True
           
       elif opt in ("-k", "--keep-lfn"):
           if(removeLinks):
              sys.stderr.write("Bad usage: The -k and -l options are incompatible"+'\n')
              usage()
              return -1
           keepLfn = True
           
       elif opt in ("-x", "--force"):
           force = True
           
       elif opt in ("-f","--from-file"):
           if(all):
              sys.stderr.write("Bad usage: The -a and -f options are incompatible"+'\n')
              usage()
              return -1
           fromFile = True
           fname = val


   if(removeLinks):
       if (not (all or fromFile)):
          sys.stderr.write("Bad usage: The -l option can only be specified together with -a or -f"+'\n')
          usage()
          return -1
         

 # Do the removal 
 
   # From file
   if(fromFile):
      try:
         file=open(fname, 'r')
      except IOError, inst:
         msg="The file "+fname+" could not be opened: "+str(inst)+"\n"
         sys.stderr.write(msg)
         return -1
      lineList=file.readlines()
      
      
      # Removal loops (under session)
      lfc.lfc_startsess("", "")
      err = deleteEntries(lineList, keepLfn, removeLinks, force, verbose)
      lfc.lfc_endsess()
      
      # Removal loops (under transaction)
      #lfc.lfc_starttrans("", "")
      #err = deleteEntries(lineList, keepLfn, removeLinks, force, verbose)
      #if(err):
      #   sys.stderr.write("Rolling back"+'\n')
      #   lfc.lfc_aborttrans()
      #else:
      #   lfc.lfc_endtrans()

    # From command line options
   else:
      if(len(args)<1):
         print "Bad usage: Not enough input arguments"
         usage()
         return -1
      if(len(args)<2):
         if(not all):
            print "Bad usage: Not enough input arguments (either -a is used, or a SE is specified)"
            usage()
            return -1
      else:
         if(all):
            sys.stderr.write("Bad usage: The -a option is incompatible with a SE argument"+'\n')
            usage()
            return -1
            
      lfn = args[0]
      lfn = checkHome(lfn)
      seList = args[1:]
     
      # Delete (under session/)
      lfc.lfc_startsess("", "")
      err = deleteOneEntry(lfn, seList, keepLfn, removeLinks, force, verbose)
      lfc.lfc_endsess()
      
      # Delete (under transaction)
      #lfc.lfc_starttrans("", "")
      #err = deleteOneEntry(lfn, seList, keepLfn, removeLinks, force, verbose)
      #if(err):
      #   sys.stderr.write("Rolling back"+'\n')
      #   lfc.lfc_aborttrans()
      #else:
      #   lfc.lfc_endtrans()
         
 # Finally, if no error exited before, exit succesfully
   return err


######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))

