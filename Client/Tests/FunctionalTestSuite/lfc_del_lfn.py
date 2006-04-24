#!/usr/bin/env python

# lfc_del_lfn.py
# 11/05
# Author: Antonio Delgado Peris. CERN, LCG.

"""
 This module provides functions and a command line tool to delete entries in an
 LFC catalog, including the associated replicas, if specified.
 
 These methods do not check if a physical file is associated with a replica name,
 and does not try to delete those physical files either. It just deletes the
 LFNs and SFNs in the catalog.
"""

import lfc
import sys
import commands
import os
import getopt


######################## FUNCTIONS ########################

def usage():
   """
    Provides usage information
   """
   print "Usage: lfc-del-lfn [-v] [-k | -l] <LFN>"
   print "       lfc-del-lfn [-v] [-k | -l] -f <listfile>"
   print "       lfc-del-lfn -u"
   print "       lfc-del-lfn -h"

def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -v, --verbose
   -k, --keep-lfn
   -l, --remove-links
   -f, --from-file
   """

def help():
   """
    Provides some help information
   """
   print """Removes all the SFNs for the specified LFN (or symlink) and
also the given LFN (or symlink) itself from the LFC name server.

If the "-l" option is specified, also the rest of sym links (including the main LFN)
are removed.

If the "-k" option is given, then only the SFNs are removed, but not the LFN or
symlinks.

The "-k' and "-l" options cannot be specified together.

ATTENTION: This command does not check if a physical file is associated with a replica
name, and does not try to delete those physical files either. It just deletes the
LFNs and SFNs in the catalog.

The "-f" option can be used to specify LFNs in a file rather than in the arguments. The
file must contain one LFN per line (and nothing else in each line). In this case, the 
"-k" and "-l" options have the same meaning as before and affect all LFNs in <listfile>.

If "-u" is specified, usage information is displayed.

If "-h" is specified, help information is displayed.
   """
   options()
   usage()


def checkHomeDir(pLfn):
  """
   Checks if the specified LFN is relative (not starting by '/') or absolute.
   If relative, the contents of the LFC_HOME env var are prepended to form
   an absolute path. The function returns the basename of the path, and the
   parent dir of this path.
  """

  lfn = pLfn

  if(lfn.startswith('/')):
     lfn = lfn.rstrip('/')
  else:
     lfc_home = os.environ.get("LFC_HOME")
     if(lfc_home):
        lfn = lfc_home + '/' + lfn
        
  dirlfn = lfn[0:lfn.rfind('/')+1]
  lfn = lfn[lfn.rfind('/')+1:]

  return lfn, dirlfn
  


def deleteOneReplica(pSfn, pVerbose):
   """
    Tries to delete the replica information with the SFN specified.
   """

   verbose = pVerbose

   if(verbose):
      print "--lfc.lfc_delreplica(\"\", None, \""+pSfn+"\")"   
   if(lfc.lfc_delreplica("", None, pSfn) < 0):   
      sys.stderr.write("Warning: Error deleting replica:"+pSfn+": "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
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
      sys.stderr.write("Warning: Error removing LFN:"+lfn+": "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return -1


def deleteOneEntry(pLfn, pDir, pKeepLfn, pRemoveLinks, pVerbose):
   """
    Tries to delete all the replicas associated with the specified LFN (or
    symlink). After all the replicas have been removed and if pKeepLfn is not
    true, then the LFN or symlink given is also removed. 

    If pRemoveLinks == True, then all the other links (including the main
    LFN) are also removed.

    pKeepLfn and pRemoveLinks should not be true at the same time. In such a
    case, results are uncertain.

    If the specified LFN is relative, then pDir should contain its parent
    directory, so that links can be checked to be in the same directory (in
    order to use relative names, what should give faster operation). If pDir
    is "", or if the lfn is absolute (for whatever value of pDir), absolute
    names are used for the LFN and sym links deletion.

    The specified LFN is used as it is, without using the LFC_HOME env
    variable (that should be pre-pended by the caller, or the LFC directory
    changed to it beforehand in order to use the relative path).
   """

   lfn = pLfn
   if(lfn.startswith('/')):
      dir = ""
   else:
      dir = pDir
   
   keepLfn = pKeepLfn
   removeLinks = pRemoveLinks
   verbose = pVerbose
   
   if(verbose):
      print "--Deleting entry: "+lfn

   rc = 0

   # Delete the replicas (unconditional)
   listrep=lfc.lfc_list()
   flagsrep=lfc.CNS_LIST_BEGIN 
   
   # Call the retrieval in a loop
   rc=0
   filerep=lfc.lfc_listreplica(lfn, "", flagsrep, listrep)
   
   while(filerep):
   
      if(verbose):
         print "--lfc.lfc_listreplica(\""+lfn+"\", \"\",",flagsrep,",listrep)"

      err = deleteOneReplica(filerep.sfn, verbose)
      if(err): rc=err
      
      flagsrep=lfc.CNS_LIST_CONTINUE
      filerep=lfc.lfc_listreplica(lfn, "", flagsrep, listrep)

   flagsrep=lfc.CNS_LIST_END
   lfc.lfc_listreplica(lfn, "", flagsrep, listrep)


   # Now go on with LFN and sym links: if -l was specified, delete them all
   if(removeLinks):
      list=lfc.lfc_list()
      flags=lfc.CNS_LIST_BEGIN 
      link=lfc.lfc_listlinks(lfn, "", flags, list)

      while(link):

         if(verbose):
            print "--lfc.lfc_listlinks(\""+lfn+"\", \"\",",flags,",list)"

         linkpath = link.path

         # For links in the same directory, we use the relative name
         if(dir):
             linkdir = linkpath[0:linkpath.rfind('/')]
             if(linkdir == dir):
                linkpath = linkpath[linkpath.rfind('/')+1:]

         err = deleteOneLFN(linkpath, verbose)
         if(err): rc=err          
         
         flags=lfc.CNS_LIST_CONTINUE
         link=lfc.lfc_listlinks(lfn, "", flags, list)

      flags=lfc.CNS_LIST_END
      lfc.lfc_listlinks(lfn, "", flags, list)
      
   # If not done yet, remove also the given LFN or link (unless -k was specified)
   if((not removeLinks) and (not keepLfn)):

      err = deleteOneLFN(lfn, verbose)
      if(err): rc = err


 # Return the error code
   return rc


def deleteEntries(pLfnList, pKeepLfn, pRemoveLinks, pVerbose):
   """
    For each LFN in pLfnList, the contents of LFC_HOME env var are prepended
    for the relative paths (absoulte paths are used as they are), and
    deleteOneEntry is invoked.

    Please check deleteOneEntry for the meaning of the other arguments.
   """

   lfnList = pLfnList
   keepLfn = pKeepLfn
   removeLinks = pRemoveLinks
   verbose = pVerbose

   rc = 0

   for lfn in lfnList:
     lfn = lfn.strip()
     [lfn, dir] = checkHomeDir(lfn)
     err = lfc.lfc_chdir(dir)
     if(err < 0):
        sys.stderr.write("Error changing dir to: "+dir+" : "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
        return err
     err = deleteOneEntry(lfn, dir, keepLfn, removeLinks, verbose)

     if(err): rc=err
     
 # Return the error code
   return rc




###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Analyze options and args... 
 
   longoptions=["help", "usage", "verbose", "keep-lfns", "remove-links", "from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'huvklf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)
   
   removeLinks = False
   keepLfn = False
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
           
       elif opt in ("-k", "--keep-lfns"):
           if(removeLinks):
              sys.stderr.write("Bad usage: The -k and -l options are incompatible"+'\n')
              usage()
              return -1
           keepLfn = True
           
       elif opt in ("-l", "--remove-links"):
           if(keepLfn):
              sys.stderr.write("Bad usage: The -k and -l options are incompatible"+'\n')
              usage()
              return -1
           removeLinks = True
           
       elif opt in ("-f","--from-file"):
           fromFile = True
           fname = val

 # Create the list of lfns to remove
   # From file
   if(fromFile):
      try:
         file=open(fname, 'r')
      except IOError, inst:
         msg="The file "+fname+" could not be opened: "+str(inst)+"\n"
         sys.stderr.write(msg)
         return -1
      lfnList=file.readlines()
      
   # From command line options
   else:
      if(len(args)<1):
         print "Not enough input arguments"
         usage()
         return(-1)
      lfnList=[args[0]]



 # Do the removal (under session)
   lfc.lfc_startsess("", "")
   err = deleteEntries(lfnList, keepLfn, removeLinks, verbose)
   lfc.lfc_endsess()

      
 # Return the error code
   return err



######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))

