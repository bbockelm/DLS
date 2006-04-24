#!/usr/bin/env python

# lfc_del_dir.py
# 11/05
# Author: Antonio Delgado Peris. CERN, LCG.

"""
 This module provides functions and a command line tool to delete directories
 in an LFC catalog, including the contained LFNs and associated replicas, if
 specified.
 
 These methods do not check if a physical file is associated with a replica name,
 and does not try to delete those physical files either. It just deletes the
 directories, LFNs and SFNs in the catalog.

 This module invokes the lfc_del_lfn for the LFNs and SFNs deletions.
"""

import lfc
import lfc_del_lfn
import sys
import commands
import getopt

import time


######################## FUNCTIONS ########################

def usage():
   """
    Provides usage information
   """
   print "Usage: lfc-del-dir [-v] [-r] [-l] [-x] <LFN>"
   print "       lfc-del-dir [-v] [-r] [-l] [-x] -f <listfile>"
   print "       lfc-del-dir -u"
   print "       lfc-del-dir -h"

def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -v, --verbose
   -r, --recursive
   -l, --remove-ext-links
   -x, --remove-ext-lfns
   -f, --from-file
   """

def help():
   """
    Provides some help information
   """
   print """Removes the specified directory from the LFC name server.
   
If the "-r" option is specified, then the directory will be removed even if it
is not empty and all the entries in the directory (including subdirectories) will
be removed as well, in a recursive way. Otherwise, the deletion will be performed
only if the directory is empty.

ATTENTION: This script does not check if a physical file is associated with a replica
name, and does not try to delete those physical files either. It just deletes the
directory, the LFNs and the SFNs in the catalog.

The default action is to remove only the specified directory contents and not to
touch external sym links pointing to LFNs in the directory, nor entries pointed
to by links located in the directory.

If "-l" is specified, then links located in other directories that point to LFNs in 
the specified directory are also removed.

If "-x" is specified, then LFNs (and their replicas and all their sym links)
located in other directories and pointed to by links in the specified directory are
also removed.

The "-f" option can be used to specify directory names in a file rather than in the
arguments. The file must contain one dir per line (and nothing else in each line).
In this case, the "-r", "-l" and "-x" option have the same meaning as before and
affect all directories included in <listfile>.

If "-u" is specified, usage information is displayed.

If "-h" is specified, help information is displayed.
   """
   options()
   usage()



def deleteOneDir(pDir, pForce, pExtLinks, pExtLfns, pVerbose):
   """
    Tries to delete the specified directory. If "pForce" is true, then the
    directory will be removed even if it is not empty and all the entries in
    the directory (including subdirectories) will be removed as well, in a
    recursive way. Otherwise, the deletion will be performed only if the
    directory is empty.

    If pExtLinks == pExtLfns == False, the function will remove only the
    specified directory contents and will not to  touch external sym links
    pointing to LFNs in the directory, nor entries pointed to by links located
    in the directory.

    If pExtLinks == True, then links located in other directories that point
    to LFNs in the specified directory are also removed.

    If pExtLfns == True, then LFNs (and their replicas and all their sym links)
    located in other directories and pointed to by links in the specified
    directory are also removed.
   """

   dir = pDir
   force = pForce
   extLinks = pExtLinks 
   extLfns = pExtLfns
   verbose = pVerbose

   rc = 0
   err = 0

   subdirlist = [] # Subdirectories to remove when we are done with current one

   [dir, parentdir] = lfc_del_lfn.checkHomeDir(dir)
   fulldir = parentdir+dir

   if(verbose):
      print "--Deleting Dir: "+dir
      
   err = lfc.lfc_chdir(fulldir)
   if(err < 0):
      sys.stderr.write("Error changing dir to: "+fulldir+" : "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return err

   dir_p=lfc.lfc_DIR()
   dir_entry=lfc.lfc_direnstatg()
   dir_p=lfc.lfc_opendirg(".", "")
   if(dir_p < 0):
      sys.stderr.write("Error opening specified dir: "+dir+": "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return -1

   # Read first entry
   lfc.lfc_rewinddir(dir_p)
   dir_entry=lfc.lfc_readdirg(dir_p)

   if(force):
      # Remove all LFNs in a loop
      S_IFDIR = 0x4000   
      S_IFLNK = 0xA000  

      while(dir_entry):
         lfn = dir_entry.d_name
         if(dir_entry.filemode & S_IFDIR):
         # This entry is a directory
            subdirlist.append(lfn)
         else:
           if((dir_entry.filemode & S_IFLNK) == S_IFLNK):
           # This entry is a sym link
              if(extLfns):
                 # Remove the replicas and all the links (including main LFN)
                 err = lfc_del_lfn.deleteOneEntry(lfn, fulldir, False, True, verbose)
              else:
                 # Remove only the sym link (no replicas)
                 err = lfc_del_lfn.deleteOneLFN(lfn, verbose)
           else:
           # This entry is a main LFN
#              # First check if the file has been alredy deleted (links, etc...)
#              fstat = lfc.lfc_filestatg()
#              if(lfc.lfc_statg(lfn, "", fstat)<0):
#                if(verbose):
#                   print "--Warning. Skipping deletion of non-accessible file:",lfn,":",\
#                          lfc.sstrerror(lfc.cvar.serrno)
#              else:
                 if(extLinks):
                    # Remove the replicas and all the links that point to this LFN
                    err = lfc_del_lfn.deleteOneEntry(lfn, fulldir, False, True, verbose)
                 else:
                    # Remove only this LFN and replicas (but no external sym links)
                    err = lfc_del_lfn.deleteOneEntry(lfn, fulldir, False, False, verbose)
            
         if(err): rc = err
         dir_entry=lfc.lfc_readdirg(dir_p)
      
   else:
      if(dir_entry):
         sys.stderr.write("Error: Directory "+dir+" not empty! Consider using -r.\n")
         return -1

   # Close the directory
   if(lfc.lfc_closedir(dir_p) < 0):
      sys.stderr.write("Error closing dir: "+dir+" : "+lfc.sstrerror(lfc.cvar.serrno)+"\n")

   # Remove all subdirectories in the list 
   for subdir in subdirlist:
      err = deleteOneDir(fulldir+'/'+subdir, force, extLinks, extLfns, verbose)
      if(err): rc=err
 
   # Finally, remove also the top directory itself 
   err = lfc.lfc_chdir("..")
   if(err < 0):
      sys.stderr.write("Error changing dir to \"..\" : "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return err

   if(verbose):
      print "--lfc.lfc_unlink(\""+dir+"\")"
   err = lfc.lfc_rmdir(dir)
   if(err<0):
      sys.stderr.write("Error removing dir: "+dir+": "+lfc.sstrerror(lfc.cvar.serrno)+"\n")
      return err

 # Return the error code
#   return rc
   return err



def deleteDirs(pDirList, pForce, pExtLinks, pExtLfns, pVerbose):
   """
    Tries to delete all the directories specified in the list by calling
    deleteOneDir. Please check the help information of that function.
   """

   dirList = pDirList
   force = pForce
   extLinks = pExtLinks 
   extLfns = pExtLfns
   verbose = pVerbose

   rc = 0

   for dir in dirList:
      dir = dir.strip()
      err = deleteOneDir(dir, force, extLinks, extLfns, verbose)
      if(err): rc=err


 # Return the error code
   return rc



###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Options and args... 
 
   longoptions=["help", "usage", "verbose", "--recursive",\
                "--remove-ext-links", "--remove-ext-links", "from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'huvrlxf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)

   force = False
   verbose = False
   extLinks = False 
   extLfns = False
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
           
       elif opt in ("-r", "--recursive"):
           force = True
           
       elif opt in ("-l", "--remove-ext-links"):
           extLinks = True
           
       elif opt in ("-x", "--remove-ext-lfns"):
           extLfns = True
           
       elif opt in ("-f","--from-file"):
           fromFile = True
           fname = val


 # Build the list of directories to remove
   # From file
   if(fromFile):
      try:
         file=open(fname, 'r')
      except IOError, inst:
         msg="The file "+fname+" could not be opened: "+str(inst)+"\n"
         sys.stderr.write(msg)
         return -1
      dirList=file.readlines()
      
   # From command line options
   else:
      if(len(args)<1):
         print "Not enough input arguments"
         usage()
         return(-1)
      dirList=[args[0]]

      
 # Do the removal (under session)
   lfc.lfc_startsess("", "")
   err = deleteDirs(dirList, force, extLinks, extLfns, verbose)
   lfc.lfc_endsess()

         
 # Finally, if no error exited before, exit succesfully
   return err


######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
