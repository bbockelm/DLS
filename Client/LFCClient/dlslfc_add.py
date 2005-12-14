#!/usr/bin/python

# 
# 11/2005. dlslfc v 0.7
# Antonio Delgado Peris. CERN, LCG. $Id
#

import lfc
import sys
import commands
import time
import getopt
import os

######################## FUNCTIONS ########################

def usage():
   """
    Provides usage information
   """
   print "Usage: dlslfc-add [-v] [-t] [-p] <fileblock> [attr=val ..] [<SE> [attr=val ..] ..]"
   print "       dlslfc-add [-v] [-t] [-p] -f <listing_file>"
   print "       dlslfc-add -u"
   print "       dlslfc-add -h"

def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -p, --create-parent
   -t, --transaction
   -v, --verbose
   -f, --from-file
   """
def example():
  """
   Provides with an example of correct use of the script
  """
  print """
  """

def help():
   """
    Provides some help information
   """
   print """This script will create an entry in the DLS server and associate the specified
SEs with it (via replica creation). If the entry already exists, then it will just add the
specified replicas.

Fileblock attributes can be specified after the fileblock name (and before any SE), and 
are identified by the '=' sign joining key and value for each attribute (there must be
no spaces between them). Likewise, replica attributes can be specified after each
given SE. Unknown attribute names (keys) will be ignored.

Currently understood fileblock attributes are:    
      guid, mode, filesize, csumtype, csumvalue

Currently understood replica attributes are:    
      sfn, f_type, ptime

If "-p" is specified, then the parent directories of the specified LFN are checked for
existence, and created if non existing.

The "-f" option can be used to retrieve fileblock names from a file rather than from the
given arguments. The file must contain one line per fileblock to be added, optionally with
the entry attributes, then the SEs to associate as replicas and perhaps the attributes for
the replicas; all in the same line and separated by whitespaces. In this case, the
"-p" option has the same meaning as before and affects all fileblocks in <listfile>.

The "-t" option indicates if transactions should be used. In that case, any error in an
operation provokes a stop (no more removals) and a rollback of the previous operations.
Otherwise (no transactions), after a failure in one replica, the tool tries to go on with
next replicas (and fileblocks).

NOTE: It is not recommended to maintain a transaction for too long (~10 seconds).
Thus, it is suggested not to include too many fileblock in a <listfile> if transactions
are to be used. For a larger number of file blocks, please split them into several
invocations of the command.

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


def checkAndCreateDir(pDir, pMode, pVerbose):
  """
   Checks if the specified directory and all its parents exist in the LFC nameserver.
   Otherwise, it creates the whole tree (with the specified mode).

   If the specified directory exists, or if it can be created, the method returns 0.
   Otherwise (there is an error creating the specified dir or one of its parents), 
   the method returns -1.
  """

  dir = pDir
  mode = pMode
  verbose = pVerbose

  if(dir == "/"):
     return -1
  dir = dir.rstrip('/')
  
  fstat = lfc.lfc_filestatg()
  parentdir = dir[0:dir.rfind('/')+1]  
  fstat = lfc.lfc_filestatg()
  if(lfc.lfc_statg(dir, "", fstat)<0):
     if(checkAndCreateDir(parentdir, mode, verbose) < 0):
        return -1
     guid=commands.getoutput('uuidgen')         
     if(verbose):
        print "--lfc.lfc_mkdirg(",dir,",",guid,",",mode,")"
     if(lfc.lfc_mkdirg(dir, guid, mode) < -1):
        sys.stderr.write("Error creating directory: "+dir+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
        return -1
        
  return 0
         


def createOneEntry(pLfn, pAttrList, pCreateParent, pTrans, pVerbose):
   """
    Creates an entry with the specified fileblock (pLfn) and for the attributes in
    pAttrList that are understood, sets them to the specified value.
    pAttrList should be a list of key-value pairs: [[key,val], ..]
    Understood fileblock attributes are:    
      guid, mode, filesize, csumtype, csumvalue
      
    If pCreateParent == true, then the parent directory is checked for existence,
        and created if non existing.
      
    The specified LFN is used as it is, without using the LFC_HOME env
    variable (that should be pre-pended by the caller, or the LFC directory
    changed to it beforehand in order to use the relative path).

    The method returns the created GUID of the entry (even if this existed already),
    or -1 if an error occurred.
   """
   lfn = pLfn
   attrList = pAttrList
   createParent = pCreateParent
   trans = pTrans
   verbose = pVerbose

   if(verbose):
      print "--createOneEntry(\""+lfn+"\",",pAttrList,")"


 # Analyze attribute list
 
   # Defaults
   [guid, mode, filesize, csumtype, csumvalue] = ["", 0775, long(1000), '', '']

   # Get what was passed
   for attr in attrList:
      if(attr[0] == "guid"):
         guid=attr[1]
         continue
      if(attr[0] == "mode"): 
         if((attr[1])[0] == '0'):
            mode = int(attr[1], 8)
         else:
            mode = int(attr[1])
         continue
      if(attr[0] == "filesize"):     
         filesize=long(attr[1])
         continue
      if(attr[0] == "csumtype"):  
         csumtype=attr[1]
         continue
      if(attr[0] == "csumvalue"): 
         csumvalue=attr[1]
         continue
      else:
         print "Warning: Fileblock attribute \""+attr[0]+"\" unknown."

 # Check if entry exists
   fstat=lfc.lfc_filestatg()
   if(lfc.lfc_statg(lfn, "", fstat) <0):
   
      # If it does not exist...

      # First, check parents (only for absolute paths!, and if asked for)
      if(createParent and lfn.startswith('/')):
         if(verbose):
            print "--Checking parents of requested file: "+lfn
         parentdir = lfn[0:lfn.rfind('/')+1]
         if(checkAndCreateDir(parentdir, mode, verbose) < 0):
            sys.stderr.write("Error creating the parent directories for LFN: "+lfn+'\n')
            return -1

      # Now, create it
      if(not guid):
         guid=commands.getoutput('uuidgen')         
      if(verbose):
         print "--lfc.lfc_creatg(\""+lfn+"\", \""+guid+"\",",mode,")"   
      if(lfc.lfc_creatg(lfn, guid, mode) < 0):
         sys.stderr.write("Error creating the LFN: "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return -1
         
      # And set the size and cksum
      if(verbose):
         print "--lfc.lfc_setfsizeg(\""+guid+"\",",filesize,",",csumtype,",",csumvalue,")"
      if (lfc.lfc_setfsizeg(guid, filesize, csumtype, csumvalue)):
         sys.stderr.write("Error setting the filesize/cksum for the LFN: "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return (-1)

   else:
      # If it exists, get the real GUID
      guid=fstat.guid


 # If everything ewnt well, return the GUID
   return guid



#TODO: If this method is not used, we could as well remove it, since it is very easy to 
#      implement it in user code (with the exact logic that the user requires).
#def createEntries(pLfnAttrList, pTrans, pVerbose):
#   """
#    Creates entries with the specified fileblocks and setting the specified attributes
#    (see the method createOneEntry).
#    pLfnAttrList should be a list of one LFN and a attribute list, where each attribute
#    is a key-value pair:
#    [[lfn, attrList], ..]   I.e.:   [[lfn, [[key,val] ..]], ..]
#
#    The method returns 0 if all the entries were created correctly, or -1 if an error
#    occurred.
#   """
#
#   lfnAttrList = pLfnAttrList
#   trans = pTrans
#   verbose = pVerbose
#
#   rc = 0
#   
#   for lfnAttrPair in lfnAttrList:
#      guid = createOneEntry(lfnAttrPair[0], lfnAttrPair[1], trans, verbose)
#      if(guid == -1):
#         rc = -1
#         if(trans):  return -1
#   
#   return rc


def addOneReplica(pGuid, pSe, pAttrList, pTrans, pVerbose):
   """
    Adds a replica with the specified SE as host to the fileblock with the specified GUID
    and for the attributes in pAttrList that are understood, sets them to the specified
    value.
    pAttrList should be a list of key-value pairs: [[key,val], ..]
    Understood replica attributes are:    
      sfn, f_type, ptime
   """
   guid = pGuid
   se = pSe
   attrList = pAttrList
   trans = pTrans
   verbose = pVerbose

 # Analyze attribute list
 
   # Defaults
#      sfn = 'sfn://'+se+'/'+guid+'/'+str(time.time())  # Includes timestamp
   [sfn, f_type, ptime] = ["sfn://"+str(se)+'/'+str(guid), 'V', 0]

   # Get what was passed
   for attr in attrList:
      if(attr[0] == "sfn"):      
         sfn=attr[1]
         continue
      if(attr[0] == "f_type"):      
         f_type=attr[1]
         continue
      if(attr[0] == "ptime"):      
         ptime=int(attr[1])
         continue
      else:
         print "Warning: Replica attribute \""+attr[0]+"\" unknown."


 # Register replica
   if(verbose):
      print "--lfc.lfc_addreplica(\""+guid+"\", None, \""+se+"\", \""+sfn+"\",'-',\""+f_type+"\", \"\", \"\")"
   if(lfc.lfc_addreplica(guid, None, se, sfn, '-', f_type, "", "") < 0):
   
      if(trans):
         sys.stderr.write("Error adding SE for LFN: "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return -1
   
      if (lfc.cvar.serrno != 17):  
         sys.stderr.write("Error adding SE for LFN: "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return -1
      else:
         # This error is due to existence of file ==> Just warn (cause no trans)
         print "Warning: Replica", sfn,"exists. Skipping."

 # Set pin time
   if(ptime):
      if(lfc.lfc_setptime(sfn, ptime)<0):
         sys.stderr.write("Error setting pin time for SFN: "+sfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return -1

   return(0)
 


def addReplicas(pGuid, pSeListAttrList, pTrans, pVerbose):
   """
    Adds replicas with the specified SEs are hosts, to the specified fileblock, and 
    setting the specified attributes (see the method addOneReplica).
    pSeAttrList should be a list of one SE and an attribute list, where each attribute
    is a key-value pair:
    [[se, attrList], ..]   I.e.:   [[se, [[key,val] ..]], ..]
   """

   guid = pGuid
   seListAttrList = pSeListAttrList
   trans = pTrans
   verbose = pVerbose

   err = 0
   rc = 0
   
   for seAttrListPair in seListAttrList:
      err = addOneReplica(guid, seAttrListPair[0], seAttrListPair[1], trans, verbose)
      if(err == -1):
         rc = err
         if(trans):  return err
   
   return rc



def insertOneEntry(pLfn, pAttrList, pSeListAttrList, pCreateParent, pTrans, pVerbose):
   """
    Tries to add replicas for the specified SEs (pSeList) to the specified fileblock
    entry (pLfn). It also tries to create the fileblock entry itself, if it did not
    exist yet.

    See createOneEntry and addOneReplica for more information on supported attributes.

    pAttrList is a list of key-value pairs for the entry itself: [[key,val], ..]
    pSeListAttrList is a list of SE-attribute list pairs, where each attribute list
    is a list of key-value pairs for that replica:
    [[se, attrList], ..]   I.e.:   [[se, [[key,val] ..]], ..]

    If pCreateParent == true, then the parent directory is checked for existence,
    and created if non existing.

    The specified LFN is used as it is, without using the LFC_HOME env
    variable (that should be pre-pended by the caller, or the LFC directory
    changed to it beforehand in order to use the relative path).

    Returns 0 if the whole operation was successfull and -1 otherwise.
   """

   lfn = pLfn
   attrList = pAttrList
   seListAttrList = pSeListAttrList
   createParent = pCreateParent
   trans = pTrans
   verbose = pVerbose

   err = 0
   rc = 0

 # Create the entry
   guid = createOneEntry(lfn, attrList, createParent, trans, verbose)
   if(guid == -1):
      return -1

 # Add the replicas
   err = addReplicas(guid, seListAttrList, trans, verbose)
   if(err == -1):
      rc = err
      if(trans):  return err

   return rc




def insertEntries(pLineList, pCreateParent, pTrans, pVerbose):
   """
    Tries to add the specified fileblocks and replicas by calling insertOneEntry for
    each of the lines in pLineList. These lines are strings containing information
    regarding the fileblock name (LFN), its attributes, and possibly several SEs where
    a replica is located and the attributes for them. 

    For each LFN present in pLineList, the contents of LFC_HOME env var are prepended
    for the relative paths. Absoulte paths are used as they are.

    Please see insertOneEntry for more information on attributes.
    
    The format of a line is then:
    <lfn> [attr_lfn_1=val ..] [<se1> [attr_se1_1=val ..] ..]

    All elements separated by whitespaces, and attributes identified by the presence
    of the '=' character.

    Returns 0 if all the operations were successfull and -1 otherwise.
   """

   lineList = pLineList
   createParent = pCreateParent
   trans = pTrans
   verbose = pVerbose
   
   err = 0
   rc = 0
   
   for line in pLineList:

      # Split
      line = (line.strip()).split()

      if(not line):
         continue

      # First is LFN
      lfn = line.pop(0)
      lfn = checkHome(lfn)

      # Then the LFN's attributes (key=val)
      attrList = []
      while(line):
         token=line[0]
         pos = token.find('=')
         if( pos == -1):
            break
         else:
            line.pop(0)
            attrPair = [token[:pos], token[(pos+1):]]
            attrList.append(attrPair)            

      # Then the SEs
      seListAttrList = []
      seAttrList = []
      se = ''
      for token in line:
         pos = token.find('=')
         if( pos == -1):
            if(se):
               seListAttrList.append([se, seAttrList])
            se = token
            seAttrList = []
         else:
            attrPair = [token[:pos], token[(pos+1):]]
            seAttrList.append(attrPair)
      if(se):
         seListAttrList.append([se, seAttrList])


      # Finally, do the insertion
      err = insertOneEntry(lfn, attrList, seListAttrList, createParent, trans, verbose)

      if(err == -1):
         rc = err
         if(trans):  return err

   # Now, return the error code
   return rc

            

###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Options and args... 
 
   longoptions=["help", "usage", "create-parent", "transaction", "verbose","from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'huptvf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)

   err=0
   createParent = False
   trans = False
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
           
       elif opt in ("-p", "--create-parent"):
           createParent = True
           
       elif opt in ("-t", "--transaction"):
           trans = True

       elif opt in ("-v", "--verbose"):
           verbose = True

       elif opt in ("-f","--from-file"):
           fromFile = True
           fname = val
           
  
 # Build the arguments 

   # From file
   if(fromFile):
      try:
         file=open(fname, 'r')
      except IOError, inst:
         msg="The file "+fname+" could not be opened: "+str(inst)+"\n"
         sys.stderr.write(msg)
         return -1
      lineList=file.readlines()
      
   # From command line options
   else:
      if(len(args)<1):
         print "Not enough input arguments"
         usage()
         return(-1)
      line=""
      for token in args:
         line += token +" "

      lineList = [line]

      
 # Do the update (under session/transaction)
 
   if(trans):
      lfc.lfc_starttrans("", "")
      err = insertEntries(lineList, createParent, trans, verbose)
      if(err):
         sys.stderr.write("Rolling back"+'\n')
         lfc.lfc_aborttrans()
      else:
         lfc.lfc_endtrans()
   else:
      lfc.lfc_startsess("", "")
      err = insertEntries(lineList, createParent, trans, verbose)
      lfc.lfc_endsess()


 # Finally, return error code
   return err



######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
