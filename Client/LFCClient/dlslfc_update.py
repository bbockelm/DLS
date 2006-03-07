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
   print "Usage: dlslfc-update [-v] [-t] <fileblock> [attr=val ..] [<SE> [attr=val ..]  ..]"
   print "       dlslfc-update [-v] [-t] -f <listing_file>"
   print "       dlslfc-update -u"
   print "       dlslfc-update -h"

def options():
   """
    Provides some information regarding the available options
   """
   print """Options summary:
   -h, --help
   -u, --usage
   -t, --transaction
   -v, --verbose
   -f, --from-file
   """

def help():
   """
    Provides some help information
   """
   print """This script will update the attributes of a fileblock (or one its replicas,
if the corresponding SE is specified) in the DLS server. Attibutes must be specified
in a key=val format, and only known attributes will be considered.

Understood fileblock attributes are:    
 filesize, csumtype, csumvalue

Understood replica attributes are:    
 ptime, atime

If atime=xxx is specified, the value for the attribute will not be taken into account,
but the access time of the replica will be updated to present time.
 
The "-f" option can be used to retrieve fileblock names from a file rather than from the
given arguments. The file must contain one line per fileblock to be added, with the
optional SE and attributes in the same line and separated by whitespaces.

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



def updateOneLfn(pLfn, pAttrList, pTrans, pVerbose):
   """
    Updates attributes in an entry with the specified fileblock (pLfn), for the attributes in
    pAttrList that are understood.
    pAttrList should be a list of key-value pairs: [[key,val], ..]
    Understood fileblock attributes are:    
      filesize, csumtype, csumvalue
      
    The method returns 0 if the update was successful or -1 if an error occurred.
   """
   lfn = pLfn
   attrList = pAttrList
   trans = pTrans
   verbose = pVerbose

   lfn = checkHome(lfn)
      
   if(verbose):
      print "--updateOneLfn(",lfn,",",pAttrList,")"

   # Check if entry exists
   fstat=lfc.lfc_filestatg()
   if(lfc.lfc_statg(lfn, "", fstat) <0):
      sys.stderr.write("Error accessing specified LFN: "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
      return -1

   # Get current guid, filesize, csumtype, csumvalue
   guid = fstat.guid
   filesize = fstat.filesize
   csumtype = fstat.csumtype
   csumvalue = fstat.csumvalue
   
   # Analyze attribute list to modify what was passed
   update = False
   for attr in attrList:
      if(attr[0] == "filesize"):
         filesize=long(attr[1])
         update = True
         continue
      if(attr[0] == "csumtype"):  
         csumtype=attr[1]
         update = True
         continue
      if(attr[0] == "csumvalue"): 
         csumvalue=attr[1]
         update = True
         continue
      else:
         print "Warning: Fileblock attribute \""+attr[0]+"\" unknown."

   # Set the size and cksum
   if(update):
      if(verbose):
         print "--lfc.lfc_setfsizeg(\""+guid+"\",",filesize,",",csumtype,",",csumvalue,")"
      if (lfc.lfc_setfsizeg(guid, filesize, csumtype, csumvalue)):
         sys.stderr.write("Error setting the size/cksum for the LFN: "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return (-1)


 # If everything ewnt well, return success
   return 0



#TODO: If this method is not used, we could as well remove it, since it is very easy to 
#      implement it in user code (with the exact logic that the user requires).
#def updateLFNs(pLfnAttrList, pTrans, pVerbose):
#   """
#    Update entries with the specified fileblocks and setting the specified attributes
#    (see the method updateOneLFN).
#    pLfnAttrList should be a list of one LFN and a attribute list, where each attribute
#    is a key-value pair:
#    [[lfn, attrList], ..]   I.e.:   [[lfn, [[key,val] ..]], ..]
#
#    The method returns 0 if all the entries were updated correctly, or -1 if an error
#    occurred.
#   """
#
#   lfnAttrList = pLfnAttrList
#   trans = pTrans
#   verbose = pVerbose
#
#   err = 0
#   rc = 0
#   
#   for lfnAttrPair in lfnAttrList:
#      err = updateOneLFN(lfnAttrPair[0], lfnAttrPair[1], trans, verbose)
#      if(err == -1):
#         rc = err
#         if(trans):  return err
#   
#   return rc


def updateOneReplica(pLfn, pSe, pAttrList, pTrans, pVerbose):
   """
    Update a replica with the specified SE as host to the specified fileblock for the
    attributes in pAttrList that are understood.
    pAttrList should be a list of key-value pairs: [[key,val], ..]
    Understood replica attributes are:    
      ptime, atime

    For "atime" the value of the attribute is not considered, but the access time is set
    to present time.
   """
   lfn = pLfn
   se = pSe
   attrList = pAttrList
   trans = pTrans
   verbose = pVerbose

 # Get the replica

   lfn = checkHome(lfn)
      
   # First check we can access the entry (cause next method's error are difficult to get)
   R_OK=4
   if(lfc.lfc_access(lfn, R_OK)<0):
      sys.stderr.write("Error when accessing provided lfn: "+lfn+
                       ": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
      return -1
   
   # Call the retrieval in a loop
   found = False
   #list = lfc.lfc_list()
   #flags = lfc.CNS_LIST_BEGIN 
   #filerep=lfc.lfc_listreplica(lfn, "", flags, list)
   #while(filerep):
   #   if(verbose):
   #      print "--lfc.lfc_listreplica(\""+lfn+"\", \"\",",flags,",list)"

   if(verbose):
      print "--lfc.lfc_getreplica(\""+lfn+"\", \"\",\"\")"
   err, list = lfc.lfc_getreplica(lfn, "", "")
   if(err):
      sys.stderr.write("Error retrieving replicas for: "+lfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
      return -1

   for filerep in list:                    
      if(filerep.host == se):
         found = True
         sfn = filerep.sfn
         ptime = filerep.ptime
         f_type = filerep.f_type  # This cannot be updated yet (nor SFN)
         break
         
      #flags=lfc.CNS_LIST_CONTINUE
      #filerep=lfc.lfc_listreplica(lfn, "", flags, list)
  
   #flags=lfc.CNS_LIST_END
   #lfc.lfc_listreplica(lfn, "", flags, list)

   if(not found):
      sys.stderr.write("No replica on \""+se+"\" for the specified fileblock: "+lfn+'\n')
      return -1

   # Analyze attribute list to modify what was passed
   update_atime = False
   update_ptime = False
   for attr in attrList:
      if(attr[0] == "atime"):      
         update_atime = True
         continue
      if(attr[0] == "ptime"):      
         ptime = int(attr[1])
         update_ptime = True
         continue
      else:
         print "Warning: Replica attribute \""+attr[0]+"\" unknown."

   # Set pin time
   if(update_ptime):
      if(verbose):
         print "--lfc.lfc_setptime(\""+lfn+"\",",ptime,")"   
      if(lfc.lfc_setptime(sfn, ptime)<0):
         sys.stderr.write("Error setting pin time for SFN: "+sfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return -1
         

   # Set access time
   if(update_atime):
      if(verbose):
         print "--lfc.lfc_atime(\""+lfn+"\")"   
      if(lfc.lfc_setratime(sfn)<0):
         sys.stderr.write("Error setting access time for SFN: "+sfn+": "+lfc.sstrerror(lfc.cvar.serrno)+'\n')
         return -1


   # If arrived here, return success
   return(0)
 


def updateReplicas(pLfn, pSeListAttrList, pTrans, pVerbose):
   """
    Updates replicas with the specified SEs as hosts, to the specified fileblock, with
    the specified attributes (see the method updateOneReplica).
    pSeAttrList should be a list of one SE and an attribute list, where each attribute
    is a key-value pair:
    [[se, attrList], ..]   I.e.:   [[se, [[key,val] ..]], ..]
   """

   lfn = pLfn
   seListAttrList = pSeListAttrList
   trans = pTrans
   verbose = pVerbose

   err = 0
   rc = 0

   for seAttrListPair in seListAttrList:
      err = updateOneReplica(lfn, seAttrListPair[0], seAttrListPair[1], trans, verbose)
      if(err == -1):
         rc = err
         if(trans):  return err
   
   return rc



def updateOneEntry(pLfn, pAttrList, pSeListAttrList, pTrans, pVerbose):
   """
    Updates the attributes of the specified fileblock (pLfn) and also of the replicas
    in the specified SEs (pSeList) for it.

    See updateOneLfn and updateOneReplica for more information on supported attributes.

    pAttrList is a list of key-value pairs for the entry itself: [[key,val], ..]
    pSeListAttrList is a list of SE-attribute list pairs, where each attribute list
    is a list of key-value pairs for that replica:
    [[se, attrList], ..]   I.e.:   [[se, [[key,val] ..]], ..]

    Returns 0 if the whole operation was successfull and -1 otherwise.
   """

   lfn = pLfn
   attrList = pAttrList
   seListAttrList = pSeListAttrList
   trans = pTrans
   verbose = pVerbose

   err = 0
   rc = 0
   
 # Update the LFN
   err = updateOneLfn(lfn, attrList, trans, verbose)
   if(err == -1):
      return err

 # Update the replicas
   err = updateReplicas(lfn, seListAttrList, trans, verbose)
   if(err == -1):
      rc = err
      if(trans):  return err

   return rc




def updateEntries(pLineList, pTrans, pVerbose):
   """
    Updates the attributes of the specified fileblocks and their replicas in the
    specified SEs by calling updateOneEntry for each of the lines in pLineList.
    These lines are strings containing information regarding the fileblock name
    (LFN), its attributes, and possibly several SEs where a replica is located
    and the attributes for these replicas. 

    Please see updateOneEntry for more information on attributes.
    
    The format of a line is then:
    <lfn> [attr_lfn_1=val ..] [<se1> [attr_se1_1=val ..] ..]

    All elements separated by whitespaces, and attributes identified by the presence
    of the '=' character.

    Returns 0 if all the operations were successfull and -1 otherwise.
   """

   lineList = pLineList
   trans=pTrans
   verbose=pVerbose

   err = 0
   rc = 0
   
   for line in pLineList:
      # Split
      line = (line.strip()).split()

      if(not line):
         continue

      # First is LFN
      lfn = line.pop(0)

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

      # Finally, do the update
      err = updateOneEntry(lfn, attrList, seListAttrList, trans, verbose)

      if(err == -1):
         rc = err
         if(trans):  return err

   return rc

            

###################### MAIN FUNCTION ########################

def main(pArgs):
   """
    Performes the main task of the script (invoked directly).
    For information on its functionality, please call the help function.
   """

 # Options and args... 
 
   longoptions=["help", "usage", "transaction", "verbose","from-file"]
   try:
      optlist, args = getopt.getopt(pArgs, 'hutvf:', longoptions)
   except getopt.GetoptError, inst:
      sys.stderr.write("Bad usage: "+str(inst)+'\n')
      usage()
      sys.exit(-1)

   err = 0
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
      if(len(args)<2):
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
      err = updateEntries(lineList, trans, verbose)
      if(err):
         sys.stderr.write("Rolling back"+'\n')
         lfc.lfc_aborttrans()
      else:
         lfc.lfc_endtrans()
   else:
      lfc.lfc_startsess("", "")
      err = updateEntries(lineList, trans, verbose)
      lfc.lfc_endsess()


 # Finally, return error code
   return err



######################### SCRIPT ###########################

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
