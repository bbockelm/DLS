#!/usr/bin/env python

#
# $Id: dlsCommandsCommon.py,v 1.1 2007/03/16 12:35:43 delgadop Exp $
#
# DLS Client. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
# 

#########################################
# Imports
#########################################
import dlsClient
from dlsApi import DLS_VERB_HIGH, DLS_VERB_NONE, DLS_VERB_WARN
from sys import stderr
from os import environ

#########################################
# Error codes
#########################################
# Error in the options or arguments parsing
OPT_ERROR = 230
# Error in the specified DLS type 
TYPE_ERROR = 231
# Error accessing a file specified as argument
FILE_ERROR = 232
# Caught API exception with no defined error code
GENERIC_ERROR = 250


#########################################
# CONSTANTS
#########################################
ADMITTED_VERB_VALUES = [0, 1, 2]


#########################################
# Handful functions for several CLI files
#########################################
def err(msg):
   """
   Prints the specified string msg to standard error (with an added trailing newline)
   """
   stderr.write(msg+'\n')


def check_iface_type(iface_type, admitted):
   if (not iface_type):
      iface_type = environ.get("DLS_TYPE")
   val = iface_type

   if (not (val in admitted)):
      err("Unsupported interface type: "+str(val)+"\nSupported values: "+str(admitted))
      return TYPE_ERROR
   if(val == "DLS_TYPE_DLI"):
      return dlsClient.DLS_TYPE_DLI
   if(val == "DLS_TYPE_DBS"):
      return dlsClient.DLS_TYPE_DBS
   if(val == "DLS_TYPE_PHEDEX"):
      return dlsClient.DLS_TYPE_PHEDEX
   if(val == "DLS_TYPE_LFC"):
      return dlsClient.DLS_TYPE_LFC
   if(val == "DLS_TYPE_MYSQL"):
      return dlsClient.DLS_TYPE_MYSQL


def check_verbosity_value(val):
  try:
    val = int(val)
  except ValueError, inst:
    err("Error: Unsupported verbosity value: %s " % str(val))
    return OPT_ERROR
  if (not (val in ADMITTED_VERB_VALUES)):
     err("Error: Unsupported verbosity value: %d" % (val))
     return OPT_ERROR
  return val



def create_iface_binding(iface_type, endpoint, dbsConf, verbose):
   iface = dlsClient.getDlsApi(iface_type, endpoint, dbs_client_config=dbsConf, checkEndpoint=True)
   if(verbose == 2):
      iface.setVerbosity(DLS_VERB_HIGH)
   else:
      if(verbose == 0):
         iface.setVerbosity(DLS_VERB_NONE)
      else:
         if(verbose == 1):
            iface.setVerbosity(DLS_VERB_WARN)
   return iface


def commonHelpText(admitted):

  print """The "-e" option can be used to set the DLS endpoint to use.

The "-i" option specifies the type of interface that should be used (which
depends on the DLS backend to access). If not specified, the interface type
is retrieved from:
    - DLS_TYPE environmental variable
If the interface type cannot be retrieved in any of these ways, the command fails.

Currently accepted values are:"""

  admitted_doc(admitted)

  print """
The "-v" option sets the verbosity level for the command. Accepted values are:
  -v 0 ==> print nothing else than error messages
  -v 1 ==> print also warning messages (default)
  -v 2 ==> print extra debug information

If "-u" is specified, usage information is displayed.

If "-h" is specified, help information is displayed.
   """

def admitted_doc(admitted):
  if "DLS_TYPE_PHEDEX" in admitted: 
    print " - DLS_TYPE_PHEDEX =>  DlsPhedexApi  class (read-only API with PhEDEx back-end)"
  if "DLS_TYPE_DBS" in admitted:
    print " - DLS_TYPE_DBS =>   DlsDbsApi class (almost complete API with DBS back-end)"
  if "DLS_TYPE_LFC" in admitted:
    print " - DLS_TYPE_LFC  =>  DlsLfcApi class (complete API with LFC back-end)"
  if "DLS_TYPE_MYSQL" in admitted:
    print " - DLS_TYPE_MYSQL =>  DlsMySQLApi  class (complete API with MySQL proto back-end)"
  if "DLS_TYPE_DLI" in admitted:
    print " - DLS_TYPE_DLI  =>  DlsDliClient class (getLocations only API with LFC back-end)"


