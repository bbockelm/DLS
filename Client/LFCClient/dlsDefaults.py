#
# $Id: dlsDefaults.py,v 1.1 2008/06/17 14:44:24 delgadop Exp $
#
# DLS Client Defaults. $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module simply defines as constants some DLS default values, so that
 other modules can use them. Editing this file directly will have the same
 effect as modifying a configuration file. The runtime value for some of 
 the variables here may be modified by other means (such as API calls or 
 CLI options), for others may be not. In any case, the values here define 
 what the defaults are.
"""

DLS_PHEDEX_MAX_BLOCKS_PER_QUERY = 100
DLS_PHEDEX_MAX_BLOCKS_PER_FILE_QUERY = 50
DLS_PHEDEX_MAX_SES_PER_QUERY = 10
