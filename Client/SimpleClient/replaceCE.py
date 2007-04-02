#!/usr/bin/env python
                                                                                                                      
import sys, os, string, re
import commands


###########################################################################
def usage():
  """
  print the command line and exit
  """
  print sys.argv[0],' Replace from DLS all the fileblock in a old SE with the new SE \n'
  print '\tusage:'
  print '\t',sys.argv[0],' <SE name OLD> <SE name NEW> '
                                                                                                                      
###########################################################################
if __name__ == '__main__':
  """
  Remove all the fileblock in a old SE
  Add all the fileblock to the new SE
  """
  if len(sys.argv)<=2:
          usage()
          sys.exit(1)

  SEname=sys.argv[1]
  SEnameNEW=sys.argv[2]

  ## find all the blocks in a given SE
  SimpleClientDir='.'
  list_cmd=SimpleClientDir+'/dls-get-datablock --se '+SEname+' --host lxgate10.cern.ch --port 18081'
  datablocks=commands.getoutput(list_cmd)
  #print datablocks
  
  ## delete each of them in the old SE and add inot the new SE
  listdatablock=string.split(datablocks,'\n')
  for datablock in listdatablock:
    del_cmd=SimpleClientDir+'/dls-remove-replica --datablock '+datablock+' --se '+SEname+' --host lxgate10.cern.ch --port 18081'
    #print del_cmd
    os.system(del_cmd)
    add_cmd=SimpleClientDir+'/dls-add-replica --datablock '+datablock+' --se '+SEnameNEW+' --host lxgate10.cern.ch --port 18081'
    #print add_cmd
    os.system(add_cmd)

  sys.exit(0)



