#!/usr/bin/env python
#
# 18-Nov-2005 A. Fanfani  $Id:
#
import sys, os, string, getopt
import time
import threading 

###########################################################################
def usage():
  """
  print the command line and exit

  """
  print " "
  print 'usage: ',sys.argv[0],' <options>'
  print "Options"
  print "-h,--help \t\t\t\t\t Show this usage"
  print "-f,--function <function> \t\t\t name of the function to use (add, get-se, del)"
  print "-t,--threads <threads> \t\t\t\t number of threads"
  print "-n,--nblocks_per_thread <nblocks_per_thread> \t number of blocks per thread"
  print "-d,--dlstype <type> \t\t\t\t type of DLS client (lfc or proto) "
  print "-w,--withtransactions \t\t\t enable transaction DLS client (only for lfc)\n"
# ##########################################
def printtime(func,t1,t2,statfile,timeList):
  func_time = ((t2 - t1) / 60.0, t2 - t1, (t2 - t1) * 1000.0)
  #print timedelta(t2 - t1)
  print "%s took %0.3fm %0.3fs %0.3fms\n"%(func,func_time[0],func_time[1],func_time[2])   
  statfile.write("%s took %0.3fm %0.3fs %0.3fms\n"%(func,func_time[0],func_time[1],func_time[2]))
  timeList.append(func_time[1])

# ##############################################
class DLSlfc_clientThread(threading.Thread):
        def __init__(self, function, block, se, tcount, nblocks, statfile, timeList, transaction):
            threading.Thread.__init__(self)
            threading.currentThread().setName("thread-%i"%tcount)
            self.block = block
            self.se = se
            self.nblocks = nblocks
            self.clientdir='/bohome/fanfani/LFC/LFCproto'
            self.transaction=transaction
        def getcurrentThread(self):
            return threading.currentThread()
        def run(self):
           for n in range(self.nblocks) :
            b=self.block+"-%i"%n
            s=self.se+"-%i"%n
            if function=="add" :
             if self.transaction :
               cmd=self.clientdir+'/dlslfc-add -p -t '+b+' '+s
             else:
               cmd=self.clientdir+'/dlslfc-add -p '+b+' '+s
            elif function=="get-se":
             cmd=self.clientdir+'/dlslfc-get-se '+b
            elif function=="del":
             cmd=self.clientdir+'/dlslfc-delete -a '+b
             #cmd='./dlslfc-delete '+b+' '+s
            #print cmd
            t1 = time.time()
            os.system(cmd)
            t2 = time.time()
            printtime(cmd,t1,t2,statfile,timeList)

# ##############################################
class DLSproto_clientThread(threading.Thread):
        def __init__(self, function, block, se, tcount, nblocks, statfile, timeList):
            threading.Thread.__init__(self)  
            threading.currentThread().setName("thread-%i"%tcount)      
            self.block = block
            self.se = se
            self.nblocks = nblocks
            self.clientdir='/bohome/fanfani/COMP/DLS/Client/SimpleClient'
            self.serverhost='lxgate10.cern.ch'
            self.serverport='18081'
        def getcurrentThread(self):
            return threading.currentThread()
        def run(self):
           for n in range(self.nblocks) :
            b=self.block+"-%i"%n
            s=self.se+"-%i"%n
            if function=="add" :
             cmd=self.clientdir+'/dls-add-replica --datablock '+b+' --se '+s+' --host '+self.serverhost+' --port '+self.serverport
            elif function=="get-se":
             cmd=self.clientdir+'/dls-get-se --datablock '+b+' --host '+self.serverhost+' --port '+self.serverport
            elif function=="del":
             cmd=self.clientdir+'/dls-remove-replica --datablock '+b+' --se '+s+' --host '+self.serverhost+' --port '+self.serverport
            #print cmd
            t1 = time.time()
            os.system(cmd)
            t2 = time.time()
            printtime(cmd,t1,t2,statfile,timeList)   

# ##############################################
class NoLFCHOSTError:
  def __init__(self):
    print '\nERROR : LFC_HOST environment variable is not defined\n'
    pass
###########################################################################
if __name__ == '__main__':
     """
      test
     """
## handle options
     long_options=["help","nblocks=","threads=","function=","dlstype=","withtransactions"]
     short_options="hn:t:f:d:w"
     try:
         opts, args = getopt.getopt(sys.argv[1:],short_options,long_options)
     except getopt.GetoptError:
         usage()
         print "err"
         sys.exit(1)

     if len(opts)<1:
         usage()
         sys.exit(1)

     nblocks = None
     threads = None
     function = None
     transaction = 0
     dlstype='lfc'

     for o, a in opts:
            if o in ("-h", "--help"):
                usage()
                sys.exit(1)
            if o in ("-t", "--threads"):
                threads=int(a)
            if o in ("-n", "--nblocks"):
                nblocks=int(a)
            if o in ("-f", "--function"):
                function=a
            if o in ("-d", "--dlstype"):
                dlstype=a
            if o in ("-w", "--withtransactions"):
                transaction = 1

     if nblocks==None:
            usage()
            print "Error: --nblocks_per_thread <nblocks_per_thread> is required"
            sys.exit(1)

     if threads==None:
            usage()
            print "Error: --threads <threads> is required"
            sys.exit(1)

     if function==None:
            usage()
            print "Error: --function <function> is required"
            sys.exit(1)
     if ( function!="add" ) & (function!="get-se") & (function!="del"):
         usage()
         print "Error: --function <function> can be \"add\" or \"get-se\" or \"del\""
         sys.exit(1)

     if ( dlstype!="lfc" ) & (dlstype!="proto"):
         usage()
         print "Error: --dlstype <type> can be \"lfc\" or \"proto\""
         sys.exit(1)

     if ( dlstype=="lfc" ):  
       try:
          lfc = os.environ['LFC_HOST']
       except:
          raise NoLFCHOSTError


## here the script really starts

     outfile = "statDLS%s_%s_%ithreads_%iblocksperthread.out"%(dlstype,function,threads,nblocks)
     statfile = open(outfile,'w')
     timeList=[]
     tcount=0
     tstart = time.time()
     for t in range(threads) :

       ## number the threads and define block and se based on thread number
       tcount=tcount+1
       #block="block%i"%tcount
       #block="blockpath/block%i"%tcount
       block="blockpath%i/block%i"%(tcount,tcount)
       se="SE%i"%tcount

       ## start the thread for the specified client command
       if (dlstype=='lfc'):
         thr = DLSlfc_clientThread(function,block,se,tcount,nblocks,statfile,timeList,transaction)
       elif (dlstype=='proto'):
         thr = DLSproto_clientThread(function,block,se,tcount,nblocks,statfile,timeList)
       #print 'starting thread %i'%tcount
       thr.start()
       #print 'thread name: '+thr.getcurrentThread().getName()

       #thr.join() # Wait for the background task to finish

     ## wait for all threads to end
     while ( threading.activeCount() > 1):
       pass
     #print "active count %i"%threading.activeCount()
     ## compute rate
     tend=time.time()
     timetot=tend-tstart
     ntot=nblocks*threads
     rate=ntot/timetot
     ## compute average-max-min time 
     sum_threadstime=0.
     for threadtime in timeList:
      sum_threadstime=sum_threadstime+threadtime

     ## write the overall statistics
     statfile.write(">>> DLSclient %s operation: %s #threads: %i #blocks_per_thread: %i \n"%(dlstype,function,threads,nblocks))
     statfile.write("Total time = %0.3fs \n"%(timetot))
     statfile.write("Number of %s blocks ( %i blocks for %i threads) = %i \n"%(function,nblocks,threads,ntot))
     statfile.write("%s Rate = %s \n"%(function,rate))
     statfile.write("%s average-max-min time  = %0.3fs - %0.3fs - %0.3fs \n"%(function,(sum_threadstime/len(timeList)),max(timeList),min(timeList)))
     statfile.close()
     sys.exit(0)
