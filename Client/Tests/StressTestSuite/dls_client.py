#!/usr/bin/env python
#
# 17-February-2006 Fabio Farina
#

import time
import threading 
import os, sys, string
#from string import Template

class dls_client(threading.Thread):
    def __init__(self, cmdList, tcount, blockID, seID, stats):
        threading.Thread.__init__(self)
        threading.currentThread().setName("thread-%i"%tcount)
        
        self.block = "blockpath%i/block%i"%(blockID, blockID)
        self.se = "SE%i"%seID

        self.cmdList = cmdList
        self.delay = 0.0
        self.tavg = 0.0
        self.tmax = -1.0
        self.tmin = 10000
        self.err_pc = self.succ_pc = 0
        self.stats = stats
        self.cmd = []
        
    def getcurrentThread(self):
        return threading.currentThread()
    
    def setDelayTime(self, t):
        self.delay = t
    
    def run(self):
       total = len(self.cmdList)
       
       if self.delay > 0.0:
           time.sleep(self.delay)
            
       for n in range(total) :
           self.cmd = self.cmdList[n]
           
           # --- Python 2.4 compliant
           #templ = Template(self.cmd)
           #self.cmd = templ.safe_substitute(block=self.block,se=self.se)
           #del templ
           
           # --- Python 2.3 compliant
           self.cmd = string.replace(self.cmd, "$block", self.block)
           self.cmd = string.replace(self.cmd, "$se", self.se)
           
           
           print threading.currentThread().getName() + " " + self.cmd
           
           tBase = time.time()
           std_in, std_out, std_err = os.popen3(self.cmd)
           self.error = std_err.readlines()
           t = time.time()
            
           self.res = std_out.readlines()
           self.addTimeNotice(tBase, t, self.res, self.error)
       
       self.stats = "%s, "%time.ctime(t)+ threading.currentThread().getName()
       self.stats += ", %f, %f, %f, %f, %f"%( self.tmin, (self.tavg/total), self.tmax, float(self.succ_pc)/total, float(self.err_pc)/total ) 
       print self.stats
       
    def addTimeNotice(self, t1, t2, out, err):
        # unexpected error check (use both err and success)
        if err:
            self.err_pc += 1
        if out:
            self.succ_pc += 1
        
        self.delta = t2 - t1
        self.tavg += self.delta
        
        if self.delta < self.tmin:
            self.tmin = self.delta
        
        if self.delta > self.tmax:
            self.tmax = self.delta
        
        
            
            