#!/usr/bin/env python
#
# 17-February-2006 Fabio Farina
#

import time, random
import threading 
import os, sys, string
#from string import Template
import aux, parameters

class dls_user(threading.Thread):
    def __init__(self, tcount, stats):
        threading.Thread.__init__(self)
        threading.currentThread().setName("thread-%i"%tcount)
        
        self.delay = 0.0
        self.tavg = 0.0
        self.tmax = -1.0
        self.tmin = 10000
        self.err_pc = self.succ_pc = 0
        self.stats = stats
        
        self.cmdList = []
        self.userAutomaton()
    
    def getcurrentThread(self):
        return threading.currentThread()
    
    def setDelayTime(self, t):
        self.delay = t
    
    def run(self):
       total = len(self.cmdList)
       if self.delay > 0.0 :
           time.sleep(self.delay)
       for n in range(total) :
           print threading.currentThread().getName() + " " + self.cmdList[n]
           tBase = time.time()
           std_in, std_out, std_err = os.popen3(self.cmdList[n])
           self.error = std_err.readlines()
           t = time.time()
           self.res = std_out.readlines()
           self.addTimeNotice(tBase, t, self.res, self.error)
       
       self.stats = "%s, "%time.ctime(t)+ threading.currentThread().getName()
       self.stats += ", %f, %f, %f, %f, %f"%( self.tmin, (self.tavg/total), self.tmax, float(self.succ_pc)/total, float(self.err_pc)/total ) 
       
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
        
    def userAutomaton(self):
        # actions sequence
        self.acts = []
        self.state = [0.0] *6
        # initial status
        self.acts.append(aux.userFirstAction(self.state))
        # chain
        #r = random.randrange(0, parameters.userBound)
        for l in range(parameters.userBound):
            self.acts.append(aux.userActionStep(self.state))
            #print self.state
            
        # expand commands
        aux.expandFunctionSequence(self.acts, self.cmdList, "", "")
        for i in range(len(self.cmdList)):
            r = random.randrange(0, parameters.threadNum)
            self.block = "blockpath%i/block%i"%(r, r)
            self.se = "SE%i"%r
            #templ = Template(self.cmdList[i])
            #self.cmdList[i] = templ.safe_substitute(block=self.block,se=self.se)
            #del templ
            
            # --- Python 2.3 compliant
            self.cmd = string.replace(self.cmd, "$block", self.block)
            self.cmd = string.replace(self.cmd, "$se", self.se)

        
            
            