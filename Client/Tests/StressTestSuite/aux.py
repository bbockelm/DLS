#!/usr/bin/env python
#
# 16-February-2006 Fabio Farina
#
import parameters, time
import random, math
from dls_client import *
import threading

def runRepetition(acts, dist):
    cmds=[]
    thrList = []
    outputs = [""] * (parameters.threadNum+1)
    outputs[0] = "Time, ClientID, tmin, tAvg, tMAX, successPerCent, failurePerCent"
    localActs = acts
    
    tInit = time.time()
    expandFunctionSequence(localActs, cmds, '', '')
    # generate clients and run tests
    for i in range(parameters.threadNum):
       thr = dls_client(cmds, i, i, i, outputs[i+1])
       thrList.append(thr)
       if (dist == "poisson"):
           thr.start()
    
    if dist != "poisson":
        if dist == "pareto":
            print "Pareto arrivals simulation"
            for i in range(parameters.threadNum):
                thrList[i].setDelayTime(clientWaitParetoTime())
        else:
            print "Ready for burst-start"
            
        for i in range(parameters.threadNum):
            thrList[i].start()
            
    while (threading.activeCount() > 1):
        pass
    # resuming 
    tEnd = time.time()
    tTest = tEnd-tInit
    print "Test Total Time: %f"%(tTest)
    print "Test Case Mean Time: %f"%(tTest/parameters.threadNum)
    # print output
    for i in range(parameters.threadNum):
        outputs[i+1] = thrList[i].stats
    saveStatistics("repetition.csv",outputs)

    # dealloc
    del outputs
    del thrList
    del cmds
    
def runConcurrent(acts, targets, dist, saveAs, hugeData):
    thrList = []
    outputs = [""] * (parameters.threadNum+1)
    outputs[0] = "Time, ClientID, tmin, tAvg, tMAX, successPerCent, failurePerCent"
    cmds= [[]] * len(acts)
    
    tInit = time.time()
    # generate clients and run tests
    for i in range(len(acts)):
       c = []
       if hugeData==0: 
           expandFunctionSequence(acts[i], c, '', '')
       else:
           preB = "%d"%i 
           preS = "%d"%i
           preB *= parameters.magBlockPreambleLen
           preS *= parameters.magSePreambleLen
           expandFunctionSequence(acts[i], c, preB, preS)
       cmds[i] = c
       del c
    
    for i in range(parameters.threadNum):
       index = i % len(acts)
       thr = dls_client(cmds[index], i, targets[index], targets[index], outputs[i+1])
       thrList.append(thr)
       if (dist == "poisson"):
           thr.start()
    
    if dist != "poisson":
        if dist == "pareto":
            print "Pareto arrivals simulation"
            for i in range(parameters.threadNum):
                thrList[i].setDelayTime(clientWaitParetoTime())
        else:
            print "Ready for burst-start"
            
        for i in range(parameters.threadNum):
            thrList[i].start()
            
    while (threading.activeCount() > 1):
        pass
    # resuming 
    tEnd = time.time()
    tTest = tEnd-tInit
    print "Test Total Time: %f"%(tTest)
    print "Test Case Mean Time: %f"%(tTest/parameters.threadNum)
    
    for i in range(parameters.threadNum):
        outputs[i+1] = thrList[i].stats
    if saveAs==0:
        saveStatistics("concurrency.csv",outputs)
    elif saveAs==1:
        saveStatistics("magnitude.csv",outputs)
    elif saveAs==2:
        saveStatistics("random.csv",outputs)
    
    # dealloc
    del outputs
    del thrList

def expandFunctionSequence(actionsList, globalList, blockPreamble, sePreamble):
    acts = []
    acts = actionsList

    for n in range(parameters.nBlocks) :
        b = blockPreamble+"$block" + "-%i"%n
        s = ' '
        for rep_num in range(parameters.nReplicas):
            s += sePreamble+"$se-%i"%n + "-%i"%rep_num
            if rep_num < parameters.nReplicas-1:
                s += " "
             
        for i in range(len(acts)):
            par_lis=""
            if acts[i] == "add":
                par_lis = parameters.clientDir + "dlslfc-add -p "
                par_lis += b
                par_lis += s
            elif acts[i] == "add-tr":
                par_lis = parameters.clientDir + "dlslfc-add -p -t "
                par_lis += b
                par_lis += s
            elif acts[i] == "del":
                par_lis = parameters.clientDir + "dlslfc-delete -a "
                par_lis += b
            elif acts[i] == "get":
                par_lis = parameters.clientDir + "dlslfc-get-se "
                par_lis += b
            elif acts[i] == "dli":
                par_lis = parameters.clientDir + "lfc-dli-client -e http://$LFC_HOST:8085/ lfn:$LFC_HOME/"
                par_lis += b
            elif acts[i] == "lcg":
                par_lis = "lcg-lr --vo cms lfn:$LFC_HOME/"
                par_lis += b

            globalList.append(par_lis)
              
def saveStatistics(filename, data):
    writer = open(filename, "ab")
    for row in data:
        writer.write(row+"\n")
    writer.close()

def clientWaitParetoTime():    # transform method the implemented pareto rnd doesn't allow to control the k parameter
    return parameters.paretoK * pow( 1.0 - random.random(), -1.0/parameters.paretoAlpha)

def randomize():
    return int(math.ceil(random.random()*parameters.randFactor))

def userFirstAction(state):
        cumulate = [0.0] *6
        for i in range(6):
            for j in range(i+1):
                cumulate[i] += parameters.userInitStateP[j]
        
        r = random.random()
        for i in range(6):
            if r < cumulate[i]:
                break
        state[i] = 1.0
        if i==0 :
            return "add"
        elif i==1:
            return "add-tr"
        elif i==2:
            return "del"
        elif i==3:
            return "get"
        elif i==4:
            return "dli"
        elif i==5:
            return "lcg"
 
def userActionStep(state):
        localState = []
        localState += state
        cumulate = [0.0]*6
        # get new state probabilities
        for i in range(6):
            if localState[i] == 1.0:
                break
        localState = parameters.userMarkov[i]
                
        # draw a new action
        for i in range(6):
            for j in range(i+1):
                cumulate[i] += localState[j]
        
        r = random.random()
        for i in range(6):
            if r < cumulate[i]:
                break
            
        for j in range(6):
            state[j] = 0.0
        state[i] = 1.0
        # return the action
        if i==0 :
            return "add"
        elif i==1:
            return "add-tr"
        elif i==2:
            return "del"
        elif i==3:
            return "get"
        elif i==4:
            return "dli"
        elif i==5:
            return "lcg"
        
        
        
        
