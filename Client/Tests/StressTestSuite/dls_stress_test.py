#!/usr/bin/env python
#
# 16-February-2006 Fabio Farina
#

import aux, time, random, math
import parameters
from dls_user import *
import threading

def repetitionTest(stoc_dist):
    tStart = time.time()
    print "******* STARTING REPETITION TEST *******\n"
    print "******* DLS-Proto Tests *******\n\n"
    
    # standard fair sequence
    print "Standard fair sequences\n"
    aux.runRepetition(["add"], stoc_dist)
    aux.runRepetition(["get"], stoc_dist)
    aux.runRepetition(["dli"], stoc_dist)
    aux.runRepetition(["lcg"], stoc_dist)
    
    aux.runRepetition(["del"], stoc_dist)
 
    aux.runRepetition(["add-tr"], stoc_dist)
    aux.runRepetition(["get"], stoc_dist)
    aux.runRepetition(["dli"], stoc_dist)
    aux.runRepetition(["lcg"], stoc_dist)
    
    aux.runRepetition(["del"], stoc_dist)
    
    print "\t Unfair sequences"
    aux.runRepetition(["add-tr"], stoc_dist)
    aux.runRepetition(["add-tr"], stoc_dist)
    aux.runRepetition(["del"], stoc_dist)
    aux.runRepetition(["del"], stoc_dist)
    
    aux.runRepetition(["get"], stoc_dist)
    aux.runRepetition(["dli"], stoc_dist)
    aux.runRepetition(["lcg"], stoc_dist)
        
    print "\t Composed sequences"
    aux.runRepetition(["add","add","del"], stoc_dist)
    aux.runRepetition(["add-tr","get","get","get","del"], stoc_dist)
    aux.runRepetition(["add-tr","dli","dli","dli","del"], stoc_dist)
    aux.runRepetition(["add-tr","lcg","lcg","lcg","del"], stoc_dist)
    
    #...other test cases ????
 
    print "******* REPETITION TEST COMPLETED *******"
    tStop = time.time()
    print "Total Repetition Test Time: %f"%(tStop-tStart)
    
def concurrencyTest(stoc_dist):
    tStart = time.time()
    print "******* STARTING CONCURRENCY AND RACE CONDITIONS TEST *******\n"
    
    print "Concurrent add/delete among clients"
    actions= [["add"],["del"]] * (parameters.threadNum/2)
    targetClients = [i/3 for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 0, 0)
    #print actions,targetClients
    #return
    print "Concurrent add/get among clients"
    actions= [["add-tr"],["get"],["dli"],["lcg"]] * (parameters.threadNum/4)
    targetClients = [i/3 for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 0, 0)
    
    print "Concurrent get/delete among clients"
    actions= [["get"],["dli"],["lcg"],["del"]] * (parameters.threadNum/4)
    targetClients = [i/3 for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 0, 0)
    
    # 3 kind of concurrent sequences
    print "Complex sequence of actions on a unique fileblock"
    actions= [["add-tr","get","get","del"]]
    for i in range(parameters.threadNum-1):
        actions.append(["get","lcg","dli"])
    targetClients = [0 for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 0, 0)
    
    #...other test cases ????
     
    print "******* CONCURRENCY AND RACE CONDITIONS TEST COMPLETED *******"
    tStop = time.time()
    print "Total Race Conditions Test Time: %f"%(tStop-tStart)

def magnitudeTest(stoc_dist):
    tStart = time.time()
    print "******* STARTING MAGNITUDE TEST *******\n"
     
    print "LFC Server induced load imbalance"
    parameters.threadNum *= parameters.magThread
    actions= [["add"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)
    
    actions= [["get"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)
    
    actions= [["dli"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)
    
    actions= [["lcg"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)

    actions= [["del"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)
    
    parameters.threadNum /= parameters.magThread

    print "Block/Replica Flooding"
    parameters.nBlocks *= parameters.magBlocks
    parameters.nReplicas *= parameters.magBlocks
    
    actions= [["add-tr"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)
    # ###
    print "High Latency Pareto Arrivals"
    parameters.paretoK *= parameters.magParetoK
    actions= [["get"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, "pareto", 1, 0)
    
    actions= [["dli"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, "pareto", 1, 0)
    
    actions= [["lcg"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, "pareto", 1, 0)
    parameters.paretoK /= parameters.magParetoK
    # ### 
    print "Flood cleanup"
    actions= [["del"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 0)
    
    parameters.nBlocks /= parameters.magBlocks
    parameters.nReplicas /= parameters.magBlocks
    
    print "Huge data size"
    actions= [["add-tr"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 1)

    actions= [["del"]] * (parameters.threadNum)
    targetClients = [i for i in range(parameters.threadNum)]
    aux.runConcurrent(actions, targetClients, stoc_dist, 1, 1)

    #...other test cases ????
    
    print "******* MAGNITUDE TEST COMPLETED *******"
    tStop = time.time()
    print "Total Magnitude Test Time: %f"%(tStop-tStart)

def randomVariationTest(stoc_dist):
    print "******* RANDOM VARIATION TEST *******\n"
    print "---------------------------------------"
    print "WARNING: The random variation test repeate the other tests"
    print "using randomized parameters. The output will be appended to"
    print "the previous test scenarios."
    print "---------------------------------------\n\n"
    
    # original values backup
    ori_thNum = parameters.threadNum
    ori_nBlocks = parameters.nBlocks
    ori_nReplicas = parameters.nReplicas
    ori_paretoK = parameters.paretoK
    ori_magThread = parameters.magThread
    ori_magBlocks = parameters.magBlocks
    ori_magReplicas = parameters.magReplicas
    ori_magBlockPreambleLen = parameters.magBlockPreambleLen
    ori_magSePreambleLen = parameters.magSePreambleLen
    
    # run test
    #fixed random seed. reduce variability (easier analysis)
    r = random.randrange(1, 100)
    random.seed(1234567)
    for i in range(r):
       random.random()
    print "Random fixed seed pre-cycles: %d"%r
    
    for i in range(parameters.randRepetitions):
        parameters.threadNum = ori_thNum * aux.randomize()
        parameters.nBlocks = ori_nBlocks * aux.randomize()
        parameters.nReplicas = ori_nReplicas * aux.randomize()
        parameters.paretoK = ori_paretoK * aux.randomize()
        parameters.magThread = ori_magThread * aux.randomize()
        parameters.magBlocks = ori_magBlocks * aux.randomize()
        parameters.magReplicas = ori_magReplicas * aux.randomize()
        parameters.magBlockPreambleLen = ori_magBlockPreambleLen * aux.randomize()
        parameters.magSePreambleLen = ori_magSePreambleLen * aux.randomize()

        repetitionTest(stoc_dist)
        concurrencyTest(stoc_dist)
        magnitudeTest(stoc_dist)
        
            
    # original values restore
    random.seed()    
        
    parameters.threadNum = ori_thNum 
    parameters.nBlocks = ori_nBlocks
    parameters.nReplicas = ori_nReplicas
    parameters.paretoK = ori_paretoK
    parameters.magThread = ori_magThread
    parameters.magBlocks = ori_magBlocks
    parameters.magReplicas = ori_magReplicas
    parameters.magBlockPreambleLen = ori_magBlockPreambleLen
    parameters.magSePreambleLen = ori_magSePreambleLen

    print "******* RANDOM VARIATION TEST COMPLETED *******"
 
def userSimulationTest(stoc_dist):
    tStart = time.time()
    print "******* USER SIMULATION TEST *******\n"
    thrList = []
    outputs = [""] * (parameters.threadNum+1)
    outputs[0] = "Time, ClientID, tmin, tAvg, tMAX, successPerCent, failurePerCent"
    
    # generate clients and run tests
    for i in range(parameters.threadNum):
       thr = dls_user(i, outputs[i+1])
       thrList.append(thr)
       if (stoc_dist == "poisson"):
           thr.start()
    
    if stoc_dist != "poisson":
        if stoc_dist == "pareto":
            print "Pareto arrivals simulation"
            for i in range(parameters.threadNum):
                thrList[i].setDelayTime(clientWaitParetoTime())
        else:
            print "Ready for burst-start"
            
        for i in range(parameters.threadNum):
            thrList[i].start()
            
    while (threading.activeCount() > 1):
        pass
    
    for i in range(parameters.threadNum):
        outputs[i+1] = thrList[i].stats

    aux.saveStatistics("user_simulation.csv",outputs)
     
    print "******* USER SIMULATION TEST COMPLETED *******"
    tStop = time.time()
    print "Total User Simulation Test Time: %f"%(tStop-tStart)
    # dealloc
    del outputs
    del thrList
    