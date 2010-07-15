#!/usr/bin/python

import os
import os.path
import sys
import getopt
import time
from numpy import *
from scipy import *
from subprocess import Popen, PIPE, STDOUT
#from asyncproc import Process

myDir = sys.argv[1]
numNodes = int(sys.argv[2]) 
isDataLine = 0
dataLine = []
allReadTimes = []
allReadAmounts = []
flowTputs = []
allTputs = []
totTime = 0

os.system("rm %s/tput%d.data" % (myDir,numNodes))

def printStats(timeElapsed, winSize, winAmount):
    global flowTputs
    tput = 0
    if (timeElapsed == 0):
        tput=0
        if (len(flowTputs) > 0):
            allTputs.append(flowTputs)
            #print flowTputs
            flowTputs = []
    else:
        tput=winAmount/winSize
    flowTputs.append(tput)
    #print "%d %d" % (timeElapsed, tput)
    #outcommand = "echo \"%d %d\" >> tput.data" % (timeElapsed, tput)
    #os.system(outcommand)
    #print "timeElapsed: %d  winSize: %d  winAmount: %d  tput: %d" % (timeElapsed, winSize, winAmount, tput)

for i in range(numNodes):
    for j in range(i+1,numNodes):
        for k in ["c","s"]:
            readTimes = []
            readAmounts = []
            print "%d,%d,%s" % (i,j,k)
            command = "cat %s/%d.%d.%s" % (myDir,i,j,k)
            cmd = os.popen(command)
            cmdout = cmd.readlines()
            for line in cmdout:
                if (line.find("ReadTime ReadAmnt") != -1):
                    isDataLine = 1;
                elif (line.find("Total number of reads") != -1):
                    isDataLine = 0;
                elif (isDataLine == 1):
                    dataLine = line.strip().split();
                    try:
                        time = dataLine[0]
                        amnt = dataLine[1]
                        readTimes.append(int(time))
                        readAmounts.append(int(amnt))
                    except:
                        print dataLine
            allReadTimes.append(readTimes)
            allReadAmounts.append(readAmounts)

for i in range(0,(numNodes*(numNodes-1))):
    totElements = len(allReadTimes[i])
    print "TotElements: " + str(totElements)
    winLimit = 100000
    stepSize = 1000
    winTime = 0
    #winStartTime is taken from start of TIMER of last index included in window
    #so that winEndTime - winStartTime is the timed length of the elements included
    #in the window and can vary from the winLimit
    winStartTime = 0 
    winEndTime = 0
    winAmount = 0
    totAmount = 0
    stepTime = 0
    minWinIndex = -1 #the last index just before the window
    maxWinIndex = 0 #the first index just after the window

    #First step: grow window to window limit
    while (stepTime <= winLimit):
        while ((maxWinIndex < totElements) and
               stepTime >= winEndTime + allReadTimes[i][maxWinIndex]):
            winAmount += allReadAmounts[i][maxWinIndex]
            totAmount += allReadAmounts[i][maxWinIndex]
            winEndTime += allReadTimes[i][maxWinIndex]
            if (allReadTimes[i][maxWinIndex] > 200000):
                print "TIMEOUT at %d (finally read %d B in last %d us)" % (stepTime,allReadAmounts[i][maxWinIndex],allReadTimes[i][maxWinIndex])
            maxWinIndex += 1
        #print "step1: stepTime %d minWinIndex %d maxWinIndex %d winStartTime %d winEndTime %d" % (stepTime,minWinIndex,maxWinIndex,winStartTime,winEndTime)
        printStats(stepTime, winTime, winAmount)
        winTime += stepSize
        stepTime += stepSize

    #wintime now fixed at limit, so manage the sliding window
    winTime = winLimit
    winStartTime = 0 #allReadTimes[i][0]
    while (maxWinIndex < totElements):
        #or winAmount > 0):
        #fix max elements of window
        while ((maxWinIndex < totElements) and
               stepTime >= winEndTime + allReadTimes[i][maxWinIndex]):
            winAmount += allReadAmounts[i][maxWinIndex]
            totAmount += allReadAmounts[i][maxWinIndex]
            winEndTime += allReadTimes[i][maxWinIndex]
            if (allReadTimes[i][maxWinIndex] > 200000):
                print "TIMEOUT at %d (finally read %d B in last %d us)" % (stepTime,allReadAmounts[i][maxWinIndex],allReadTimes[i][maxWinIndex])
            maxWinIndex += 1
        #fix min elements of window
        while ((minWinIndex+1 < totElements) and 
               winStartTime + allReadTimes[i][minWinIndex+1] <= (stepTime - winTime)):
            minWinIndex += 1
            winAmount -= allReadAmounts[i][minWinIndex]
            winStartTime += allReadTimes[i][minWinIndex]
        #print "step2: stepTime %d minWinIndex %d maxWinIndex %d winStartTime %d winEndTime %d" % (stepTime,minWinIndex,maxWinIndex,winStartTime,winEndTime)
        printStats(stepTime, winLimit, winAmount)
        #print (totAmount / stepTime)
        stepTime += stepSize

allTputs.append(flowTputs)

maxFlowSize = 0
#find max length of a flow
for i in range(0,len(allTputs)):
    if (len(allTputs[i]) > maxFlowSize):
        maxFlowSize = len(allTputs[i])

#append zeros to make a full array
for i in range(0,len(allTputs)):
    neededZeros = maxFlowSize - len(allTputs[i])
    for j in range(0,neededZeros):
        allTputs[i].append(0)

allTputsArray = array(allTputs)
tputfile = "%s/tput%d.data" % (myDir,numNodes)
print tputfile
savetxt(tputfile,allTputsArray.T,fmt='%d')

command = "cat %s/%d.out" % (myDir,numNodes)
cmd = os.popen(command)
cmdout = cmd.readlines()
for line in cmdout:
    if (line.find("Total received at nodeIndex 0:") != -1):
        totDataReceivedPerNode = int(line.strip().split()[5])
        print "Total received per node: %d B" % (totDataReceivedPerNode)
    elif (line.find("Full job finished:") != -1):
        totTime = int(line.strip().split()[3])
        print "Total job time: %d us" % (totTime)

print "Average Throughput: %f" % (float(totDataReceivedPerNode)/float(totTime))
