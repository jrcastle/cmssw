import math 
import re
import optparse 
import commands 
import os 
import sys 
import time 
import datetime

###########################################################################################
def timeoutManager(type,timeout=-1,fileName=".timeout"):
    if timeout == 0:
        return 1
    timeFormat = "%a,%Y/%m/%d,%H:%M:%S"
    currentTime = time.gmtime()
    timeoutLine = type + ' ' + time.strftime(timeFormat, currentTime) + '\n'
    isTimeout = False
    alreadyThere = False
    timeoutType = -1;
    fileExist = os.path.isfile(fileName)
    text = ''
    fields = []
    reset = False
    if timeout == -1:
        reset = True
    if fileExist:
        file = open(fileName)
        for line in file:
            text += line
            fields = line.strip('\n').split(' ')
            if fields[0] == type:
                alreadyThere = True
                if reset:
                    text = text.replace(line,'')
                    continue

                fileTime = time.strptime(fields[1],timeFormat)
                myTime = time.mktime(fileTime)
                referenceTime = time.mktime(time.gmtime())
                daylight = 0
                if currentTime.tm_isdst == 0:
                    daylight = 3600
                elapsedTime = referenceTime-myTime-daylight
                if elapsedTime > timeout:
                    isTimeout = True
                    timeoutType = 1
                    print "Timeout! " + str(elapsedTime) + " seconds passed since the " + type + " timeout was set and you can't tolerate more than " + str(timeout) + " seconds!"
                else:
                    timeoutType = 0
                    print "Timeout of type " + type + " already exist and was generated " + str(elapsedTime) + " seconds ago at " + fields[1]

        file.close()

    if not fileExist or not alreadyThere and not reset:
        timeoutType = -1
        text += timeoutLine

    if not fileExist or not alreadyThere or isTimeout or (reset and alreadyThere):
        if fileExist:
            commands.getstatusoutput("rm -rf " + fileName)
        file = open(fileName,'w')
        file.write(text)
        file.close()

    return timeoutType
