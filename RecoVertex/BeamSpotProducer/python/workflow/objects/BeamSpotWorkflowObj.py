import commands
import os
import re
import sys

import RecoVertex.BeamSpotProducer.workflow.utils.colorer

from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj import BeamSpot
from RecoVertex.BeamSpotProducer.workflow.utils.CommonMethods import ls, readBeamSpotFile, sortAndCleanBeamList, setLockName
from RecoVertex.BeamSpotProducer.workflow.utils.CommonMethods import timeoutManager, createWeightedPayloads, checkLock, lock

from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import error_crab, error_lumi_range, error_run_not_in_DBS, error_timeout
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import error_out_of_tolerance, error_run_not_in_rr, error_missing_large_run
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import error_source_dir, error_failed_copy, error_failed_copy_dirs, error_sql_write_failed 
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import error_iov_not_implemented, error_iov_unrecognised, error_tag_exist_last_iov_doesnt, error_cant_connect_db
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import warning_missing_small_run, warning_no_valid_fit, warning_unable_to_create_payload 
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import warning_setting_dbs_mismatch_timeout, warning_dbs_mismatch_timeout_progress

# from RecoVertex.BeamSpotProducer.workflow.utils.initCrab      import initCrab
from RecoVertex.BeamSpotProducer.workflow.utils.initLogger    import initLogger
from RecoVertex.BeamSpotProducer.workflow.utils.smartCopy     import cyclicCp
from RecoVertex.BeamSpotProducer.workflow.utils.setupDbsApi   import setupDbsApi
from RecoVertex.BeamSpotProducer.workflow.utils.locker        import Locker

# RIC cannot make it work, as the environment variables set in 
# a subprocess don't outlive the subprocess itself
# initCrab() 

if sys.version_info < (2,6,0):
    print 'Python interpreter version < 2.6.0'
    print 'importing json'
    import json
else:
    print 'Python interpreter version >= 2.6.0'
    print 'importing simplejson as json'
    import simplejson as json


class BeamSpotWorkflow(object):
    '''
    FIXME! docstring to be written
    '''

    def __init__(self                 ,
                 sourceDir            ,
                 archiveDir           ,
                 workingDir           ,
                 databaseTag          ,
                 dataSet              ,
                 fileIOVBase          ,
                 dbIOVBase            ,
                 dbsTolerance         ,
                 dbsTolerancePercent  ,
                 rrTolerance          ,
                 missingFilesTolerance,
                 missingLumisTimeout  ,
                 jsonFileName         ,
                 mailList             ,
                 payloadFileName      ):

                
        self.logger = initLogger(filename          = 'beamspot_workflow.log', 
                                 mode              = 'w+',
                                 formatter         = '[%(asctime)s] [%(levelname)-8s] '  \
                                                     '[%(funcName)-33s : L%(lineno)4d]: '\
                                                     '%(message)s ',
                                 formatter_options = '%Y-%m-%d %H:%M:%S',
                                 emails            = mailList,
                                 file_level        = 'info',
                                 stream_level      = 'info',
                                 email_level       = 'critical')
        
        self.logger.info('Initialising a BeamSpotWorkflow class')

        self.api = setupDbsApi(logger = self.logger)

        self.sourceDir             = sourceDir
        self.archiveDir            = archiveDir
        self.workingDir            = workingDir
        self.databaseTag           = databaseTag
        self.dataSet               = dataSet
        self.fileIOVBase           = fileIOVBase
        self.dbIOVBase             = dbIOVBase
        self.dbsTolerance          = dbsTolerance
        self.dbsTolerancePercent   = dbsTolerancePercent
        self.rrTolerance           = rrTolerance
        self.missingFilesTolerance = missingFilesTolerance
        self.missingLumisTimeout   = missingLumisTimeout
        self.jsonFileName          = jsonFileName
        self.mailList              = mailList
        self.payloadFileName       = payloadFileName
        
        self._checkIfAlreadyRunning()
        self._setupDirectories()


    def _checkIfAlreadyRunning(self):
        '''
        Check whether there's another BeamSpotWorkflow
        running
        '''
        self.logger.info('Check whether another script is running')        
        
        self.locker = Locker('locker.lock')
        
        if self.locker.checkLock():
            self.logger.error('There is already a megascript runnning... exiting')
            exit()
        else:
            self.locker.lock()

    def _setupDirectories(self):
        '''
        Directory setup
        FIX THE DOC
        '''
        self.logger.info('Setting up the directory structure')        
        # check that the relevant directories end with a '/'
        for dir in (self.sourceDir, self.archiveDir, self.workingDir):
            if not dir.endswith('/'):
                dir += '/'

        # check that sourceDir exists
        if not ( os.path.isdir(self.sourceDir) and
                 os.path.exists(self.sourceDir) ) :
            self.logger.error(error_source_dir(self.sourceDir))

        # create archiveDir
        if not os.path.isdir(self.archiveDir):
            os.mkdir(self.archiveDir)

        # create workingDir
        if not os.path.isdir(self.workingDir):
            os.mkdir(self.workingDir)
        else:
            os.system('rm -f {WORKDIR}*'.format(WORKDIR = self.workingDir))

    def getLastUploadedIOV(self, tagName):
        '''
        This function gets the last uploaded IOV from the conditional DB
        to make sure we are not re-running on already considered runs
        '''
        self.logger.info('Getting last IOV for tag: ' + self.databaseTag)
        listIOVCommand = 'conddb --nocolor list {TAGNAME} -L 2000000000000'.format(TAGNAME = self.tagName)
        dbError = commands.getstatusoutput( listIOVCommand )
        if dbError[0] != 0 :
            if 'metadata entry {TAGNAME} does not exist'.format(TAGNAME= self.tagName) in dbError[1]:
                self.logger.warning('Creating a new tag because I got the following '\
                                    'error contacting the DB \n%s' %dbError[1])
                return 1
            else:
                self.logger.error(error_cant_connect_db(dbError[1]))

        aCommand = listIOVCommand+' | grep Beam | tail -1 | awk \'{print $1}\''
        output = commands.getstatusoutput( aCommand )
        #WARNING when we pass to lumi IOV this should be long long
        if output[1] == '':
          self.logger.error(error_tag_exist_last_iov_doesnt(tagName, emails = []))
        goodoutput =  output[1].split('\n')[1]
        self.logger.info('Last IOV from DB = %d' %int(goodoutput))
        return long(goodoutput)

    def getRunNumberFromFileName(self, fileName):
        regExp = re.search('(\D+)(\d+)_(\d+)_[a-zA-Z0-9]+.txt',fileName)
        if not regExp:
            return -1
        return long(regExp.group(3))

    def getListOfRunsAndLumiFromFile(self, firstRun = -1, fileName = ''):
        
        self.logger.info('Getting list of runs and lumis from Json %s' %fileName)

        file = open(fileName)
        jsonFile = file.read()
        file.close()
        jsonList = json.loads(jsonFile)

        selected_dcs = {}
        for element in jsonList:
            selected_dcs[long(element)] = jsonList[element]
        return selected_dcs

    def getNewFileList(self, fromDir, lastUploadedIOV):
        '''This function takes the list of files already processed and checks if
           the run number of those files is greater than the last uploaded IOV
        '''
        self.logger.info('Getting list of files processed after IOV %s:'  %str(lastUploadedIOV))
        newRunList = []
        listOfFiles = ls(fromDir,'.txt')
        runFileMap = {}
        for fileName in listOfFiles:
            runNumber = self.getRunNumberFromFileName(fileName)
            self.logger.info('\t'+str(fileName)+' run: '+str(runNumber))
            if runNumber > lastUploadedIOV:
                newRunList.append(fileName)

        if len(newRunList) == 0:
            self.logger.error('There are no new runs after %s' %str(lastUploadedIOV))
            sys.exit()

        return newRunList

    def getListOfRunsAndLumiFromDBS(self, dataSet, lastRun=-1):
        '''
        FIXME!
        '''
        datasetList    = dataSet.split(',')
        outputFileList = []
        runsAndLumis   = {}
        self.logger.info('Getting list of runs and lumis from DBS')
        for data in datasetList:
            self.logger.info('Getting list of files from DBS for the %s dataset. From this list we will query for the run and lumisections' %str(data))
            output_files = self.api.listFiles(dataset=str(data))
            for myfile in output_files:
                output = self.api.listFileLumis(logical_file_name=str(myfile['logical_file_name']))
                run = long(output[0]['run_num'])
                if run < lastRun: continue
                if not run in runsAndLumis:
                    runsAndLumis[run] = []
                for lumi in output[0]['lumi_section_num']:
                    runsAndLumis[run].append(long(lumi))
        if len(runsAndLumis) == 0:
           self.logger.error('There are no new runs or lumis in DBS to process, exiting.')
           exit()
        return runsAndLumis

    def getNumberOfFilesToProcessForRun(self, dataSet, run):
        queryCommand = "dbs --search --query \"find file where dataset=" + dataSet + " and run = " + str(run) + "\" | grep .root"
        output = commands.getstatusoutput( queryCommand )
        if output[0] != 0:
            return 0
        else:
            return len(output[1].split('\n'))

    def compareLumiLists(self, listA, listB, errors=[], tolerance=0):
        lenA = len(listA)
        lenB = len(listB)
        if lenA < lenB-(lenB*float(tolerance)/100):
            errors.append('ERROR: The number of lumi sections is different: listA(' + str(lenA) + ')!=(' + str(lenB) + ')listB')
        #else:
            #errors.append('Lumi check ok!listA(' + str(lenA) + ')-(' + str(lenB) + ')listB')
        #print errors
        listA.sort()
        listB.sort()
        badA = []
        badB = []
        for lumi in listA:
            if not lumi in listB:
                errors.append('Lumi (' + str(lumi) + ') is in listA but not in listB')
                badB.append(lumi)
        for lumi in listB:
            if not lumi in listA:
                errors.append('Lumi (' + str(lumi) + ') is in listB but not in listA')
                badA.append(lumi)

        return badA, badB

    def selectFilesToProcess(self                    ,
                             listOfRunsAndLumiFromDBS,
                             listOfRunsAndLumiFromRR ,
                             newRunList              ,
                             runListDir              ):

        runsAndLumisProcessed = {}
        runsAndFiles          = {}
        for fileName in newRunList:
            file = open(runListDir+fileName)
        for line in file:
            if 'Runnumber' in line:
                run = long(line.rstrip().split()[1])
            elif 'LumiRange' in line:
                lumiLine = line.rstrip().split()
                begLumi  = long(lumiLine[1])
                endLumi  = long(lumiLine[3])
                if begLumi != endLumi:
                    self.logger.error(error_lumi_range(run, line, runListDir, fileName))
                else:
                    if not run in runsAndLumisProcessed:
                        runsAndLumisProcessed[run] = []
                    if begLumi in runsAndLumisProcessed[run]:
                        self.logger.error('Lumi {BEGLUMI} in event {RUN} already exist. '\
                                          'This MUST not happen but right now           '\
                                          'I will ignore this lumi!'.format(BEGLUMI = str(begLumi),
                                                                            RUN     = str(run)    ))
                    else:
                        runsAndLumisProcessed[run].append(begLumi)
        if not run in runsAndFiles:
            runsAndFiles[run] = []
        runsAndFiles[run].append(fileName)
        file.close()

        rrKeys = listOfRunsAndLumiFromRR.keys()
        rrKeys.sort()
        dbsKeys = listOfRunsAndLumiFromDBS.keys()
        dbsKeys.sort();
        #I remove the last entry from DBS since I am not sure it is an already closed run!
        lastUnclosedRun = dbsKeys.pop()
        self.logger.info('Last unclosed run: ' + str(lastUnclosedRun))
        procKeys = runsAndLumisProcessed.keys()
        procKeys.sort()
        filesToProcess = []
        for run in rrKeys:
            RRList = []
            for lumiRange in listOfRunsAndLumiFromRR[run]:
                if lumiRange != []:
                    for l in range(lumiRange[0],lumiRange[1]+1):
                        RRList.append(long(l))
            if run in procKeys and run <= lastUnclosedRun:
                #print 'run ' + str(run) + ' is in procKeys'
                if not run in dbsKeys and run != lastUnclosedRun:
                    self.logger.error(error_run_not_in_DBS(run))
                nFiles = 0
                for data in self.dataSet.split(','):
                    nFiles = self.getNumberOfFilesToProcessForRun(data,run)
                    if nFiles != 0:
                        break
                if len(runsAndFiles[run]) < nFiles:
                    #print runsAndFiles[run]
                    self.logger.warning('I haven\'t processed all files yet : ' + str(len(runsAndFiles[run])) + ' out of ' + str(nFiles) + ' for run: ' + str(run))
                    if nFiles - len(runsAndFiles[run]) <= missingFilesTolerance:
                        timeoutManager('DBS_VERY_BIG_MISMATCH_Run'+str(run)) # resetting this timeout
                        timeoutType = timeoutManager('DBS_MISMATCH_Run'+str(run), self.missingLumisTimeout)
                        if timeoutType == 1:
                            self.logger.error(error_timeout())
                        else:
                            if timeoutType == -1:
                                warning_setting_dbs_mismatch_timeout(run)
                            else:
                                warning_dbs_mismatch_timeout_progress(run)
                            return filesToProcess
                    else:
                        timeoutType = timeoutManager('DBS_VERY_BIG_MISMATCH_Run'+str(run), self.missingLumisTimeout)
                        if timeoutType == 1:
                            self.logger.error(error_timeout(runsAndFiles, self.missingLumisTimeout, run))
                            return filesToProcess
                        else:
                            if timeoutType == -1:
                                self.logger.warning('Setting the DBS_VERY_BIG_MISMATCH_Run' + str(run) + ' timeout because I haven\'t processed all files!')
                            else:
                                self.logger.warning('Timeout DBS_VERY_BIG_MISMATCH_Run' + str(run) + ' is in progress.')
                            return filesToProcess

                else:
                    timeoutManager('DBS_VERY_BIG_MISMATCH_Run'+str(run))
                    timeoutManager('DBS_MISMATCH_Run'+str(run))
                    #print 'I have processed ' + str(len(runsAndFiles[run])) + ' out of ' + str(nFiles) + ' files that are in DBS. So I should have all the lumis!'
                errors          = []
                badProcessed    = []
                badDBSProcessed = []
                badDBS          = []
                badRRProcessed  = []
                badRR           = []
                #It is important for runsAndLumisProcessed[run] to be the first because the comparision is not ==
                badDBSProcessed, badDBS = self.compareLumiLists(runsAndLumisProcessed[run],listOfRunsAndLumiFromDBS[run],errors)
                for i in range(0,len(errors)):
                    errors[i] = errors[i].replace('listA','the processed lumis')
                    errors[i] = errors[i].replace('listB','DBS')
                if len(badDBS) != 0:
                    self.logger.warning('This is weird because I processed more lumis than the ones that are in DBS!')
                if len(badDBSProcessed) != 0 and run in rrKeys:
                    lastError = len(errors)
                    #It is important for runsAndLumisProcessed[run] to be the first because the comparision is not ==
                    badRRProcessed, badRR = self.compareLumiLists(runsAndLumisProcessed[run],RRList,errors)
                    for i in range(0,len(errors)):
                        errors[i] = errors[i].replace('listA','the processed lumis')
                        errors[i] = errors[i].replace('listB','Run Registry')
                    if len(badRRProcessed) != 0:
                        for lumi in badDBSProcessed:
                            if lumi in badRRProcessed:
                                badProcessed.append(lumi)
                        lenA = len(badProcessed)
                        lenB = len(RRList)
                        if lenA > 0.:
                            self.logger.warning('I have not processed some of the lumis that are in the run registry for run: ' + str(run))
                            self.logger.warning('badRRProcessed size is ' + str(len(badRRProcessed)) + ' and badDBSProcessed size is ' + str(len(badDBSProcessed)))
                            if 100.*lenA/lenB <= self.dbsTolerancePercent and lenA > 0.:
                                self.logger.warning('I didn\'t process ' + str(100.*lenA/lenB) + \
                                                    '% of the lumis but I am within the ' + str(self.dbsTolerancePercent) + \
                                                    '% set in the configuration. Which corrispond to ' + str(lenA) + \
                                                    ' out of ' + str(lenB) + ' lumis')
                                badProcessedString = ''
                                for item in badProcessed:
                                    badProcessedString += str(item) + ' '
                                
                                self.logger.warning('badProcessed:\n ' + badProcessedString)
                                badProcessed = []
                            elif lenA <= self.dbsTolerance:
                                self.logger.warning('I didn\'t process ' + str(lenA) + \
                                                    ' lumis but I am within the ' + str(self.dbsTolerance) + \
                                                    ' lumis set in the configuration. Which corrispond to ' \
                                                    + str(lenA) + ' out of ' + str(lenB) + ' lumis')
                                badProcessedString = ''
                                for item in badProcessed:
                                    badProcessedString += str(item) + ' '
                                
                                self.logger.warning('badProcessed:\n ' + badProcessedString)
                                badProcessed = []
                            else:
                                self.logger.error(error_out_of_tolerance(run, lenA, lenB,
                                                                         self.dbsTolerancePercent,
                                                                         self.dbsTolerance))
                                return filesToProcess
                    elif len(errors) != 0:
                        self.logger.warning('The number of lumi sections processed didn\'t match the one in DBS but they cover all the ones in the Run Registry, so it is ok!')

                #If I get here it means that I passed or the DBS or the RR test
                if len(badProcessed) == 0:
                    for file in runsAndFiles[run]:
                        filesToProcess.append(file)
                else:
                    self.logger.error('This should never happen because if I '\
                                      'have errors I return or exit! Run: {RUN}'.format(RUN = str(run)))
            else:
                self.logger.error = ('Run ' + str(run) + ' is in the run registry but it has not been processed yet!')
                timeoutType = timeoutManager('MISSING_RUNREGRUN_Run'+str(run), self.missingLumisTimeout)
                if timeoutType == 1:
                    if len(RRList) <= self.rrTolerance:
                        self.logger.warning(warning_missing_small_run(run, RRList, self.rrTolerance))
                    else:
                        self.logger.error(error_missing_large_run(run, RRList, self.rrTolerance))
                        return filesToProcess
                else:
                    if timeoutType == -1:
                        self.logger.warning('Setting the MISSING_RUNREGRUN_Run' + str(run) + ' timeout because I haven\'t processed a run!')
                    else:
                        self.logger.warning('Timeout MISSING_RUNREGRUN_Run' + str(run) + ' is in progress.')
                    return filesToProcess

        return filesToProcess
        
    def process(self):
        
        '''
        FIXME!
        '''
        
        self.logger.info('Begin process')

        ######### Check the last IOV from querying the DBS
        #FIXME: That's an hack to make it work with the example files
        #       by Kevin, need to remove the following line in normal operations
        try:
            lastUploadedIOV = self.getLastUploadedIOV(self.databaseTag)
        except:
            self.logger.warning('This is an hack to make it work with the example files by Kevin,'\
                                ' lastUploadedIOV set by hand to 194000')
            lastUploadedIOV = 194000

        ######### Get list of files processed after the last IOV
        newProcessedFileList = self.getNewFileList(self.sourceDir, lastUploadedIOV)

        ######### Copy files to archive directory
        copiedFiles = cyclicCp(self.sourceDir, self.archiveDir, 
                               newProcessedFileList, logger = self.logger)
        
        ######### Get from DBS the list of Runs and lumis last IOV
        listOfRunsAndLumiFromDBS  = self.getListOfRunsAndLumiFromDBS(self.dataSet, lastUploadedIOV)
        listOfRunsAndLumiFromJson = self.getListOfRunsAndLumiFromFile(lastUploadedIOV, self.jsonFileName)

        ######### Get list of files to process for DB
        self.logger.info('Getting list of files to process')
        selectedFilesToProcess = self.selectFilesToProcess(listOfRunsAndLumiFromDBS ,
                                                           listOfRunsAndLumiFromJson,
                                                           copiedFiles              ,
                                                           self.archiveDir          )
        if len(selectedFilesToProcess) == 0:
           exit('There are no files to process')

        ######### Copy files to working directory
        copiedFiles = cyclicCp(self.archiveDir, self.workingDir, 
                               selectedFilesToProcess, logger = self.logger)

        self.logger.info('Sorting and cleaning beamlist')
        beamSpotObjList = []
        for fileName in copiedFiles:
            readBeamSpotFile(self.workingDir + fileName,
                             beamSpotObjList           ,
                             self.fileIOVBase)

        sortAndCleanBeamList(beamSpotObjList, self.fileIOVBase)

        if len(beamSpotObjList) == 0:
            warning_no_valid_fit()


        runBased = False
        if self.dbIOVBase == 'runnumber':
            runBased = True

        payloadList = createWeightedPayloads(self.workingDir + self.payloadFileName,
                                             beamSpotObjList, runBased)
        if len(payloadList) == 0:
            self.logger.warning(warning_unable_to_create_payload())
        
        
        if hasattr(self, 'locker'):
            self.locker.unlock()
        

if __name__ == '__main__':

    sourceDir             = './Runs2012B_FULL/Results/'
    archiveDir            = './Runs2012B_FULL/Archive/'
    workingDir            = './Runs2012B_FULL/Working/'
    jsonFileName          = 'beamspot_payload_2012BONLY_merged_JSON_all.txt'
    databaseTag           = 'BeamSpotObjects_2009_LumiBased_SigmaZ_v29_offline'
    dataSet               = '/StreamExpress/Run2012B-TkAlMinBias-v1/ALCARECO'
    fileIOVBase           = 'lumibase'
    dbIOVBase             = 'lumiid'
    dbsTolerance          = 200000
    dbsTolerancePercent   = 5.
    rrTolerance           = 200000
    missingFilesTolerance = 200000
    missingLumisTimeout   = 0
    mailList              = 'manzoni@cern.ch'
    payloadFileName       = 'PayloadFile.txt'

    bswf = BeamSpotWorkflow(
                             sourceDir             = sourceDir            ,
                             archiveDir            = archiveDir           ,
                             workingDir            = workingDir           ,
                             databaseTag           = databaseTag          ,
                             dataSet               = dataSet              ,
                             fileIOVBase           = fileIOVBase          ,
                             dbIOVBase             = dbIOVBase            ,
                             dbsTolerance          = dbsTolerance         ,
                             dbsTolerancePercent   = dbsTolerancePercent  ,
                             rrTolerance           = rrTolerance          ,
                             missingFilesTolerance = missingFilesTolerance,
                             missingLumisTimeout   = missingLumisTimeout  ,
                             jsonFileName          = jsonFileName         ,
                             mailList              = mailList             ,
                             payloadFileName       = payloadFileName      ,
                           )

    bswf.process()
