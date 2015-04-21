import commands
import glob
import os
import re
import sys

import RecoVertex.BeamSpotProducer.workflow.utils.colorer

from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj    import BeamSpot
from RecoVertex.BeamSpotProducer.workflow.utils.CommonMethods    import ls, readBeamSpotFile, sortAndCleanBeamList
from RecoVertex.BeamSpotProducer.workflow.utils.CommonMethods    import timeoutManager, createWeightedPayloads 

from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages    import *
from RecoVertex.BeamSpotProducer.workflow.utils.setupDbsApi      import setupDbsApi
from RecoVertex.BeamSpotProducer.workflow.utils.initLogger       import initLogger
from RecoVertex.BeamSpotProducer.workflow.utils.smartCopy        import cyclicCp
from RecoVertex.BeamSpotProducer.workflow.utils.locker           import Locker
from RecoVertex.BeamSpotProducer.workflow.utils.readJson         import readJson
from RecoVertex.BeamSpotProducer.workflow.utils.dbsCommands      import getListOfRunsAndLumiFromDBS, getNumberOfFilesToProcessForRun
from RecoVertex.BeamSpotProducer.workflow.utils.condDbCommands   import getLastUploadedIOV
from RecoVertex.BeamSpotProducer.workflow.utils.compareLumiLists import compareLumiLists
    
class BeamSpotWorkflow(object):
    '''
    FIXME! docstring to be written
    '''

    def __init__(self, cfg, lock, overwrite):
                
        self.logger = initLogger(filename          = 'beamspot_workflow.log', 
                                 mode              = 'w+'                   ,
                                 formatter         = '[%(asctime)s] '     \
                                                     '[%(levelname)-8s] ' \
                                                     '[%(funcName)-33s : '\
                                                     'L%(lineno)4d]: '    \
                                                     '%(message)s '         ,
                                 formatter_options = '%Y-%m-%d %H:%M:%S'    ,
                                 emails            = cfg.mailList           ,
                                 file_level        = 'info'                 ,
                                 stream_level      = 'debug'                 ,
                                 email_level       = 'critical'             )
        
        self.logger.info('Initialising a BeamSpotWorkflow class')
        
        self.api = setupDbsApi(logger = self.logger)

        self.sourceDir             = cfg.sourceDir
        self.archiveDir            = cfg.archiveDir
        self.workingDir            = cfg.workingDir
        self.databaseTag           = cfg.databaseTag
        self.dataSet               = cfg.dataSet
        self.fileIOVBase           = cfg.fileIOVBase
        self.dbIOVBase             = cfg.dbIOVBase
        self.dbsTolerance          = cfg.dbsTolerance
        self.dbsTolerancePercent   = cfg.dbsTolerancePercent
        self.rrTolerance           = cfg.rrTolerance
        self.missingFilesTolerance = cfg.missingFilesTolerance
        self.missingLumisTimeout   = cfg.missingLumisTimeout
        self.jsonFileName          = cfg.jsonFileName
        self.mailList              = cfg.mailList
        self.payloadFileName       = cfg.payloadFileName
        
        if lock: 
            self._checkIfAlreadyRunning()
        
        self._setupDirectories()

    def _checkIfAlreadyRunning(self):
        '''
        Check whether there's another BeamSpotWorkflow
        running, by assessing the existence of locker.lock file
        '''
        self.logger.info('Check whether another script is running')        
        
        self.locker = Locker('locker.lock')
        
        if self.locker.checkLock():
            self.logger.error('There is already a megascript '\
                              'runnning... exiting')
            exit()
        else:
            self.locker.lock()

    def _setupDirectories(self):
        '''
        Directory setup
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

    def _associateRunsLumisAndFiles(self, newRunList, runListDir):
        '''
        Parses the txt files produced by the crab jobs and returns a dictionary
        where each run (inferred from the file names) is associated to its 
        lumi sections that got processed and the absolute path to its files.
        '''
                
        runs = set([re.findall(r'\d+', fileName)[1] for fileName in newRunList]) 

        runsFilesLumis = {run:{'files':[], 'lumis':[]} for run in runs}
        
        for run in runs:
            
            filesForRun = glob.glob('/'.join([runListDir, '*' + run + '*.txt']))
            runsFilesLumis[run]['files'] = filesForRun
            
            for fileName in filesForRun:
                
                file = open(fileName) 
                
                for line in file:
                    if 'LumiRange' in line:
                        lumiLine = line.rstrip().split()
                        begLumi  = lumiLine[1]
                        endLumi  = lumiLine[3]
                        if begLumi != endLumi:
                            self.logger.error(error_lumi_range(run       , 
                                                               line      , 
                                                               runListDir, 
                                                               fileName  ))
                        else:
                            if begLumi not in runsFilesLumis[run]['lumis']:
                                runsFilesLumis[run]['lumis'].append(begLumi)                                        
                            else:
                                self.logger.error(error_lumi_in_run(begLumi, 
                                                                    run    ))
                file.close()
        
        return runsFilesLumis

    def _checkProcessedFiles(self, run, runsAndFiles, nFiles, filesToProcess):
    
        if len(runsAndFiles[run]) < nFiles:
    
            self.logger.warning(warning_not_all_file_yet(run         , 
                                                         runsAndFiles, 
                                                         nFiles      ))
    
            if nFiles - len(runsAndFiles[run]) <= self.missingFilesTolerance:
                timeoutManager('DBS_VERY_BIG_MISMATCH_Run'+str(run)) # resetting this timeout
                timeoutType = timeoutManager('DBS_MISMATCH_Run'+str(run), self.missingLumisTimeout)
                if timeoutType == 1:
                    self.logger.error(error_timeout())
                else:
                    if timeoutType == -1: self.logger.warning(warning_setting_dbs_mismatch_timeout(run) )
                    else                : self.logger.warning(warning_dbs_mismatch_timeout_progress(run))
                    return filesToProcess
            else:
                timeoutType = timeoutManager('DBS_VERY_BIG_MISMATCH_Run'+str(run), self.missingLumisTimeout)
                if timeoutType == 1:
                    self.logger.error(error_timeout(runsAndFiles, self.missingLumisTimeout, run))
                    return filesToProcess
                else:
                    if timeoutType == -1: self.logger.warning(warning_dbs_mismatch_setting_timeout(run))
                    else                : self.logger.warning(warning_dbs_mismatch_timeout(run)        )
                    return filesToProcess
        else:
            timeoutManager('DBS_VERY_BIG_MISMATCH_Run'+ str(run))
            timeoutManager('DBS_MISMATCH_Run'         + str(run))

        return filesToProcess
       
    def getRunNumberFromFileName(self, fileName):
        regExp = re.search('(\D+)(\d+)_(\d+)_[a-zA-Z0-9]+.txt',fileName)
        if not regExp:
            return -1
        return long(regExp.group(3))

    def getNewFileList(self, fromDir, lastUploadedIOV):
        '''This function takes the list of files already processed and checks if
           the run number of those files is greater than the last uploaded IOV
        '''
        self.logger.info('Getting list of files processed '\
                         'after IOV %s:'  %str(lastUploadedIOV))
        newRunList = []
        listOfFiles = ls(fromDir,'.txt')
        runFileMap = {}
        for fileName in listOfFiles:
            runNumber = self.getRunNumberFromFileName(fileName)
            self.logger.info('\t'+str(fileName)+' run: '+str(runNumber))
            if runNumber > lastUploadedIOV:
                newRunList.append(fileName)

        if len(newRunList) == 0:
            self.logger.error('There are no new runs '\
                              'after %s' %str(lastUploadedIOV))
            sys.exit()

        return newRunList

    def selectFilesToProcess(self                    ,
                             listOfRunsAndLumiFromDBS,
                             listOfRunsAndLumiFromRR ,
                             newRunList              ,
                             runListDir              ):
        '''
        FIXME!
        Need some decent doc here! 
        '''

        runsFilesLumis = self._associateRunsLumisAndFiles(newRunList, runListDir)

        runsAndFiles          = {k : v['files'] for k, v in runsFilesLumis.items()}
        runsAndLumisProcessed = {k : v['lumis'] for k, v in runsFilesLumis.items()}
        
        self.logger.info('test') ; import pdb ; pdb.set_trace()

        # list of runs in RunRegistry
        rrKeys   = sorted(listOfRunsAndLumiFromRR .keys())
        # list of runs in DBS for the given DS 
        dbsKeys  = sorted(listOfRunsAndLumiFromDBS.keys())
        # list of runs on which the offline BS reconstruction has been run 
        procKeys = sorted(runsFilesLumis          .keys())

        self.logger.info('test') ; import pdb ; pdb.set_trace()
        
        # I remove the last entry from DBS since 
        # I am not sure it is an already closed run!
        lastUnclosedRun = max(dbsKeys)
        self.logger.info('Last unclosed run: %s' %str(lastUnclosedRun))

        filesToProcess = []
        self.logger.info('test') ; import pdb ; pdb.set_trace()

        # check that all the runs in runs where the BS fit has been run 
        # are also in DBS
        runToProcess = [run for run in procKeys if (int(run) in dbsKeys and 
                                                    int(run) < lastUnclosedRun)]

        self.logger.info('test') ; import pdb ; pdb.set_trace()
        
        # raise an error if there are runs in RR but not in DBS
        for run in set(procKeys) - set(dbsKeys):
            if run > lastUnclosedRun: continue
            self.logger.error(error_run_not_in_DBS(run))

        self.logger.info('test') ; import pdb ; pdb.set_trace()
        
        for run in runToProcess:
            
            self.logger.info('test') ; import pdb ; pdb.set_trace()
            
            # spacchetta il range di lumi sections in una lista piana.
            # e.g.:
            # 194116 : [1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L]
            RRList = []
            for lr in listOfRunsAndLumiFromRR[run]:
                RRList += [l for l in range(lr[0], lr[1]+1)]
    
            self.logger.info('test') ; import pdb ; pdb.set_trace()
            
            if run in runToProcess:
            
                # get number of files listed in DBS for the given dataset and run
                for data in self.dataSet.split(','):
                    nFiles = getNumberOfFilesToProcessForRun(self.api, data, run)
                    if nFiles != 0:
                        break
                        
                # check that the most part of the files have been processed
                # by crab. Some tolerance is allowed, through config file

                filesToProcess = self._checkProcessedFiles(run, runsAndFiles, nFiles, filesToProcess)
                
                self.logger.info('test') ; import pdb ; pdb.set_trace()
                
                errors          = []
                badProcessed    = []
                badDBSProcessed = []
                badDBS          = []
                badRRProcessed  = []
                badRR           = []
                #It is important for runsAndLumisProcessed[run] to be the first because the comparision is not ==
                badDBSProcessed, badDBS = compareLumiLists(runsAndLumisProcessed   [run]    ,
                                                           listOfRunsAndLumiFromDBS[run]    ,
                                                           listAName = 'the processed lumis',
                                                           listBName = 'DBS lumis'          )
                if len(badDBS) != 0:
                    self.logger.warning('This is weird because I processed '\
                                        'more lumis than the ones that are '\
                                        'in DBS!')
                                        
                if len(badDBSProcessed) != 0 and run in rrKeys:
                    lastError = len(errors)
                    #It is important for runsAndLumisProcessed[run] to be the first because the comparision is not ==
                    badRRProcessed, badRR = compareLumiLists(runsAndLumisProcessed[run]       ,
                                                             RRList                           ,
                                                             listAName = 'the processed lumis',
                                                             listBName = 'Run Registry'       )
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
        FIXME! Write the DOC
        '''
        
        self.logger.info('Begin process')

        ######### Check the last IOV from querying the DBS
        #FIXME: That's an hack to make it work with the example files
        #       by Kevin, need to remove the following line in normal operations
        try:
            lastUploadedIOV = getLastUploadedIOV(self.logger     , 
                                                 self.databaseTag, 
                                                 self.tagName    )
        except:
            self.logger.warning('This is an hack to make it work with the '\
                                'example files by Kevin, '                 \
                                'lastUploadedIOV set by hand to 194000')
            lastUploadedIOV = 194000

        ######### Get list of files processed after the last IOV
        newProcessedFileList = self.getNewFileList(self.sourceDir , 
                                                   lastUploadedIOV)

        ######### Copy files to archive directory
        copiedFiles = cyclicCp(self.sourceDir      , 
                               self.archiveDir     , 
                               newProcessedFileList, 
                               logger = self.logger)
        
        ######### Get from DBS the list of Runs and lumis last IOV
        jsonOfRunsAndLumiFromDBS = getListOfRunsAndLumiFromDBS(self.api       ,
                                                               self.dataSet   , 
                                                               lastUploadedIOV,
                                                               self.logger    )
        import pdb ; pdb.set_trace()
        listOfRunsAndLumiFromDBS = readJson(lastUploadedIOV, 
                                            jsonOfRunsAndLumiFromDBS)
        
        self.logger.info('Getting list of runs >=%s and lumis from Json %s' \
                         %(lastUploadedIOV, self.jsonFileName))
                         
        listOfRunsAndLumiFromJson = readJson(lastUploadedIOV  , 
                                             self.jsonFileName)

        ######### Get list of files to process for DB
        # Possibly we just want to do a diff between the two jsons...
        # https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideGoodLumiSectionsJSONFile?redirectedfrom=CMS.SWGuideGoodLumiSectionsJSONFile#How_to_compare_Good_Luminosity_f
        self.logger.info('Getting list of files to process')
        
        selectedFilesToProcess = self.selectFilesToProcess(listOfRunsAndLumiFromDBS ,
                                                           listOfRunsAndLumiFromJson,
                                                           copiedFiles              ,
                                                           self.archiveDir          )
        if len(selectedFilesToProcess) == 0:
           self.logger.error('There are no files to process')
           exit()
           
        ######### Copy files to working directory
        copiedFiles = cyclicCp(self.archiveDir       , 
                               self.workingDir       , 
                               selectedFilesToProcess, 
                               logger = self.logger  )

        self.logger.info('Sorting and cleaning beamlist')
        beamSpotObjList = []
        for fileName in copiedFiles:
            readBeamSpotFile(self.workingDir + fileName,
                             beamSpotObjList           ,
                             self.fileIOVBase)

        sortAndCleanBeamList(beamSpotObjList, self.fileIOVBase)

        if len(beamSpotObjList) == 0:
            self.logger.warning(warning_no_valid_fit())

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

    from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotWorkflowCfgObj import BeamSpotWorkflowCfg
    
    cfg = BeamSpotWorkflowCfg()
    
    cfg.sourceDir             = '../Runs2012B_FULL/Results/'
    cfg.archiveDir            = '../Runs2012B_FULL/Archive/'
    cfg.workingDir            = '../Runs2012B_FULL/Working/'
    cfg.jsonFileName          = '../beamspot_payload_2012BONLY_merged_JSON_all.txt'
    cfg.databaseTag           = 'BeamSpotObjects_2009_LumiBased_SigmaZ_v29_offline'
    cfg.dataSet               = '/StreamExpress/Run2012B-TkAlMinBias-v1/ALCARECO'
    cfg.fileIOVBase           = 'lumibase'
    cfg.dbIOVBase             = 'lumiid'
    cfg.dbsTolerance          = 200000
    cfg.dbsTolerancePercent   = 5.
    cfg.rrTolerance           = 200000
    cfg.missingFilesTolerance = 200000
    cfg.missingLumisTimeout   = 0
    cfg.mailList              = 'manzoni@cern.ch'
    cfg.payloadFileName       = 'PayloadFile.txt'

    bswf = BeamSpotWorkflow( 
                            cfg       = cfg                  ,
                            lock      = options.lock         ,
                            overwrite = options.overwrite    ,
                           )

    bswf.process()
