import commands
import glob
import os
import re
import sys

import RecoVertex.BeamSpotProducer.workflow.utils.colorer

from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj    import BeamSpot
from RecoVertex.BeamSpotProducer.workflow.objects.PayloadObj     import Payload

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

    def __init__(self, cfg, lock, overwrite, globaltag):
                
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

        self.tagName               = globaltag
        
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

    def _getRunLumiBeamspot(self, resultsDir, minRun = -1):
        '''
        Loads the payload files produced by the crab jobs.
        Returns the runs and lumi sections processed as well as a collection
        with one BeamSpotObject for each single fit.
        '''
        
        # get the run numbers from the file names in resultsDir
        fileList = [file for file in os.listdir(resultsDir) 
                    if '.txt' in file]
                    #if os.path.isfile(file) and '.txt' in file]
                    
        runs = set([re.findall(r'\d+', file)[1] for file in fileList]) 
        files = []
        for run in runs:
            if int(run) <= minRun: continue
            files.extend(glob.glob('/'.join([resultsDir, '*' + run + '*.txt'])))

        # for each run parse the ASCII files and load the BS objects 
        newPayloadTot = Payload(files)

        # this returns a dictionary like {195660 : [1,2,3,...]}
        runsLumis = newPayloadTot.getProcessedLumiSections() 

        # this returns a dictionary like {195660 : {'60-61' : BeamSpotObject}}
        runsLumisBS = newPayloadTot.fromTextToBS() 
        
        return runsLumis, runsLumisBS

    def _unpackList(self, packedList):
        '''
        Unpacks a JSON like dictionary into something plainer
        e.g.:
        {194116 : [[2-5],[15-18]]}
        {194116 : [2, 3, 4, 5, 15, 16, 17, 18]}
        '''

        unpackedList = {}
        
        for k, v in packedList.items():
            LSlist = []
            for lr in v:
                LSlist += [l for l in range(lr[0], lr[1]+1)]
            
            unpackedList[k] = LSlist
        
        return unpackedList
               
    def process(self):
        
        '''
        FIXME! Write the DOC
        '''
        
        self.logger.info('Begin process')

        ######### Check the last IOV from querying the COND DB
        #lastUploadedIOV = getLastUploadedIOV(self.databaseTag, 
        #                                     self.tagName    ,
        #                                     self.logger     )
        
        # RIC: just for testing
        lastUploadedIOV = 194000
                                             
        ######### Get processed files in self.sourceDir, with run 
        ######### number greater than lastUploadedIOV
        runsLumisCRAB, runsLumisBSCRAB = self._getRunLumiBeamspot(self.sourceDir , 
                                                                  lastUploadedIOV)
                
        ######### Get from DBS the list of Runs and lumis last IOV
        jsonOfRunsAndLumiFromDBS = getListOfRunsAndLumiFromDBS(self.api       ,
                                                               self.dataSet   , 
                                                               lastUploadedIOV,
                                                               self.logger    )
        listOfRunsAndLumiFromDBS = readJson(lastUploadedIOV, 
                                            jsonOfRunsAndLumiFromDBS)
        
        self.logger.info('Getting list of runs >=%s and lumis from Json %s' \
                         %(lastUploadedIOV, self.jsonFileName))
                         
        listOfRunsAndLumiFromJson = readJson(lastUploadedIOV  , 
                                             self.jsonFileName)


        # this passage can be avoided and be made part of readJson
        # e.g. with an option, packed or unpacked.
        runsLumisDBS  = self._unpackList(listOfRunsAndLumiFromDBS )
        runsLumisJSON = self._unpackList(listOfRunsAndLumiFromJson)

        
        # This part can be outsourced to a module outside self.process.

        ######### Get run numbers of runs that are in CRAB but not in JSON
        ######### and vice-versa
        inJSONnotInCRAB, inCRABnotInJSON = compareLumiLists(self.logger         , 
                                                            runsLumisCRAB.keys(), 
                                                            runsLumisJSON.keys(), 
                                                            tolerance = 100     , 
                                                            listAName = 'CRAB'  , 
                                                            listBName = 'JSON'  )

        if len(inCRABnotInJSON) > 0:
            self.logger.error('in CRAB not in JSON')
            #exit()

        ######### Get run numbers of runs that are in DBS but not in the JSON
        ######### and vice-versa
        inJSONnotInDBS, inDBSnotInJSON = compareLumiLists(self.logger         , 
                                                          runsLumisDBS .keys(), 
                                                          runsLumisJSON.keys(), 
                                                          tolerance = 100     , 
                                                          listAName = 'DBS'   , 
                                                          listBName = 'JSON'  )
        if len(inJSONnotInDBS) > 0:
            self.logger.error('in JSON not in DBS')
            #exit()
        
        
        ######### Get run numbers of runs that are in CRAB but not in DBS
        ######### and vice-versa
        inDBSnotInCRAB, inCRABnotInDBS = compareLumiLists(self.logger         , 
                                                          runsLumisCRAB.keys(), 
                                                          runsLumisDBS .keys(), 
                                                          tolerance = 100     , 
                                                          listAName = 'CRAB'  , 
                                                          listBName = 'DBS'   )

        if len(inCRABnotInDBS) > 0:
            self.logger.error('in CRAB not in DBS')
            #exit()
        
        # RIC: here some logging and management of the missing runs is needed
        # for now I only try to get it done till the end.
        # 
        # Not clear what should be consistent with what though, i.e.
        # I think the JSON need to fully contained in the DBS, 
        # but not vice versa
        # The CRAB stuff should be contained in both DBS and JSON, 
        # but not vice versa
        # 
        # Tolerances should be double checked too.
        #
        # exit() are commented out for now, since my dummy setup 
        # it'd hardly be complete
        
        
        # check that for the runs that are processed and in both JSON and DBS
        for run in runsLumisCRAB.keys():
            LSinJSONnotInDBS, LSinDBSnotInJSON = compareLumiLists(self.logger         , 
                                                                  runsLumisCRAB[run]  , 
                                                                  runsLumisJSON[run]  , 
                                                                  tolerance = 5       , 
                                                                  listAName = 'CRABLS', 
                                                                  listBName = 'JSONLS')
            
        # RIC: works as expected so far. 
        # It would be nice to have a complete example, though, rather than 
        # bits and pieces
        #
        # In principle, if all the sanity tests are passed we should
        # merge all the processed files.
        # 
        # In any case, it'd better to foresee some additional skim
        # of the CRAB files at this point
        #
        # we also need to rewrite completely 'createWeightedPayloads'
        # it is such a mess.

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
