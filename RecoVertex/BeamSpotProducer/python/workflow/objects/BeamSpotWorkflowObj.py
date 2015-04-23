import glob
import os
import re

import RecoVertex.BeamSpotProducer.workflow.utils.colorer

from RecoVertex.BeamSpotProducer.workflow.objects.PayloadObj   import Payload

from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages  import *
from RecoVertex.BeamSpotProducer.workflow.utils.setupDbsApi    import setupDbsApi
from RecoVertex.BeamSpotProducer.workflow.utils.locker         import Locker
from RecoVertex.BeamSpotProducer.workflow.utils.readJson       import readJson
from RecoVertex.BeamSpotProducer.workflow.utils.dbsCommands    import getListOfRunsAndLumiFromDBS
from RecoVertex.BeamSpotProducer.workflow.utils.dbsCommands    import getNumberOfFilesToProcessForRun
from RecoVertex.BeamSpotProducer.workflow.utils.condDbCommands import getLastUploadedIOV
from RecoVertex.BeamSpotProducer.workflow.utils.compareLists   import compareLists
from RecoVertex.BeamSpotProducer.workflow.utils.timeoutManager import timeoutManager 
from RecoVertex.BeamSpotProducer.workflow.utils.beamSpotMerge  import averageBeamSpot
    
class BeamSpotWorkflow(object):
    '''
    FIXME! docstring to be written
    '''

    def __init__(self, cfg, lock, overwrite, globaltag, logger):
                
        self.logger  = logger
        self.tagName = globaltag
        
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
        
        # This should be a bit more elaborated, possibly
        self.procFileList = [file for file in os.listdir(self.sourceDir) 
                             if '.txt' in file]

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
        
        # get the run numbers from the file names in resultsDir.
        runs = set([re.findall(r'\d+', file)[1] for file in self.procFileList]) 
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
        
    def _getRunDiffs(self, runsLumisCRAB, runsLumisJSON, runsLumisDBS):
        '''
        Checks the runs that have been processed by CRAB, or listed
        in DBS or listed in the JSON/RunRegistry.
        Returns all the diffs.  
        '''
        ######### Get run numbers of runs that are in CRAB but not in JSON
        ######### and vice-versa
        runsInJSONnotInCRAB, runsInCRABnotInJSON = compareLists(
            runsLumisCRAB.keys()   , 
            runsLumisJSON.keys()   , 
            tolerance = 100        , 
            listAName = 'CRAB runs', 
            listBName = 'JSON runs',
            logger = self.logger   
        )

        ######### Get run numbers of runs that are in DBS but not in the JSON
        ######### and vice-versa
        runsInJSONnotInDBS, runsInDBSnotInJSON = compareLists(
            runsLumisDBS .keys()   , 
            runsLumisJSON.keys()   , 
            tolerance = 100        , 
            listAName = 'DBS  runs', 
            listBName = 'JSON runs',
            logger = self.logger   
        )
        
        ######### Get run numbers of runs that are in CRAB but not in DBS
        ######### and vice-versa
        runsInDBSnotInCRAB, runsInCRABnotInDBS = compareLists(
            runsLumisCRAB.keys()   , 
            runsLumisDBS .keys()   , 
            tolerance = 100        , 
            listAName = 'CRAB runs', 
            listBName = 'DBS  runs',
            logger = self.logger   
        )
            
        return (runsInJSONnotInCRAB, runsInCRABnotInJSON,
                runsInJSONnotInDBS , runsInDBSnotInJSON ,
                runsInDBSnotInCRAB , runsInCRABnotInDBS )

    def _logMissingLumi(self, missingProcLS, lumisInJSONnotInCRAB,
                        lumisInDBSnotInCRAB, lumisJSON, run, filesToProcess):
        '''
        If, for a given run, there are missing processed lumis, check if 
        they're within the tolerance, or raise some errors.
        '''
                        
        self.logger.warning('I have not processed some of the lumis that '\
                            'are in the run registry for run: %d' %run    )
                            
        self.logger.warning('lumisInJSONnotInCRAB size is '      \
                            + str( len(lumisInJSONnotInCRAB) ) + \
                            ' and lumisInDBSnotInCRAB size is '  \
                            + str( len(lumisInDBSnotInCRAB) )    )
        
        percentageMissing = float(len(missingProcLS)) / float(len(lumisJSON))
        
        # relative tolerance
        if percentageMissing <= self.dbsTolerancePercent:
            self.logger.warning(
                'I didn\'t process ' + str(percentageMissing) +      \
                '% of the lumis but '                                \
                'I am within the ' + str(self.dbsTolerancePercent) + \
                '% set in the configuration. Which '                 \
                'corrispond to ' + str(missingProcLS) + ' out of '   \
                + str(lumisJSON) + ' lumis'
            )
            
            self.logger.warning('{BAD}'.format(BAD = missingProcLS))
        
        # absolute tolerance
        elif len(missingProcLS) <= self.dbsTolerance:
            self.logger.warning(
                'I didn\'t process ' + str(missingProcLS) +              \
                ' lumis but I am within the ' + str(self.dbsTolerance) + \
                ' lumis set in the configuration. Which corrispond to '  \
                + str(missingProcLS) + ' out of '                        \
                + str(lumisJSON) + ' lumis'
            )
            self.logger.warning('{BAD}'.format(BAD = missingProcLS))
 
        # RIC: beyond what's acceptable
        else:    
            self.logger.critical(
                'For run ' + str(run) + ' I didn\'t process '            \
                + str(percentageMissing) + '% of the lumis and I '       \
                'am not within the ' + str(self.dbsTolerancePercent)     \
                + '% set in the configuration. The number of '           \
                'lumis that I didn\'t process (' + str(missingProcLS) +  \
                ' out of ' + str(dbsTolerance) +                         \
                ') is greater also than the '                            \
                + str(self.dbsTolerance) +                               \
                ' lumis that I can tolerate. I can\'t process runs >= '  \
                + str(run) + ' but I\'ll process the runs before!'
            )
            return filesToProcess
        
    def process(self):
        
        '''
        FIXME! Write the DOC
        '''
        
        self.logger.info('Begin process')


        filesToProcess = []


        ######### Check the last IOV from querying the COND DB
        lastUploadedIOV = getLastUploadedIOV(self.databaseTag, 
                                             self.tagName    ,
                                             self.logger     )
        
        # RIC: just for testing
        lastUploadedIOV = 194000
                                             
        ######### Get processed files in self.sourceDir, with run 
        ######### number greater than lastUploadedIOV
        runsLumisCRAB, runsLumisBSCRAB = self._getRunLumiBeamspot(
                                            self.sourceDir, 
                                            lastUploadedIOV
                                            )
                
        ######### Get from DBS the list of Runs and lumis after last IOV
        runsLumisDBS  = getListOfRunsAndLumiFromDBS(self.api       , 
                                                    self.dataSet   , 
                                                    lastUploadedIOV,
                                                    packed = False )

        ######### Get from JSON the list of Runs and lumis after last IOV
        runsLumisJSON = readJson(lastUploadedIOV  , 
                                 self.jsonFileName,
                                 packed = False   )

        ######## At this point for CRAB, JSON and DBS we have a dictionary like:
        ######## {194116 : [2, 3, 4, 5, 15, 16, 17, 18]}
        
        ######### Get from runs in {DBS, CRAB, JSON} but not in {DBS, CRAB, JSON}
        runDiffs =  self._getRunDiffs(runsLumisCRAB, 
                                      runsLumisJSON, 
                                      runsLumisDBS )
        
        runsInJSONnotInCRAB = runDiffs[0]
        runsInCRABnotInJSON = runDiffs[1]
        runsInJSONnotInDBS  = runDiffs[2]
        runsInDBSnotInJSON  = runDiffs[3]
        runsInDBSnotInCRAB  = runDiffs[4]
        runsInCRABnotInDBS  = runDiffs[5]
 
        if len(runsInCRABnotInDBS) and \
            lastUnclosedRun not in runsInCRABnotInDBS:
            self.logger.critical('Runs {RUNS} are processed but not in DBS'\
                                 ''.format(RUNS = runsInCRABnotInDBS))
        
        # check that for the runs that are processed and in both JSON and DBS
        for run in runsLumisCRAB.keys():
            # number of files in DBS for the given run 
            nDbsFilesForRun = getNumberOfFilesToProcessForRun(self.api    ,
                                                              self.dataSet,
                                                              run         )

            # number of processed files and number of dbs files
            # for the given run
            procFilesForRun = [file for file in self.procFileList
                               if str(run) in file]
                               
            # do something with the timeoutManager,
            # I have to understand this... later
            if len(procFilesForRun) < nDbsFilesForRun:
                pass
            else:
                timeoutManager('DBS_VERY_BIG_MISMATCH_Run%d'%run)
                timeoutManager('DBS_MISMATCH_Run%d'         %run)
                

            # check how many lumis for the given run have been processed
            lumisInDBSnotInCRAB, lumisInCRABnotInDBS = compareLists(
                runsLumisCRAB[run]                      , 
                runsLumisDBS [run]                      , 
                tolerance = 100                         , 
                listAName = 'CRAB lumis for run %d' %run, 
                listBName = 'DBS  lumis for run %d' %run,
                logger = self.logger                    
            )
            
            # check whether there are lumis in CRAB that are not in DBS
            # this should not happen at all!
            if len(lumisInCRABnotInDBS):
                self.logger.critical(
                    'This is weird because for run {RUN} '\
                    'I processed these '                  \
                    'lumis {LUMIS} that are not in '      \
                    'DBS!'.format(RUN   = str(run)           ,
                                  LUMIS = lumisInCRABnotInDBS)
                )
    
            lumisInJSONnotInCRAB = []
    
            if len(lumisInDBSnotInCRAB) and run in runsLumisJSON.keys():
                # check how many lumis for the given run have been processed
                lumisInJSONnotInCRAB, lumisInCRABnotInJSON = compareLists(
                    runsLumisCRAB[run]                      , 
                    runsLumisJSON[run]                      , 
                    tolerance = 100                         , 
                    listAName = 'CRAB lumis for run %d' %run, 
                    listBName = 'JSON lumis for run %d' %run,
                    logger = self.logger                    
                )
            
            # consider as missing lumis the ones that are present in 
            # both the JSON and DBS but not in CRAB
            missingProcLS = set(lumisInJSONnotInCRAB) & set(lumisInDBSnotInCRAB)
            
            if len(missingProcLS):
                self._logMissingLumi(missingProcLS, lumisInJSONnotInCRAB,
                                     lumisInDBSnotInCRAB, lumisJSON, 
                                     run, filesToProcess)
            
            if len(set(lumisInDBSnotInCRAB) - set(lumisInJSONnotInCRAB)):
                self.logger.warning('All JSON lumis have been processed , '\
                                    'but some DBS lumis are not. '         \
                                    'Fine, moving on') 
            
        # FIXME! RIC: here we'd need to check the BS slow drift
        #             and sub divide the beam spot list into smaller 
        #             chunks to be fed into averageBeamSpot().
        #             Can be externalised to another function.
        
        bslist_1_50    = runsLumisBSCRAB.values()[0].values()[0   : 50 ]
        bslist_51_67   = runsLumisBSCRAB.values()[0].values()[50  : 67 ]
        bslist_68_105  = runsLumisBSCRAB.values()[0].values()[67  : 105]
        bslist_106_109 = runsLumisBSCRAB.values()[0].values()[105 : 109]
        bslist_110_134 = runsLumisBSCRAB.values()[0].values()[109 : 134]
        bslist_135_153 = runsLumisBSCRAB.values()[0].values()[134 : 160]
        bslist_161_182 = runsLumisBSCRAB.values()[0].values()[160 : 182]
        bslist_183_186 = runsLumisBSCRAB.values()[0].values()[182 : 192]
        bslist_193_201 = runsLumisBSCRAB.values()[0].values()[192 : 201]
        bslist_202_220 = runsLumisBSCRAB.values()[0].values()[201 : 220]
            
        # just for testing, for now, merging the entire run 195660
        # aveBeamSpot = averageBeamSpot(runsLumisBSCRAB.values()[0].values())    
        # aveBeamSpot.Dump('Payload_tot.txt')
        
        for lsrange in [bslist_1_50   , bslist_51_67  , 
                        bslist_68_105 , bslist_106_109,
                        bslist_110_134, bslist_135_153, 
                        bslist_161_182, bslist_183_186,
                        bslist_193_201, bslist_202_220]:
            aveBeamSpot = averageBeamSpot(lsrange) 
            aveBeamSpot.Dump(self.payloadFileName)
                
        if hasattr(self, 'locker'):
            self.locker.unlock()

        

if __name__ == '__main__':

    from RecoVertex.BeamSpotProducer.workflow.utils.initLogger            import initLogger
    
    cfg = object()
    
    cfg.sourceDir             = 'Runs2012B_FULL/Results/'
    cfg.archiveDir            = 'Runs2012B_FULL/Archive/'
    cfg.workingDir            = 'Runs2012B_FULL/Working/'
    cfg.jsonFileName          = 'beamspot_payload_2012BONLY_merged_JSON_all.txt'
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

    logger = initLogger(emails = cfg.mailList)
    
    bswf = BeamSpotWorkflow( 
                             cfg       = cfg              ,
                             lock      = options.lock     ,
                             overwrite = options.overwrite,
                             globaltag = options.tag      ,
                             logger    = logger
                           )

    bswf.process()
