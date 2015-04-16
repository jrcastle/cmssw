#!/usr/bin/python

from BeamSpotWorkflow_ric import BeamSpotWorkflow

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
                       )

bswf.process()
