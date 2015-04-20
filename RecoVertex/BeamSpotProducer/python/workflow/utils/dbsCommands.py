#!/usr/bin/python

import pprint
import sys
import itertools

if sys.version_info < (2,6,0):
    import json
else:
    import simplejson as json


def ranges(i):
    for a, b in itertools.groupby(enumerate(i), lambda (x, y): y - x):
        b = list(b)
        yield b[0][1], b[-1][1]


def getListOfRunsAndLumiFromDBS(api, dataSet, lastRun = -1, logger = None):
    
    '''
    Queries the DBS for the runs and lumi sections for the dataset <dataSet>
    and run number >= <lastRun>.
    Returns a dictionary of this kind:
        {
            Run : [lumi_1, lumi_2, ...]
            ...
        }
    '''
    
    datasetList    = dataSet.split(',')
    outputFileList = []
    runsAndLumis   = {}
    
    if logger: logger.info('Getting list of runs and lumis from DBS')
    
    for data in datasetList:
        
        if logger: logger.info('Getting list of files from DBS for the %s '    \
                               'dataset. From this list we will query for the '\
                               'run and lumisections' %data)
        
        output_files = [i.values()[0] for i in api.listFiles(dataset = data)]
        
        for file in output_files:
            
            # listFileLumis() only possible call by name   
            output = api.listFileLumis(logical_file_name = file) 
            run    = output[0]['run_num']
            lumis  = output[0]['lumi_section_num']
            
            if run < lastRun             : continue
            if run in runsAndLumis.keys(): continue
            
            # this creates a nicer list of ranges, that can go directly into 
            # a CMS like json 
            runsAndLumis[run] = list(ranges(sorted(lumis)))
            #runsAndLumis[run] = sorted(lumis)
    
    if len(runsAndLumis) == 0:
       if logger: logger.error('There are no new runs or lumis in DBS '\
                               'to process, exiting.')
       exit()
    
    return runsAndLumis   # non compact dictionary

    #runsAndLumisJson = json.dumps(runsAndLumis)        
    #return runsAndLumisJson  # the nice CMS like json


if __name__ == '__main__':

    from RecoVertex.BeamSpotProducer.workflow.utils.setupDbsApi import setupDbsApi

    api     = setupDbsApi()
    dataSet = '/StreamExpress/Run2012B-TkAlMinBias-v1/ALCARECO'
    lastRun = 194000
    
    runsAndLumis = getListOfRunsAndLumiFromDBS(api, dataSet, lastRun)
    
    pp = pprint.PrettyPrinter(indent = 4)
    pp.pprint(runsAndLumis)

