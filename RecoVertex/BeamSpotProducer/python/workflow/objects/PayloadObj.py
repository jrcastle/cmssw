#!/usr/bin/python

from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj import BeamSpot

class Payload(object):
    '''
    Class meant to connect the BeamSpot fit results as saved in a typical
    Payload ASCII file, to actual BeamSpot objects, that are much 
    nicer to handle
    '''
    
    def __init__(self, files):
        '''
        '''
        # can pass a single or a list of txt files
        if not isinstance(files, (list, tuple)):
            files = [files]

        self._readFiles(files)
    
    def _readFiles(self, files):
        '''
        Reads the Payload files.
        '''
        lines = []
        
        for f in files:
            with open(f, 'r') as file:
                for line in file:
                    lines.append(line) 
        
        self.lines = lines
        
    def splitBySingleFit(self):
        '''
        Parses the ASCII files and slices them into a chunk for each fit.
        '''
        singleFits = {}

        for i, line in enumerate(self.lines):
            line = line.rstrip()
            # strings and numbers hardcoded here strictly depend on 
            # the format of the Payload file 
            if 'LumiRange' in line:
                singleFits[line] = [self.lines[j].rstrip() \
                                    for j in range(i-3, i+20)]
        
        return singleFits    

    def fromTextToBS(self):
        '''
        Return a dictionary of dictionaries, as the following:
        { Run : {Lumi Range: BeamSpot Fit Object} }
        
        Parses the files passed when the Payload is instantiated.
        '''
        
        singleFits = self.splitBySingleFit()
        
        beamspots = {}
        
        for k, v in singleFits.items():
            
            bs = BeamSpot()
            bs.Read(v)

            if bs.IOVfirst == bs.IOVlast:
                lsrange = bs.IOVfirst
            else:
                lsrange = '%d-%d' %(bs.IOVfirst, bs.IOVlast)
                
            try:   
                beamspots[bs.Run][lsrange] = bs
            except:
                toadd = { bs.Run : {lsrange : bs} }
                beamspots.update( toadd )
                   
        return beamspots
    
    def getProcessedLumiSections(self):
        '''
        Returns a dictionary with the run numbers as keys and the full
        list of lumi sections processed (fully extended), like:
        { Run : [ LS1, LS2, LS10, ...]}
        '''
        
        beamspots = self.fromTextToBS()
        
        runsAndLumis = { run : [] for run in beamspots.keys() }
        
        for k, v in beamspots.items():
            
            for lumi_range in v.keys():
                try:
                    start = int( lumi_range.split('-')[0] )
                    end   = int( lumi_range.split('-')[1] ) + 1
                except:
                    start = lumi_range
                    end   = start +1
                runsAndLumis[k].extend( range(start, end) )
            
            # sort LS nicely
            runsAndLumis[k] = sorted(runsAndLumis[k])

        return runsAndLumis


if __name__ == '__main__':
    
    myPL = Payload('/afs/cern.ch/work/m/manzoni/beamspot/CMSSW_7_4_0_pre8/src/'\
                   'RecoVertex/BeamSpotProducer/python/workflow/cfg/'          \
                   'Runs2012B_FULL/Results/XRepFinal_1_195660_1.txt'           )

    allLines = myPL.splitBySingleFit()
    
    allBs = myPL.fromTextToBS()
    allBs[195660][60].Dump('bs_dump_195660_LS60.txt', 'w+')
    
    print myPL.getProcessedLumiSections()

