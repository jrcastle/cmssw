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

            bs.Run           = int  ( v[ 0].split()[1] )
            bs.IOVBeginTime  = int  ( v[ 1].split('GMT')[1] )
            bs.IOVEndTime    = int  ( v[ 2].split('GMT')[1] )
            bs.IOVfirst      = int  ( v[ 3].split()[1] )
            bs.IOVlast       = int  ( v[ 3].split()[3] )
            bs.Type          = int  ( v[ 4].split()[1] )
            
            bs.X             = float( v[ 5].split()[1] )
            bs.Y             = float( v[ 6].split()[1] )
            bs.Z             = float( v[ 7].split()[1] )

            bs.sigmaZ        = float( v[ 8].split()[1] )
            bs.dxdz          = float( v[ 9].split()[1] )
            bs.dydz          = float( v[10].split()[1] )

            bs.beamWidthX    = float( v[11].split()[1] )
            bs.beamWidthY    = float( v[12].split()[1] )
            
            # covariance matrix defined here
            # https://github.com/MilanoBicocca-pix/cmssw/blob/CMSSW_7_5_X_beamspot_workflow_riccardo/RecoVertex/BeamSpotProducer/src/PVFitter.cc#L306
            # diagonal terms
            bs.Xerr          = float( v[13].split()[1] )
            bs.Yerr          = float( v[14].split()[2] )
            bs.Zerr          = float( v[15].split()[3] )
            bs.sigmaZerr     = float( v[16].split()[4] )
            bs.dxdzerr       = float( v[17].split()[5] )
            bs.dydzerr       = float( v[18].split()[6] )
            bs.beamWidthXerr = float( v[19].split()[7] )
            # bs.beamWidthYerr = float( v[16].split()[1] ) # not in cov matrix!
            # off diagonal terms
            bs.XYerr         = float( v[13].split()[2] )
            bs.YXerr         = float( v[14].split()[1] )
            bs.dxdzdydzerr   = float( v[17].split()[6] )
            bs.dydzdxdzerr   = float( v[18].split()[5] )
                
            bs.EmittanceX    = float( v[20].split()[1] )
            bs.EmittanceY    = float( v[21].split()[1] )

            bs.betastar      = float( v[22].split()[1] )

            try:   
                beamspots[bs.Run][str('%d-%d' %(bs.IOVfirst, bs.IOVlast))] = bs
            except:
                toadd = { bs.Run : {str('%d-%d' %(bs.IOVfirst, bs.IOVlast)) : bs} }
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
                start = int( lumi_range.split('-')[0] )
                end   = int( lumi_range.split('-')[1] ) + 1
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
    allBs[195660]['60-60'].Dump('bs_dump_195660_LS60.txt', 'w+')
    
    print myPL.getProcessedLumiSections()

