#!/usr/bin/python

import ROOT
from array import array
from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj import BeamSpot
from RecoVertex.BeamSpotProducer.workflow.objects.IOVObj import IOV

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

    def fromTextToBS(self, iov = False):
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
            
            if iov:
                beamspots[bs.GetIOV()] = bs

            else:
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

    def plot(self, variable, run, iLS = -1, fLS = 1e6, savePdf = False):
        '''
        For a given run, plot a BS parameter as a function of LS.
        Allows multiple LS bins.
        '''
        # get the list of BS objects
        myBS = {k:v for k, v in self.fromTextToBS(iov = True).items()
                if k.RunFirst == run and k.RunLast == run 
                and k.LumiFirst >= iLS and k.LumiLast <= fLS}

        nBins = len(myBS)
                
        x = array('f', [0.5 * (k.LumiLast + k.LumiFirst) for k in myBS.keys()])
        y = array('f', [getattr(v, variable) for v in myBS.values()])
        
        xe = array('f', [0.5 * max(1, (k.LumiLast - k.LumiFirst)) for k in myBS.keys()])
        ye = array('f', [getattr(v, variable + 'err') for v in myBS.values()])
        
        tge = ROOT.TGraphErrors(nBins, x, y, xe, ye)
        
        tge.SetTitle('Run ' + str(run))
        tge.GetYaxis().SetTitle(variable)
        tge.GetXaxis().SetTitle('Lumi Section')
        tge.SetMarkerStyle(8)
       
        c1 = ROOT.TCanvas('','',1000,700)
        tge.Draw('AP')
        if savePdf: 
            c1.SaveAs('BS_plot_%d_%s.pdf' %(run, variable))


if __name__ == '__main__':

    #file = '/afs/cern.ch/user/m/manzoni/public/beamspot_validation/' \
    #       'BeamFit_LumiBased_NewAlignWorkflow_alcareco_Run247388.txt'


    file = '/afs/cern.ch/user/f/fiorendi/public/beamSpot/'\
           'beamspot_firstData_run247324_byLumi_all_lumi98_107.txt'
    file = '/afs/cern.ch/user/f/fiorendi/public/beamSpot/bs_weighted_results_246908.txt'
    myPL = Payload(file)
    
    #myPL = Payload('/afs/cern.ch/work/m/manzoni/beamspot/CMSSW_7_4_0_pre8/src/'\
    #               'RecoVertex/BeamSpotProducer/python/workflow/cfg/'          \
    #               'Runs2012B_FULL/Results/XRepFinal_1_195660_1.txt'           )
    
    #myPL = Payload('payload_test.txt')
    allLines = myPL.splitBySingleFit()
    
    allBs = myPL.fromTextToBS()

    allBs2 = myPL.fromTextToBS(iov = True)
    
    #allBs[195660][60].Dump('bs_dump_195660_LS60.txt', 'w+')
    #allBs[247324][98].Dump('bs_dump_247388_LS60.txt', 'w+')
    
    print myPL.getProcessedLumiSections()

    myPL.plot('X'         , 246908, savePdf = True)
    myPL.plot('Y'         , 246908, savePdf = True)
    myPL.plot('Z'         , 246908, savePdf = True)
    myPL.plot('sigmaZ'    , 246908, savePdf = True)
    myPL.plot('dxdz'      , 246908, savePdf = True)
    myPL.plot('dydz'      , 246908, savePdf = True)
    myPL.plot('beamWidthX', 246908, savePdf = True)
    myPL.plot('beamWidthY', 246908, savePdf = True)

    myPL.plot('X'         , 246908, iLS = 90, fLS = 110, savePdf = True)
