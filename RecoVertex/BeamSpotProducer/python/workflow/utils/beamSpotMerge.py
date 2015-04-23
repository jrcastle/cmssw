#!/usr/bin/python

from math import sqrt
from numpy import average
from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj import BeamSpot

def averageBeamSpot(bslist):
    '''
    Returns a Beam Spot object contained the weighed average position
    of the Beam Spots in the list.
    
    Start and end time and lumi section are taken from the first and 
    last Beam Spot object respectively, so make sure the collection
    is sorted.
    '''

    # RIC: Type should have something to do with the fit convergence
    #      Possibly we should filter by Type.
    #      Add some logging. 

    bslist = [bs for bs in bslist if bs.Type > 0]

    # get the first and the last BS in the list
    firstBS = bslist[0 ]
    lastBS  = bslist[-1]
    
    # instantiate a new, empty Beam Spot object that will store the averages
    averageBS = BeamSpot()
        
    # weighed average of the position
    # FIXME! RIC: in the past it seems to me that they used bs.Xerr *squared*
    #             as weight, which means the error at the fourth power.
    #             Quirky...
    averageBS.X         , averageBS.Xerr          = average( [bs.X          for bs in bslist], weights = [1./max(1e-22, bs.Xerr          * bs.Xerr          ) for bs in bslist], returned = True )
    averageBS.Y         , averageBS.Yerr          = average( [bs.Y          for bs in bslist], weights = [1./max(1e-22, bs.Yerr          * bs.Yerr          ) for bs in bslist], returned = True )
    averageBS.Z         , averageBS.Zerr          = average( [bs.Z          for bs in bslist], weights = [1./max(1e-22, bs.Zerr          * bs.Zerr          ) for bs in bslist], returned = True )
    averageBS.sigmaZ    , averageBS.sigmaZerr     = average( [bs.sigmaZ     for bs in bslist], weights = [1./max(1e-22, bs.sigmaZerr     * bs.sigmaZerr     ) for bs in bslist], returned = True )
    averageBS.dxdz      , averageBS.dxdzerr       = average( [bs.dxdz       for bs in bslist], weights = [1./max(1e-22, bs.dxdzerr       * bs.dxdzerr       ) for bs in bslist], returned = True )
    averageBS.dydz      , averageBS.dydzerr       = average( [bs.dydz       for bs in bslist], weights = [1./max(1e-22, bs.dydzerr       * bs.dydzerr       ) for bs in bslist], returned = True )
    averageBS.beamWidthX, averageBS.beamWidthXerr = average( [bs.beamWidthX for bs in bslist], weights = [1./max(1e-22, bs.beamWidthXerr * bs.beamWidthXerr ) for bs in bslist], returned = True )
    averageBS.beamWidthY, averageBS.beamWidthYerr = average( [bs.beamWidthY for bs in bslist], weights = [1./max(1e-22, bs.beamWidthYerr * bs.beamWidthYerr ) for bs in bslist], returned = True )

    # RIC: this sucks, I know
    # BTW I cannot reproduce the old results, as far as errors go,
    # for run 195660, whereas I can find the correct X, Y, Z position.
    averageBS.Xerr          = 1./sqrt(averageBS.Xerr         )
    averageBS.Yerr          = 1./sqrt(averageBS.Yerr         ) 
    averageBS.Zerr          = 1./sqrt(averageBS.Zerr         ) 
    averageBS.sigmaZerr     = 1./sqrt(averageBS.sigmaZerr    ) 
    averageBS.dxdzerr       = 1./sqrt(averageBS.dxdzerr      ) 
    averageBS.dydzerr       = 1./sqrt(averageBS.dydzerr      ) 
    averageBS.beamWidthXerr = 1./sqrt(averageBS.beamWidthXerr) 
    averageBS.beamWidthYerr = 1./sqrt(averageBS.beamWidthYerr) 

    # assuming that ls are contiguous in the list given.
    averageBS.IOVfirst      = firstBS.IOVfirst    
    averageBS.IOVlast       = lastBS .IOVlast     
    averageBS.IOVBeginTime  = firstBS.IOVBeginTime
    averageBS.IOVEndTime    = lastBS .IOVEndTime  

    # check that these attributes are the same for all BS in the list.    
    for attr in ('Type', 'Run', 'EmittanceX', 'EmittanceY', 'betastar'):
        for i, bs in enumerate(bslist):
            if getattr(bs, attr) != getattr(firstBS, attr):
                print 'ERROR: "%s" for the %d element of the '    \
                      'Beam Spot collection varies from the first'\
                      %(attr, i)
                exit()
  
    averageBS.Type          = firstBS.Type        
    averageBS.Run           = firstBS.Run         
    averageBS.EmittanceX    = firstBS.EmittanceX  
    averageBS.EmittanceY    = firstBS.EmittanceY  
    averageBS.betastar      = firstBS.betastar    

    return averageBS

if __name__ == '__main__':
    print 'not yet implemented. Put here your tests'

