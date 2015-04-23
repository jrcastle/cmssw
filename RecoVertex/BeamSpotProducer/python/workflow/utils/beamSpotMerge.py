#!/usr/bin/python

from math import sqrt
from numpy import average
from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj import BeamSpot


def splitByDrift(fullList, maxDriftX, maxDriftY, maxDriftZ):
    '''
    Group lumi sections where the Beam Spot position does not 
    drift beyond the parameters specified by the user.
    If the drift in any direction exceeds the boundaries,
    split the lumi section collection.
    Returns a list of lumi section ranges.
    '''
    # RIC: to be implemented FIXME!
    pass

def averageBeamSpot(bslist):
    '''
    Returns a Beam Spot object containing the weighed average position
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
    # if you want to average additional quantities
    # just add a pair (quantity, its error) to the list of pairs    
    for pair in [('X'         ,'Xerr'         ),
                 ('Y'         ,'Yerr'         ),
                 ('Z'         ,'Zerr'         ),
                 ('sigmaZ'    ,'sigmaZerr'    ),
                 ('dxdz'      ,'dxdzerr'      ),
                 ('dydz'      ,'dydzerr'      ),
                 ('beamWidthX','beamWidthXerr'),
                 ('beamWidthY','beamWidthYerr')]:
    
        value = lambda x: getattr(x, pair[0])
        error = lambda x: 1./max(1e-22, 
                                 getattr(x, pair[1]) * getattr(x, pair[1]))
    
        ave_value, ave_error = average(a        = [value(bs) for bs in bslist],
                                       weights  = [error(bs) for bs in bslist],
                                       returned = True                        )

        setattr(averageBS, pair[0], ave_value         )
        setattr(averageBS, pair[1], 1./sqrt(ave_error))
    
    # assuming that ls are contiguous in the list given.
    averageBS.IOVfirst     = firstBS.IOVfirst    
    averageBS.IOVlast      = lastBS .IOVlast     
    averageBS.IOVBeginTime = firstBS.IOVBeginTime
    averageBS.IOVEndTime   = lastBS .IOVEndTime  

    # check that these attributes are the same for all BS in the list.    
    for attr in ('Type', 'Run', 'EmittanceX', 'EmittanceY', 'betastar'):
        for i, bs in enumerate(bslist):
            if getattr(bs, attr) != getattr(firstBS, attr):
                print 'ERROR: "%s" for the %d element of the '    \
                      'Beam Spot collection varies from the first'\
                      %(attr, i)
                exit()
  
    averageBS.Type       = firstBS.Type        
    averageBS.Run        = firstBS.Run         
    averageBS.EmittanceX = firstBS.EmittanceX  
    averageBS.EmittanceY = firstBS.EmittanceY  
    averageBS.betastar   = firstBS.betastar    

    return averageBS

if __name__ == '__main__':
    print 'not yet implemented. Put here your tests'

