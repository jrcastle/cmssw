#!/usr/bin/python

import datetime

class BeamSpot(object):
    '''
    BeamSpot object
    '''
    def __init__(self):
        self.Reset()
        
    def Reset(self):
        # the names of the errors suggest they're not squared, 
        # while they actually are!
        self.Type          = -1
        self.X             =  0.
        self.Xerr          =  0.
        self.Y             =  0.
        self.Yerr          =  0.
        self.Z             =  0.
        self.Zerr          =  0.
        self.sigmaZ        =  0.
        self.sigmaZerr     =  0.
        self.dxdz          =  0.
        self.dxdzerr       =  0.
        self.dydz          =  0.
        self.dydzerr       =  0.
        self.beamWidthX    =  0.
        self.beamWidthXerr =  0.
        self.beamWidthY    =  0.
        self.beamWidthYerr =  0.
        self.EmittanceX    =  0.
        self.EmittanceY    =  0.
        self.betastar      =  0.
        self.IOVfirst      =  0
        self.IOVlast       =  0
        self.IOVBeginTime  =  0
        self.IOVEndTime    =  0
        self.Run           =  0
        self.XYerr         =  0. 
        self.YXerr         =  0.
        self.dxdzdydzerr   =  0.
        self.dydzdxdzerr   =  0.


    def Dump(self, file, mode = 'a'):
        
        f = open(file, mode)
        
        date_start = datetime.datetime.utcfromtimestamp(self.IOVBeginTime)
        date_end   = datetime.datetime.utcfromtimestamp(self.IOVEndTime  )
        
        #import pdb ; pdb.set_trace()
        
        str_date_start = '%d.%02d.%02d %02d:%02d:%02d' %( date_start.year   ,
                                                          date_start.month  ,
                                                          date_start.day    ,
                                                          date_start.hour   ,
                                                          date_start.minute ,
                                                          date_start.second )
        
        str_date_end   = '%d.%02d.%02d %02d:%02d:%02d' %( date_end.year   ,
                                                          date_end.month  ,
                                                          date_end.day    ,
                                                          date_end.hour   ,
                                                          date_end.minute ,
                                                          date_end.second )
        
        towrite = 'Runnumber {RUN}\n'                                  \
                  'BeginTimeOfFit {DATESTART} GMT {TIMESTART}\n'       \
                  'EndTimeOfFit {DATEEND} GMT {TIMEEND}\n'             \
                  'LumiRange {LUMISTART} - {LUMIEND}\n'                \
                  'Type {TYPE}\n'                                      \
                  'X0 {X0}\n'                                          \
                  'Y0 {Y0}\n'                                          \
                  'Z0 {Z0}\n'                                          \
                  'sigmaZ0 {SZ0}\n'                                    \
                  'dxdz {DXDZ}\n'                                      \
                  'dydz {DYDZ}\n'                                      \
                  'BeamWidthX {BWX}\n'                                 \
                  'BeamWidthY {BWY}\n'                                 \
                  'Cov(0,j) {M00} {M01} 0 0 0 0 0 \n'                  \
                  'Cov(1,j) {M10} {M11} 0 0 0 0 0 \n'                  \
                  'Cov(2,j) 0 0 {M22} 0 0 0 0 \n'                      \
                  'Cov(3,j) 0 0 0 {M33} 0 0 0 \n'                      \
                  'Cov(4,j) 0 0 0 0 {M44} {M45} 0 \n'                  \
                  'Cov(5,j) 0 0 0 0 {M54} {M55} 0 \n'                  \
                  'Cov(6,j) 0 0 0 0 0 0 {M66} \n'                      \
                  'EmittanceX {EMX}\n'                                 \
                  'EmittanceY {EMY}\n'                                 \
                  'BetaStar {BSTAR}\n'                                 \
                  ''.format(RUN       = str(self.Run          ),
                            DATESTART = str(str_date_start    ),
                            TIMESTART = str(self.IOVBeginTime ),
                            DATEEND   = str(str_date_end      ),
                            TIMEEND   = str(self.IOVEndTime   ),
                            LUMISTART = str(self.IOVfirst     ),
                            LUMIEND   = str(self.IOVlast      ),
                            TYPE      = str(self.Type         ),
                            X0        = str(self.X            ),
                            Y0        = str(self.Y            ),
                            Z0        = str(self.Z            ),
                            SZ0       = str(self.sigmaZ       ),
                            DXDZ      = str(self.dxdz         ),
                            DYDZ      = str(self.dydz         ),
                            BWX       = str(self.beamWidthX   ),
                            BWY       = str(self.beamWidthY   ),
                            # diagonal
                            M00       = str(self.Xerr         ),
                            M11       = str(self.Yerr         ),
                            M22       = str(self.Zerr         ),
                            M33       = str(self.sigmaZerr    ),
                            M44       = str(self.dxdzerr      ),
                            M55       = str(self.dydzerr      ),
                            M66       = str(self.beamWidthXerr),
                            # off diagonal
                            M01       = str(self.XYerr        ),
                            M10       = str(self.YXerr        ),
                            M45       = str(self.dxdzdydzerr  ),
                            M54       = str(self.dydzdxdzerr  ),
                            EMX       = str(self.EmittanceX   ),
                            EMY       = str(self.EmittanceY   ),
                            BSTAR     = str(self.betastar     ),
                            )
        
        f.write(towrite)
       
if __name__ == '__main__':
    mybs = BeamSpot()
    mybs.Dump('bs_dump_dummy.txt', 'w+')


                  