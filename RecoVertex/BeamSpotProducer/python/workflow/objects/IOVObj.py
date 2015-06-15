class IOV(object):
    '''
    Interval of Validity object
    '''
    def __init__(self, *args):
        '''
        IOV() initiates a dummy IOV, all values set to -1.
        IOV(BeamSpot) initiates a IOV object with, run, LS and 
        time boundaries taken from the given BeamSpot object.
        Inly positional call (for now)
        '''
        self.since     = -1
        self.till      = -1
        self.RunFirst  = -1
        self.RunLast   = -1
        self.LumiFirst = -1
        self.LumiLast  = -1
        
        if len(args):
            self.since     = args[0].IOVBeginTime
            self.till      = args[0].IOVEndTime
            self.RunFirst  = args[0].Run
            self.RunLast   = args[0].Run
            self.LumiFirst = args[0].IOVfirst
            self.LumiLast  = args[0].IOVlast
