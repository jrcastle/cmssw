#!/usr/bin/python

from datetime import datetime

class DBEntry(object):
    '''
    condDB entry object
    '''
    def __init__(self, line):
        
        # this holds as long as the condDB 
        # output format stays as is
        elements = line.split()
        
        mydt = datetime(
            int(elements[3].split('-')[0]),
            int(elements[3].split('-')[1]),
            int(elements[3].split('-')[2]),
            int(elements[4].split(':')[0]),
            int(elements[4].split(':')[1]),
            int(elements[4].split(':')[2])
        )

        self.run        = int(elements[0])
        self.firstLumi  = int(elements[2])
        self.insertTime = mydt
        self.hash       = elements[5]
        self.type       = elements[6]

if __name__ == '__main__':
    line = '246908 Lumi    96  2015-06-04 14:34:12  c2bf9bdfefd920f5c31ae77e4299df5f6e042b5e  BeamSpotObjects'
    myDBEntry = DBEntry(line)
    print vars(myDBEntry)

