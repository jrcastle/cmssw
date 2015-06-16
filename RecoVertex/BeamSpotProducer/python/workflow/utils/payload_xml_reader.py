#!/usr/bin/python

from math import sqrt
from subprocess import PIPE, Popen
from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotObj  import BeamSpot
from RecoVertex.BeamSpotProducer.workflow.objects.IOVObj       import IOV
from RecoVertex.BeamSpotProducer.workflow.objects.PayloadObj   import Payload
from RecoVertex.BeamSpotProducer.workflow.utils.condDbCommands import getListOfUploadedIOV
from RecoVertex.BeamSpotProducer.workflow.utils.condDbCommands import dumpXMLPayloadByHash


databaseTag = 'BeamSpotObjects_PCL_byLumi_v0_prompt'
firstIOV    = 247321
lastIOV     = 247324
plFile      = 'dummy_bs.txt'

dbentries = getListOfUploadedIOV(databaseTag, firstIOV, lastIOV)

for i, entry in enumerate(dbentries):
    print 'entry %d/%d' %(i+1, len(dbentries))
    try:
        nextEntry = dbentries[i+1]
    except:
        nextEntry = BeamSpot()
    myxml = dumpXMLPayloadByHash(entry.hash)
    mybs = BeamSpot()
    mybs.ReadXML(myxml)
    
    myiov = IOV()
    myiov.RunFirst  = entry.run
    myiov.RunLast   = entry.run
    myiov.LumiFirst = entry.firstLumi
    myiov.LumiLast  = nextEntry.firstLumi - 1

    mybs.SetIOV(myiov)
        
    mybs.Dump(plFile, 'a')

mypl = Payload(plFile)

mypl.plot('X'         , 247324, savePdf = True)
mypl.plot('Y'         , 247324, savePdf = True)
mypl.plot('Z'         , 247324, savePdf = True)
mypl.plot('sigmaZ'    , 247324, savePdf = True)
mypl.plot('dxdz'      , 247324, savePdf = True)
mypl.plot('dydz'      , 247324, savePdf = True)
mypl.plot('beamWidthX', 247324, savePdf = True)
mypl.plot('beamWidthY', 247324, savePdf = True)
