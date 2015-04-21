#!/usr/bin/python

from RecoVertex.BeamSpotProducer.workflow.objects.BeamSpotWorkflowObj import BeamSpotWorkflow
from optparse import OptionParser
import sys


'''
   BeamSpotWorkflow.py

   A very complicate script to upload the results into the DB

   usage: %prog -d <data file/directory> -t <tag name>
   -c, --cfg = CFGFILE : Use a different configuration file than the default
   -l, --lock = LOCK   : Create a lock file to have just one script running
   -o, --overwrite     : Overwrite results files when copying.
   -T, --Test          : Upload files to Test dropbox for data validation.
   -u, --upload        : Upload files to offline drop box via scp.
   -z, --zlarge        : Enlarge sigmaZ to 10 +/- 0.005 cm.

   N.B. You will need to set up a CRAB environment and initialize a vali grid proxy

'''

parser = OptionParser()
parser.add_option('-d', '--dir'      ,  dest = 'data'     , help = 'Data file/directory'                                                                  )
parser.add_option('-t', '--tag'      ,  dest = 'tag'      , help = 'Global tag name'                                                                      )
parser.add_option('-c', '--cfg'      ,  dest = 'cfg'      , help = 'Use a different configuration file than the default', default = 'bswf_template_cfg.py')
parser.add_option('-l', '--lock'     ,  dest = 'lock'     , help = 'Create a lock file to have just one script running' , action = 'store_true'           )
parser.add_option('-o', '--overwrite',  dest = 'overwrite', help = 'Overwrite results files when copying.'              , action = 'store_true'           )
parser.add_option('-T', '--test'     ,  dest = 'Test'     , help = 'Upload files to Test dropbox for data validation.'  , action = 'store_true'           )
parser.add_option('-u', '--upload'   ,  dest = 'upload'   , help = 'Upload files to offline drop box via scp.'          , action = 'store_true'           )
parser.add_option('-z', '--zlarge'   ,  dest = 'zlarge'   , help = 'Enlarge sigmaZ to 10 +/- 0.005 cm.'                 , action = 'store_true'           )

(options, args) = parser.parse_args()

# imports the cfg, named cfg from here
execfile(options.cfg) 

bswf = BeamSpotWorkflow( 
                         cfg       = cfg              ,
                         lock      = options.lock     ,
                         overwrite = options.overwrite,
                         globaltag = options.tag      ,
                       )

bswf.process()

# here the final Payload file is saved
# the upload and DB interaction part needs to be writen
