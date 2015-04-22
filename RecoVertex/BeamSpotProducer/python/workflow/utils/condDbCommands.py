#!/usr/bin/python

from subprocess import PIPE, Popen
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import *

def getLastUploadedIOV(databaseTag, tagName, logger = None, maxIOV = 2e13):
    '''
    This function gets the last uploaded IOV from the condition DB
    to make sure we are not re-running on already considered runs.
        
    Some useful info 
    https://indico.cern.ch/event/377500/session/2/contribution/11/material/slides/0.pdf
    
    Need some more logging.
    Need to address the Global Tag part.
    '''
        
    listIOVCommand = ['conddb', '--nocolors', 'list', 
                      databaseTag, '-L', '%d'%maxIOV]
    
    if logger: logger.info('Getting last IOV for tag: %s' %databaseTag)
    if logger: logger.info(' '.join(listIOVCommand))

    conddb_query = Popen(listIOVCommand, stdout = PIPE, stderr = PIPE)
    out, err = conddb_query.communicate()
    
    # Typical output:
    # 
    # Since              Insertion Time       Payload                                   Object Type
    # -----------------  -------------------  ----------------------------------------  ---------------
    # 1 Lumi     1       2014-02-13 14:31:05  dc69c77dbeae7ebe539367dcc21d36bd12b7c264  BeamSpotObjects
    # 123815 Lumi     1  2014-02-13 14:31:05  dc69c77dbeae7ebe539367dcc21d36bd12b7c264  BeamSpotObjects
    # ...
     
    lastIOV = max( [int(line.rstrip().split()[0]) for line in out.split('\n') 
                                                      if 'Lumi' in line] )
    
    return lastIOV

if __name__ == '__main__':

    lastIOV = getLastUploadedIOV('BeamSpotObjects_2009_LumiBased_SigmaZ_v29_offline',
                                 ' ',
                                 maxIOV = 50)
                                  
    print lastIOV                              



    #dbError = commands.getstatusoutput( listIOVCommand )
    #
    #if logger: logger.info(dbError)
    #
    #if dbError[0] != 0 :
    #    if 'There is no tag or global tag named {TAGNAME} in the database.\
    #       ''.format(TAGNAME= tagName) in dbError[1]:
    #        if logger: logger.warning('Creating a new tag because I got the following '\
    #                                  'error contacting the DB \n%s' %dbError[1])
    #        return 1
    #    else:
    #        if logger: logger.error(error_cant_connect_db(dbError[1]))

    #aCommand = listIOVCommand+' | grep Beam | tail -1 | awk \'{print $1}\''
    #output = commands.getstatusoutput( aCommand )
    ##WARNING when we pass to lumi IOV this should be long long
    #if output[1] == '':
    #  logger.error(error_tag_exist_last_iov_doesnt(tagName, emails = []))
    #goodoutput =  output[1].split('\n')[1]
    #if logger: logger.info('Last IOV from DB = %d' %int(goodoutput))
    #return long(goodoutput)
