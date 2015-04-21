#!/usr/bin/python

from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import *

def getLastUploadedIOV(logger, databaseTag, tagName):
    '''
    This function gets the last uploaded IOV from the conditional DB
    to make sure we are not re-running on already considered runs.
    
    RIC: needs to be revised. FIXME!
    '''
    logger.info('Getting last IOV for tag: ' + databaseTag)
    
    # FIXME! python 'commands' are deprecated, use subprocess
    listIOVCommand = 'conddb --nocolor list {TAGNAME} -L 2000000000000'\
                     ''.format(TAGNAME = tagName)
    dbError = commands.getstatusoutput( listIOVCommand )
    
    logger.info(dbError)
    
    if dbError[0] != 0 :
        if 'metadata entry {TAGNAME} does not exist'.format(TAGNAME= tagName) in dbError[1]:
            logger.warning('Creating a new tag because I got the following '\
                                'error contacting the DB \n%s' %dbError[1])
            return 1
        else:
            logger.error(error_cant_connect_db(dbError[1]))

    aCommand = listIOVCommand+' | grep Beam | tail -1 | awk \'{print $1}\''
    output = commands.getstatusoutput( aCommand )
    #WARNING when we pass to lumi IOV this should be long long
    if output[1] == '':
      logger.error(error_tag_exist_last_iov_doesnt(tagName, emails = []))
    goodoutput =  output[1].split('\n')[1]
    logger.info('Last IOV from DB = %d' %int(goodoutput))
    return long(goodoutput)

if __name__ == '__main__':
    pass