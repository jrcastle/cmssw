#!/usr/bin/python

from dbs.apis.dbsClient import DbsApi

def setupDbsApi(url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                logger = None):
    ''' 
    Adding api for dbs3 queries.
    Default = https://cmsweb.cern.ch/dbs/prod/global/DBSReader DBS3
    This works only if the crab environment is set.
    '''
    if logger: logger.info('Opening a DBS3 instance %s' %url)
    else     : print 'Opening a DBS3 instance %s' %url
    api = DbsApi(url = url)
    return api

if __name__ == '__main__':
    print 'connected to DBS3'
    api = setupDbsApi()
