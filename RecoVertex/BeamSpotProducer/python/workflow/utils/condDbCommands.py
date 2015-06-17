#!/usr/bin/python

from subprocess import PIPE, Popen
from RecoVertex.BeamSpotProducer.workflow.utils.errorMessages import *
from RecoVertex.BeamSpotProducer.workflow.objects.DBEntryObj  import DBEntry

def getLastUploadedIOV(databaseTag, tagName, logger = None, maxIOV = 2e13):
    '''
    This function gets the last uploaded IOV from the condition DB
    to make sure we are not re-running on already considered runs.
        
    Some useful info 
    https://indico.cern.ch/event/377500/session/2/contribution/11/material/slides/0.pdf
    https://twiki.cern.ch/twiki/bin/view/CMS/CondDBToolMap
    
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

def getListOfUploadedIOV(databaseTag, firstIOV = None, lastIOV = None, maxIOV = 2e13):
    '''
    This function gets returns a list of DBEntry objects.
    firstIOV, lastIOV first and last run to consider 
    '''
    
    # Examples
    # conddb --nocolors list BeamSpotObjects_PCL_byRun_v0_prompt -L 5
    # conddb --nocolors list BeamSpotObjects_PCL_byLumi_v0_prompt -L 5
    
    listIOVCommand = ['conddb', '--nocolors', 'list', 
                      databaseTag, '-L', '%d'%maxIOV]
    
    conddb_query = Popen(listIOVCommand, stdout = PIPE, stderr = PIPE)
    out, err = conddb_query.communicate()

    # do not consider bla bla lines
    toSkip = ['Since', '-----', '','  Run']
    
    # create a container of lines from the stdout
    lines = [line for line in out.split('\n') if line[:5] not in toSkip]
    
    # from string to objects
    dbEntries = [DBEntry(line) for line in lines]
    
    # trim non interesting IOVs
    dbEntries = [dbe for dbe in dbEntries 
                 if dbe.run >= firstIOV and dbe.run <= lastIOV]
    
    return dbEntries

def dumpXMLPayloadByHash(hash):
    '''
    Queries the condDB and returns a string containing the XML payload
    associated to the given hash.
    '''
    # Example
    # conddb dump 6766a5e19c0589612545ab201c264c4f904007db --format xml 

    command = ['conddb', 'dump', hash, '--format', 'xml'] 
    conddb_query = Popen(command, stdout = PIPE, stderr = PIPE)
    out, err = conddb_query.communicate()
    
    # cluean up from what we do not want
    outlines = [line for line in out.split('\n') if not 
                ('ambiguous argument'  in line or
                 'separate paths'      in line or
                 'git'                 in line or
                 'DOCTYPE'             in line or
                 'boost_serialization' in line )]
                 
    outxml = '\n'.join(outlines)
    return outxml

if __name__ == '__main__':

    lastIOV = getLastUploadedIOV('BeamSpotObjects_2009_LumiBased_SigmaZ_v29_offline',
                                 ' ',
                                 maxIOV = 50)
                                  
    print lastIOV                              

    listOfIOVs = getListOfUploadedIOV('BeamSpotObjects_PCL_byLumi_v0_prompt',
                                      246908,
                                      300000)
                                      
    for dbe in listOfIOVs:
        print vars(dbe)
