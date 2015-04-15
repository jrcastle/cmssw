#!/usr/bin/python

'''
Add here the error messages.
Helps keeping the scripts clean.

'''

from errorBase import exception, warning, error, critical


class error_crab(error):
  def _msg(self):
    msg = 'Please set a crab environment in order to get the proper JSON lib'
    return msg


class error_lumi_range(error):
  def _msg(self, run, line, runListDir, fileName):
    msg = 'The lumi range is greater than 1 for run \
{RUN} {LINE} in file: \
{RUNLISTDIR}{FILENAME}'.format(RUN        = str(run)  ,
                               LINE       = line      ,
                               RUNLISTDIR = runListDir,
                               FILENAME   = fileName  )
    return msg


class error_run_not_in_DBS(error):
  def _msg(self, run):
    msg = 'Impossible but run {RUN} has been processed and it is also in the \
run registry but it is not in DBS! Exit.'.format(RUN = str(run))
    return msg


class error_timeout(error):
  def _msg(self, runsAndFiles, missingLumisTimeout, run):
    myRunsAndFiles = runsAndFiles[run]
    msg = 'I previously set a timeout that expired... \
I can\'t continue with the script because there are too many \
({MISSFILES} files missing) and for too long {HOURS} hours! \
I will process anyway the runs before this one \
({RUN})'.format(MISSFILES = str(nFiles - len(myRunsAndFiles)),
                HOURS     = str(missingLumisTimeout/3600)    ,
                RUN       = str(run)                         )
    return msg


class error_out_of_tolerance(error):
  def _msg(self, run, lenA, lenB, dbsTolerancePercent, dbsTolerance):
    msg = 'For run {RUN} I didn\'t process {PERCENT}% of the lumis and \
I am not within the {TOLERANCEPERCENT}% set in the configuration. \
The number of lumis that I didn\'t process ({LENA} out of \
{LENB}) is greater also than the {TOLERANCE} lumis that \
I can tolerate. I can\'t process runs >= {RUN} but I\'ll process \
the runs before!'.format(RUN              = str(run)                ,
                         PERCENT          = str(100.*lenA/lenB)     ,
                         TOLERANCEPERCENT = str(dbsTolerancePercent),
                         LENA             = str(lenA)               ,
                         LENB             = str(lenB)               ,
                         TOLERANCE        = str(dbsTolerance)       )
    return msg


class error_run_not_in_rr(critical):
  def _msg(self, run):
    msg = 'Run {RUN} is in the run registry but it has \
not been processed yet!'.format(RUN = run)
    return msg


class warning_missing_small_run(warning):
  def _msg(self, run, RRList, rrTolerance):
    msg = 'I previously set the MISSING_RUNREGRUN_Run{RUN} \
timeout that expired... I am missing run {RUN} but it only had {RRLIST} <= \
{RRTOLERANCE} lumis. So I will continue and ignore \
it... '.format(RUN         = str(run)        ,
               RRLIST      = str(len(RRList)),
               RRTOLERANCE = str(rrTolerance))
    return msg


class error_missing_large_run(critical):
  def _msg(self, run, RRList, rrTolerance):
    msg = 'I previously set the MISSING_RUNREGRUN_Run{RUN} timeout \
that expired...I am missing run {RUN} which has {RRLIST} > \
{RRTOLERANCE} lumis. I can\'t continue but I\'ll process the runs \
before this one'.format(RUN         = str(run)        ,
                        RRLIST      = str(len(RRList)),
                        RRTOLERANCE = str(rrTolerance))
    return msg


class error_source_dir(error):
  def _msg(self, sourceDir):
    msg = 'The source directory {SOURCEDIR} doesn\'t \
exist!'.format(SOURCEDIR = sourceDir)
    return msg


class error_failed_copy(error):
  def _msg(self, copiedFiles, newProcessedFileList):
    msg = 'I can\'t copy more than {COPIEDFILES} files out of \
{ALLFILES}'.format(COPIEDFILES = str(len(copiedFiles))          ,
                   ALLFILES    = str(len(newProcessedFileList)) )
    return msg


class error_failed_copy_dirs(error):
  def _msg(self, copiedFiles, selectedFilesToProcess, archiveDir, workingDir):
    msg = 'I can\'t copy more than {COPIEDFILES} files out of \
{ALLFILES} from {ARCHIVEDIR} to \
{WORKDIR}'.format(COPIEDFILES = str(len(copiedFiles))           ,
                  ALLFILES    = str(len(selectedFilesToProcess)),
                  ARCHIVEDIR  = archiveDir                      ,
                  WORKDIR     = workingDir                      )
    return msg


class warning_no_valid_fit(warning):
  def _msg(self):
    msg = 'None of the processed and copied payloads has a valid \
fit so there are no results. This shouldn\'t happen since we are \
filtering using the run register, \
so there should be at least one good run.'
    return msg


class warning_unable_to_create_payload(warning):
  def _msg(self):
    msg = 'I wasn\'t able to create any payload even \
if I have some BeamSpot objects.'
    return msg


class error_sql_write_failed(error):
  def _msg(self, tmpSqliteFileName):
    msg = 'An error occurred while writing the sqlite file: \
{SQLITE}'.format(SQLITE = tmpSqliteFileName)
    return msg


class error_iov_not_implemented(error):
  def _msg(self, dbIOVBase):
    msg = 'IOV {DBIOVBASE} still not \
implemented.'.format(DBIOVBASE = dbIOVBase)
    return msg


class error_iov_unrecognised(error):
  def _msg(self, dbIOVBase):
    msg = 'IOV {DBIOVBASE} unrecognised!'.format(DBIOVBASE = dbIOVBase)
    return msg


class error_tag_exist_last_iov_doesnt(critical):
  def _msg(self, tagName):
    msg = 'The tag {TAGNAME} exists but I can\'t \
get the value of the last IOV'.format(TAGNAME= self.tagName)
    return msg


class error_cant_connect_db(error):
  def _msg(self, dbError):
    msg = 'Can\'t connect to db because:\n' + dbError
    return msg


class warning_setting_dbs_mismatch_timeout(warning):
  def _msg(self, run):
    msg = 'Setting the DBS_MISMATCH_Run{RUN} \
timeout because I haven\'t processed all files!'.format(RUN = str(RUN))
    return msg


class warning_dbs_mismatch_timeout_progress(warning):
  def _msg(self, run):
    msg = 'Timeout DBS_MISMATCH_Run{RUN} \
is in progress.'.format(RUN = str(run))
    return msg




def initLoggerForTesting(filename = 'errors_test.log', mode = 'w+',
                         formatter = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'):

  from logging          import getLogger, FileHandler, StreamHandler, Formatter
  from logging          import DEBUG, INFO, CRITICAL, WARNING, ERROR
  from logging.handlers import SMTPHandler

  from getpass import getuser
  from socket  import gethostname

  user, host = getuser(), gethostname()
  msg = 'BeamSpot worflow critical error'  
    
  logger = getLogger('beamsporWorkflowTest')
  logger.setLevel(DEBUG)

  fh = FileHandler(filename = filename, mode = mode)
  ch = StreamHandler()
  mh = SMTPHandler(mailhost    = host               ,
                   fromaddr    = user + '@cern.ch'  ,
                   toaddrs     = ['manzoni@cern.ch'],
                   subject     = msg                ,
                   credentials = None               ,
                   secure      = None               )

  fh.setLevel(INFO)
  ch.setLevel(WARNING)
  mh.setLevel(CRITICAL)

  format = Formatter(formatter)

  fh.setFormatter(format)
  ch.setFormatter(format)
  mh.setFormatter(format)

  logger.addHandler(fh)
  logger.addHandler(ch)
  logger.addHandler(mh)
  
  return logger


if __name__ == '__main__':
  
  logger = initLoggerForTesting()
  
  #warning_unable_to_create_payload(logger)
  error_tag_exist_last_iov_doesnt(logger, tagName='mytag')

#   error(logger, emails = ['manzoni@cern.ch'])
#   error_limi_range(logger,
#                    192168,
#                    'linea del cacchio',
#                    'runListDir',
#                    'fileName',
#                    emails = ['manzoni@cern.ch', 'manzoni.riccardo@gmail.com'],
#                    exit = False)
#   error_iov_unrecognised(logger, 'IOVBaseRiccardo', emails = ['sara.fiorendi@cern.ch'])
