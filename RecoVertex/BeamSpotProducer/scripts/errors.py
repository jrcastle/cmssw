#!/usr/bin/python

'''
Add here the error messages.
Helps keeping the scripts clean.
'''

from CommonMethods import sendEmail
from CommonMethods import exit as exit_msg


class error(object):
  '''
  Base class that sends an email to the relevant people with an error message.
  The particular error message has to be implemented in child classes
  '''
  # it seems that we cannot use python 3 as of now, as it's not available in CMSSW
  # thus this trick http://stackoverflow.com/questions/5940180/python-default-keyword-arguments-after-variable-length-positional-arguments
  def __init__(self, *args, **kwargs):
    # this is nothing but setting default arguments without breaking
    # positional call at all
    self.emails = kwargs.pop('emails', ['manzoni@cern.ch'])
    self.exit   = kwargs.pop('exit', True)
    self.error = self._msg(*args, **kwargs)
    self._sendEmail()
    if self.exit:
      exit_msg(self.error)
    else:
      print self.error

  def _sendEmail(self):
    for email in self.emails:
      sendEmail(email, self.error)

  def _msg(self, *args, **kwargs):
    msg = 'ERROR: Exiting!'
    return msg


class warning(error):
  def __init__(self, *args, **kwargs):
    super(warning, self).__init__(exit = False)


class error_crab(error):
  def _msg(self):
    msg = 'Please set a crab environment in order to get the proper JSON lib'
    return msg


class error_limi_range(error):
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
    msg = 'ERROR: I previously set a timeout that expired... \
I can\'t continue with the script because there are too many \
({MISSFILES} files missing) and for too long {HOURS} hours! \
I will process anyway the runs before this one \
({RUN})'.format(MISSFILES = str(nFiles - len(myRunsAndFiles)),
                HOURS     = str(missingLumisTimeout/3600)    ,
                RUN       = str(run)                         )
    return msg


class error_out_of_tolerance(error):
  def _msg(self, run, lenA, lenB, dbsTolerancePercent, dbsTolerance):
    msg = 'ERROR: For run {RUN} I didn\'t process {PERCENT}% of the lumis and \
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


class error_run_not_in_rr(error):
  def _msg(self, run):
    msg = 'Run {RUN} is in the run registry but it has \
not been processed yet!'.format(RUN = run)
    return msg


class warning_missing_small_run(warning):
  def _msg(self, run, RRList, rrTolerance):
    msg = 'WARNING: I previously set the MISSING_RUNREGRUN_Run{RUN} \
timeout that expired... I am missing run {RUN} but it only had {RRLIST} <= \
{RRTOLERANCE} lumis. So I will continue and ignore \
it... '.format(RUN         = str(run)        ,
               RRLIST      = str(len(RRList)),
               RRTOLERANCE = str(rrTolerance))
    return msg


class error_missing_large_run(error):
  def _msg(self, run, RRList, rrTolerance):
    msg = 'ERROR: I previously set the MISSING_RUNREGRUN_Run{RUN} timeout \
that expired...I am missing run {RUN} which has {RRLIST} > \
{RRTOLERANCE} lumis. I can\'t continue but I\'ll process the runs \
before this one'.format(RUN         = str(run)        ,
                        RRLIST      = str(len(RRList)),
                        RRTOLERANCE = str(rrTolerance))
    return msg


class error_source_dir(error):
  def _msg(self, sourceDir):
    msg = 'ERROR: The source directory {SOURCEDIR} doesn\'t \
exist!'.format(SOURCEDIR = sourceDir)
    return msg


class error_failed_copy(error):
  def _msg(self, copiedFiles, newProcessedFileList):
    msg = 'ERROR: I can\'t copy more than {COPIEDFILES} files out of \
{ALLFILES}'.format(COPIEDFILES = str(len(copiedFiles))          ,
                   ALLFILES    = str(len(newProcessedFileList)) )
    return msg


class error_failed_copy_dirs(error):
  def _msg(self, copiedFiles, selectedFilesToProcess, archiveDir, workingDir):
    msg = 'ERROR: I can\'t copy more than {COPIEDFILES} files out of \
{ALLFILES} from {ARCHIVEDIR} to \
{WORKDIR}'.format(COPIEDFILES = str(len(copiedFiles))           ,
                  ALLFILES    = str(len(selectedFilesToProcess)),
                  ARCHIVEDIR  = archiveDir                      ,
                  WORKDIR     = workingDir                      )
    return msg


class warning_no_valid_fit(warning):
  def _msg(self):
    msg = 'WARNING: None of the processed and copied payloads has a valid \
fit so there are no results. This shouldn\'t happen since we are \
filtering using the run register, \
so there should be at least one good run.'
    return msg


class warning_unable_to_create_payload(warning):
  def _msg(self):
    msg = 'WARNING: I wasn\'t able to create any payload even \
if I have some BeamSpot objects.'
    return msg


class error_sql_write_failed(error):
  def _msg(self, tmpSqliteFileName):
    msg = 'An error occurred while writing the sqlite file: \
{SQLITE}'.format(SQLITE = tmpSqliteFileName)
    return msg


class error_iov_not_implemented(error):
  def _msg(self, dbIOVBase):
    msg = 'ERROR: IOV {DBIOVBASE} still not \
implemented.'.format(DBIOVBASE = dbIOVBase)
    return msg


class error_iov_unrecognised(error):
  def _msg(self, dbIOVBase):
    msg = 'ERROR: IOV {DBIOVBASE} unrecognised!'.format(DBIOVBASE = dbIOVBase)
    return msg


if __name__ == '__main__':
#   error(emails = ['manzoni@cern.ch'], exit = False)
#   error_limi_range(192168,
#                    'linea del cacchio',
#                    'runListDir',
#                    'fileName',
#                    emails = ['manzoni@cern.ch', 'manzoni.riccardo@gmail.com'],
#                    exit = False)
#   error_iov_unrecognised('IOVBaseRiccardo', emails = ['sara.fiorendi@cern.ch'])
  warning_unable_to_create_payload()
