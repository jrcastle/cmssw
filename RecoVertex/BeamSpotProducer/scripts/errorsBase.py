#!/usr/bin/python

'''
Add here the error messages.
Helps keeping the scripts clean.

'''

class exception(object):
  '''
  Base class that sends an email to the relevant people with an error message.
  By default terminates the scriptexecution.
  The particular, the error message has to be implemented in child classes
  '''
  # it seems that we cannot use python 3 as of now, as it's not available in CMSSW
  # thus this trick http://stackoverflow.com/questions/5940180/python-default-keyword-arguments-after-variable-length-positional-arguments
  def __init__(self, logger, *args, **kwargs):
    # this is nothing but setting default arguments without breaking
    # positional call at all
    self.logger = logger
    self.emails = kwargs.pop('emails', [])
    self.exit   = kwargs.pop('exit', False)
    
    error = self._msg(*args, **kwargs)
    
    self._callLogger(error)
    self._sendEmail(error)
         
    if self.exit:
      exit(0)

  def _sendEmail(self, msg):
    for email in self.emails:
      sendEmail(email, msg)
    
  def _callLogger(self, msg):
    self.logger.info(msg)

  def _msg(self, *args, **kwargs):
    msg = 'Exiting!'
    return msg


class error(exception):
  def __init__(self, logger, *args, **kwargs):
    try:
      exit = kwargs['exit']
      super(error, self).__init__(logger, *args, **kwargs)
    except:
      super(error, self).__init__(logger, exit = True, *args, **kwargs)

  def _callLogger(self, msg):
    self.logger.error(msg)
        

class warning(exception):

  def __init__(self, *args, **kwargs):
    try:
      exit = kwargs['exit']
      super(warning, self).__init__(logger, *args, **kwargs)
    except:
      super(warning, self).__init__(logger, exit = False, *args, **kwargs)

  def _callLogger(self, msg):
    self.logger.warning(msg)


class critical(error):
  def _callLogger(self, msg):
    self.logger.critical(msg)


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
  
  exception(logger)
  warning  (logger)
  error    (logger, exit = False)
  critical (logger, exit = False)
