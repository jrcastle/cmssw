import os
from subprocess import Popen, PIPE


def source(script, update=True):
  '''
  http://pythonwise.blogspot.fr/2010/04/sourcing-shell-script.html (Miki Tebeka)
  '''
  proc = Popen('. %s; env -0' % script, stdout=PIPE, shell=True)
  output = proc.communicate()[0]
  env = dict((line.split('=', 1) for line in output.split('\x00') if line))
  if update:
      os.environ.update(env)
  return env

def initCrab(crab_init_script = '/afs/cern.ch/cms/ccs/wm/scripts/Crab/crab.sh'):

  print 'initialising Crab environment'

  shell = os.getenv('SHELL')
  source(crab_init_script)
  return source(crab_init_script)

if __name__ == '__main__':
  initCrab()
