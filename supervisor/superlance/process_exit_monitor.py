#!/usr/bin/env python -u

##############################################################################
#
# This script is subject to the execution of the post script when some process
# has entered the stopped or exited state.
#
##############################################################################

import ConfigParser
import os
import re
import subprocess
import sys

from supervisor import childutils

JOB_INSTANCES_REGEX = re.compile('(?P<job>[A-Za-z_]+)(?P<instance_id>\d+)?$')

def handle_event(payload):
  '''
  Execute the post script when the monitored events happen
  '''
  pheaders, pdata = childutils.eventdata(payload+'\n')
  name_list = pheaders['groupname'].split('--')
  if len(name_list) == 3:
    service, cluster, job = name_list
  else:
    return None

  childutils.pcomm.stderr(childutils.get_asctime()+' Process %(processname)s '
    'in group %(groupname)s exited from state %(from_state)s. '
    'Now execute the post script.\n' % pheaders)

  supervisor_config_path = '%s/../supervisord.conf' % os.path.dirname(__file__)
  if not os.path.exists(supervisor_config_path):
    childutils.pcomm.stderr('Cannot find the config file: supervisord.conf.\n')

  parser = ConfigParser.SafeConfigParser()
  parser.read([supervisor_config_path])

  sys.path.append('%s/../deployment' % os.path.dirname(__file__))
  from rpcinterface import DEFAULT_APP_ROOT
  app_root = parser.get('rpcinterface:deployment', 'app_root', DEFAULT_APP_ROOT)
  reg_expr = JOB_INSTANCES_REGEX.match(job)
  job = reg_expr.group('job')

  if reg_expr.group('instance_id'):
    instance_id = reg_expr.group('instance_id')
    service_root = '%s/%s/%s/%s/%s' % (app_root, service, cluster, job, instance_id)
  else:
    service_root = '%s/%s/%s/%s' % (app_root, service, cluster, job)

  if not os.path.exists('%s/post.sh' % service_root):
    childutils.pcomm.stderr('No post.sh for %s found.\n' % service)
    return None

  cmd = ['/bin/bash', '%s/post.sh' % service_root]
  subprocess.call(cmd)


def main():
  process_state_events = ['PROCESS_STATE_STOPPED', 'PROCESS_STATE_BACKOFF',
    'PROCESS_STATE_EXITED', 'PROCESS_STATE_FATAL']
  while True:
    headers, payload = childutils.listener.wait()

    if headers['eventname'] in process_state_events:
      handle_event(payload)

    childutils.listener.ok()

if __name__ == '__main__':
  main()

