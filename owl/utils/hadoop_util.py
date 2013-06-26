# -*- coding: utf-8 -*-
import logging
import subprocess
import os

deploy_root = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + "../../../")
client_root = os.path.join(deploy_root, 'client')

logger = logging.getLogger('quota')
def get_quota_summary(cluster_name):
  res = []
  try:
    # call with python2.7 explicitly because on centos python link to python2.6
    cmd = ["python2.7", "%s/deploy.py" % client_root, 'shell', 'hdfs', cluster_name, 'dfs', '-quota', '/user/*']
    content = subprocess.check_output(cmd)
    for line in content.strip().split('\n'):
      dir_info = {}
      (dir_info['quota'], dir_info['used_quota'],
       dir_info['remaining_quota'], dir_info['space_quota'],
       dir_info['used_space_quota'], dir_info['remaining_space_quota'],
       dir_info['name']) = line.split()
      # discard prefix '/user/', only keep with user name
      dir_info['name'] = dir_info['name'][6:]
      res.append(dir_info)
  except Exception, e:
    logger.error("get_quota_summary exception: %s\nline conent: %s",e, line)
  return res

