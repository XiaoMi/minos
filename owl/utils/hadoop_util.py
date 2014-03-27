# -*- coding: utf-8 -*-
import logging
import subprocess
import os

CLIENT_DEPLOY_ENTRY = os.getenv("CLIENT_DEPLOY_ENTRY")
ENV_PYTHON = os.getenv("ENV_PYTHON")

logger = logging.getLogger('quota')
def get_quota_summary(cluster_name):
  res = []
  try:
    cmd = [ENV_PYTHON, CLIENT_DEPLOY_ENTRY, 'shell', 'hdfs', cluster_name, 'dfs', '-quota', '/user/*']
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

