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
    cmd_user = [ENV_PYTHON, CLIENT_DEPLOY_ENTRY, 'shell', 'hdfs', cluster_name, 'dfs', '-quota', '/user/*']
    cmd_hbase = [ENV_PYTHON, CLIENT_DEPLOY_ENTRY, 'shell', 'hdfs', cluster_name, 'dfs', '-quota', '/hbase']
    for cmd in [cmd_user, cmd_hbase]:
      content = subprocess.check_output(cmd)

      for line in content.strip().split('\n'):
        dir_info = {}
        (dir_info['quota'], dir_info['used_quota'],
         dir_info['remaining_quota'], dir_info['space_quota'],
         dir_info['used_space_quota'], dir_info['remaining_space_quota'],
         dir_info['name']) = line.split()
        # discard prefix '/user/', only keep with user name
        if len(dir_info['name']) > 7:
          dir_info['name'] = dir_info['name'][6:]
        else:
          dir_info['name'] = 'hbase'
        res.append(dir_info)
  except Exception, e:
    if repr(e).find("No such file") == -1:
      return ""
    raise e
  return res

