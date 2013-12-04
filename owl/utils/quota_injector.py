import logging
import os
import sys
import time

from urlparse import urlparse

root_path = os.path.abspath(
  os.path.dirname(os.path.realpath(__file__))+ '/../..')
opentsdb_path = os.path.join(root_path, 'opentsdb')
sys.path.append(opentsdb_path)
tsdb_register = __import__('tsdb_register')
from tsdb_register import conf_path
from tsdb_register import TsdbRegister

owl_config_path = os.path.join(conf_path, 'owl')
sys.path.append(owl_config_path)
owl_config = __import__('owl_config')
tsdb_host, tsdb_port = urlparse(owl_config.TSDB_ADDR).netloc.split(':')

logger_quota = logging.getLogger('quota')

# the quota items need to calculate the total value
QUOTA_TOTAL_DICT = {
  'used_quota': (int, 0),
  'used_space_quota': (int, 0),
}

class QuotaInjector():
  '''
  Push quota information into opentsdb
  '''
  def __init__(self):
    self.tsdb_register = TsdbRegister()

  def check_quota_new_keys(self, quota_list):
    if len(quota_list) > 0:
      for quota_key in quota_list[0].keys():
        if quota_key not in self.tsdb_register.register_keys and quota_key != 'name':
          self.tsdb_register.new_keys.append(quota_key)
          self.tsdb_register.register_keys.add(quota_key)
      self.tsdb_register.register_new_keys_to_tsdb()

  def push_quota_to_tsdb(self, quota_list, cluster_name):
    self.check_quota_new_keys(quota_list)
    timestamp = int(time.time())

    # reset the quota_total_dict
    quota_total_dict = dict.fromkeys(QUOTA_TOTAL_DICT, 0)

    # push every user's quota to tsdb for cluster_name
    for quota_dict in quota_list:
      for quota_key, quota_value in quota_dict.iteritems():
        if quota_key != 'name':
          if not quota_value.isdigit():
            quota_value = '0'
          quota_record = "%s %d %d user_id=%s cluster=%s" % (
            quota_key, timestamp, int(quota_value), quota_dict['name'], cluster_name)
          put_operation = 'echo put %s | nc -w 10 %s %s' % (quota_record, tsdb_host, tsdb_port)
          logger_quota.info(put_operation)
          os.system(put_operation)
        if quota_key in quota_total_dict.keys():
          quota_total_dict[quota_key] += int(quota_value)

    # push the total values to tsdb
    for quota_key, quota_value in quota_total_dict.iteritems():
      quota_record = "%s %d %d user_id=%s cluster=%s" % (
        quota_key, timestamp, quota_value, quota_key+'_total', cluster_name)
      put_operation = 'echo put %s | nc -w 10 %s %s' % (quota_record, tsdb_host, tsdb_port)
      logger_quota.info(put_operation)
      os.system(put_operation)
