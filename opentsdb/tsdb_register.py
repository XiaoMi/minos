import logging
import os
import sys
import time

root_path = os.path.abspath(
  os.path.dirname(os.path.realpath(__file__))+ '/..')

client_path = os.path.join(root_path, 'client')
sys.path.append(client_path)

deploy_utils = __import__('deploy_utils')
conf_path = deploy_utils.get_config_dir()

opentsdb_config_path = os.path.join(conf_path, 'opentsdb')
sys.path.append(opentsdb_config_path)
metrics_collector_config = __import__('metrics_collector_config')

metrics_url = metrics_collector_config.metrics_url
opentsdb_bin_path = metrics_collector_config.opentsdb_bin_path
opentsdb_extra_args = metrics_collector_config.opentsdb_extra_args
collect_period = metrics_collector_config.collect_period

logger_metrics = logging.getLogger('metrics')
logger_quota = logging.getLogger('quota')


class TsdbRegister:
  def __init__(self):
    self.new_keys = []
    self.register_keys = set()

  def register_new_keys_to_tsdb(self):
    start_time = time.time()

    size = len(self.new_keys)
    offset = 0
    MAX_REGISTERED_KEYS = 1000;

    # register MAX_REGISTERED_KEYS one time
    while size - offset >= MAX_REGISTERED_KEYS:
      keys_to_add = self.new_keys[offset:offset+MAX_REGISTERED_KEYS]
      mkmetric_operation = '%s mkmetric %s %s' % (opentsdb_bin_path, opentsdb_extra_args, ' '.join(keys_to_add))
      logger_metrics.info(mkmetric_operation)
      logger_quota.info(mkmetric_operation)
      os.system(mkmetric_operation)
      offset += MAX_REGISTERED_KEYS

    # register remainings
    if offset < size:
      keys_to_add = self.new_keys[offset:]
      mkmetric_operation = '%s mkmetric %s %s' % (opentsdb_bin_path, opentsdb_extra_args, ' '.join(keys_to_add))
      logger_metrics.info(mkmetric_operation)
      logger_quota.info(mkmetric_operation)
      os.system(mkmetric_operation)

    self.new_keys = []
    registered_metrics_log = "Registered %d metrics cost %f secs" % (size, time.time() - start_time)
    logger_metrics.info(registered_metrics_log)
    logger_quota.info(registered_metrics_log)
