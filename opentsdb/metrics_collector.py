import json
import logging
import logging.config
import os
import sys
import time
import tsdb_register
import urllib

from tsdb_register import collect_period
from tsdb_register import metrics_url
from tsdb_register import opentsdb_bin_path
from tsdb_register import opentsdb_extra_args
from tsdb_register import TsdbRegister

local_data_path = 'metrics_dump.data'

logging.config.fileConfig('metrics_logging.conf')
logger_metrics = logging.getLogger('metrics')

def verify_config():
  if not metrics_url:
    logger_metrics.warning("Please set metrics url")
    return False

  if not opentsdb_bin_path:
    logger_metrics.warning("Please set opentsdb_bin_path")
    return False

  if not collect_period:
    logger_metrics.warning("Please set collect_period")
    return False

  return True

class MetricsCollector():
  def __init__(self):
    self.tsdb_register = TsdbRegister()

  def run(self):
    while True:
      start = time.time()
      self.collect_metrics()
      self.tsdb_register.register_new_keys_to_tsdb()
      self.batch_output_to_tsdb()
      end = time.time()
      to_sleep_time = collect_period - (end - start)
      if to_sleep_time > 0:
        time.sleep(to_sleep_time)

  def collect_metrics(self):
    try:
      out_file = open(local_data_path, 'w')
      json_string = urllib.urlopen(metrics_url).read()
      metrics = json.loads(json_string)
      timestamp = metrics['timestamp']
      for endpoint, group_metrics in metrics['data'].iteritems():
        for group, key_metrics in group_metrics.iteritems():
          for key, metric in key_metrics.iteritems():
            if key.find('#') != -1:
              key = key.replace("#", "_")
            value = metric['value']
            self.append_to_file(out_file, timestamp, key, value, endpoint, group)
            if key not in self.tsdb_register.register_keys:
              self.tsdb_register.new_keys.append(key)
              self.tsdb_register.register_keys.add(key)
      out_file.close()
    except Exception, e:
      logger_metrics.error("collect_metrics exception: %s", e)

  @staticmethod
  def append_to_file(out_file, timestamp, key, value, endpoint, group):
    # format example: metric_key 1288900000 42 host=127.0.0.1-10000 group=Master
    out_file.write("%s %s %s host=%s group=%s\n" % (key, timestamp, value, endpoint, group))


  def batch_output_to_tsdb(self):
    start_time = time.time()
    os.system('%s import %s %s' % (opentsdb_bin_path, opentsdb_extra_args, local_data_path))
    logger_metrics.info("Batch import metrics cost %f secs" % (time.time() - start_time))

if __name__ == '__main__':
  if not verify_config():
    sys.exit(-1)

  collector = MetricsCollector()
  collector.run()
