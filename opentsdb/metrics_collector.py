import json
import os
import sys
import time
import urllib

root_path = os.path.abspath(
  os.path.dirname(os.path.realpath(__file__))+ '/..')

client_path = os.path.join(root_path, 'client')
sys.path.append(client_path)

deploy_utils = __import__('deploy_utils')
conf_path = deploy_utils.get_config_dir()

owl_conf_path = os.path.join(conf_path, 'opentsdb')
sys.path.append(owl_conf_path)

metrics_collector_config = __import__('metrics_collector_config')

metrics_url = metrics_collector_config.metrics_url
opentsdb_bin_path = metrics_collector_config.opentsdb_bin_path
collect_period = metrics_collector_config.collect_period
local_data_path = 'metrics_dump.data'

def verify_config():
  if not metrics_url:
    print "Please set metrics url"
    return False

  if not opentsdb_bin_path:
    print "Please set opentsdb_bin_path"
    return False

  if not collect_period:
    print "Please set collect_period"
    return False

  return True

class MetricsCollector:
  def __init__(self):
    self.new_keys = []
    self.register_keys = set()

  def run(self):
    while True:
      start = time.time()
      self.collect_metrics()
      self.register_new_keys_to_tsdb()
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
            value = metric['value']
            self.append_to_file(out_file, timestamp, key, value, endpoint, group)
            if key not in self.register_keys:
              self.new_keys.append(key)
              self.register_keys.add(key)
      out_file.close()
    except Exception, e:
      print e

  @staticmethod
  def append_to_file(out_file, timestamp, key, value, endpoint, group):
    # format example: metric_key 1288900000 42 host=127.0.0.1-10000 group=Master
    out_file.write("%s %s %s host=%s group=%s\n" % (key, timestamp, value, endpoint, group))

  def register_new_keys_to_tsdb(self):
    start_time = time.time()

    size = len(self.new_keys)
    offset = 0
    MAX_REGISTERED_KEYS = 1000;

    # register MAX_REGISTERED_KEYS one time
    while size - offset >= MAX_REGISTERED_KEYS:
      keys_to_add = self.new_keys[offset:offset+MAX_REGISTERED_KEYS]
      print '%s mkmetric %s' % (opentsdb_bin_path, ' '.join(keys_to_add))
      os.system('%s mkmetric %s' % (opentsdb_bin_path, ' '.join(keys_to_add)))
      offset += MAX_REGISTERED_KEYS

    # register remainings
    if offset < size:
      keys_to_add = self.new_keys[offset:]
      print '%s mkmetric %s' % (opentsdb_bin_path, ' '.join(keys_to_add))
      os.system('%s mkmetric %s' % (opentsdb_bin_path, ' '.join(keys_to_add)))

    self.new_keys = []
    print "Registered %d metrics cost %f secs" % (size, time.time() - start_time)

  def batch_output_to_tsdb(self):
    start_time = time.time()
    os.system('%s import %s' % (opentsdb_bin_path, local_data_path))
    print "Batch import metrics cost %f secs" % (time.time() - start_time)

if __name__ == '__main__':
  if not verify_config():
    sys.exit(-1)

  collector = MetricsCollector()
  collector.run()
