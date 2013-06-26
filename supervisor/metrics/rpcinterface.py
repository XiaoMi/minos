#!/usr/bin/env python
#
# Copyright (c) 2012, xiaomi.com.
# Author:  Wu Zesheng <wuzesheng@xiaomi.com>

import os
import subprocess
import tempfile

FORMAT_OPTION_MAP = {
  'csv': '-d',
  'json': '-j',
  'xml': '-x',
  'text': '-p',
}

class SystemMetricsRPCInterface:
  def __init__(self, supervisord, format='json'):
    self.supervisord = supervisord
    self.format_option = FORMAT_OPTION_MAP[format]

  def get_cpu_metrics(self, format=''):
    if format:
      self.format_option = FORMAT_OPTION_MAP[format]
    return self._get_metrics('-u -P ALL 1 1')

  def get_load_avg_metrics(self, format=''):
    if format:
      self.format_option = FORMAT_OPTION_MAP[format]
    return self._get_metrics('-q 1 1')

  def get_memory_metrics(self, format=''):
    if format:
      self.format_option = FORMAT_OPTION_MAP[format]
    return self._get_metrics('-r 1 1')

  def get_disk_metrics(self, format=''):
    if format:
      self.format_option = FORMAT_OPTION_MAP[format]
    return self._get_metrics('-d -p 1 1')

  def get_network_metrics(self, format=''):
    if format:
      self.format_option = FORMAT_OPTION_MAP[format]
    return self._get_metrics('-n DEV 1 1')

  def get_all_metrics(self, format=''):
    if format:
      self.format_option = FORMAT_OPTION_MAP[format]
    return self._get_metrics('-uqrdp -n DEV -P ALL 1 1')

  def _get_metrics(self, metric_options):
    result = str()
    fd, file = tempfile.mkstemp()
    try:
      os.close(fd)

      cmd = ['sar', '-o', file] + metric_options.split()
      subprocess.check_call(cmd)

      cmd = ['sadf', self.format_option, file, '--'] + metric_options.split()
      result = subprocess.check_output(cmd)
    finally:
      os.unlink(file)
    return result

def system_metrics_rpcinterface(supervisord, **config):
  format = config.get('format', 'json')
  return SystemMetricsRPCInterface(supervisord, format)

def test():
  # Test JSON format cpu/loadavg/memory/disk/network metrics
  rpc_interface = SystemMetricsRPCInterface('')
  print '==cpu metrics=='
  print rpc_interface.get_cpu_metrics()
  print '==load-avg metrics=='
  print rpc_interface.get_load_avg_metrics()
  print '==memory metrics=='
  print rpc_interface.get_memory_metrics()
  print '==disk metrics=='
  print rpc_interface.get_disk_metrics()
  print '==network metrics=='
  print rpc_interface.get_network_metrics()
  print '==all metrics=='
  print rpc_interface.get_all_metrics()

if __name__ == '__main__':
  test()
