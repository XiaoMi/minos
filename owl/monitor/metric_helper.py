# -*- coding: utf-8 -*-

import metric_view_config

def form_perf_counter_endpoint_name(task):
  delimiter = '-'
  endpoint_name = delimiter.join((task.host, str(task.port)))
  return endpoint_name

def form_perf_counter_group_name(task, bean_name):
  return parse_bean_name(bean_name)[0]

# parse bean name
# return (service, name)
# eg:
#   input 'hadoop:service=HBase,name=RPCStatistics-18600'
#   return ('HBase', 'RPCStatistics-18600')
def parse_bean_name(bean_name):
  items= bean_name.split(':')[1].split(',')[:2]
  return [item.split('=')[1] for item in items]

# input:
# 'ReplicationSource for 5-10.0.4.172%2C11600%2C1364508024855'
# return 5-bak
# input:
# 'ReplicationSource for 5'
# return 5
def parse_replication_source(name):
  fields = name.split('-')
  source_name = fields[0]
  try:
    source_num = source_name.split(' ')[2]
    if len(fields) > 1:
      return source_num + '-bak'
    else:
      return source_num
  except:
    return source_name

def form_perf_counter_key_name(bean_name, metric_name):
  # illegal perf counter char '~' exsit in hbase table metric.
  # replace it with '-'
  # eg:tbl.miliao_summary.cf.S~T.multiput_AvgTime
  #    to tbl.miliao_summary.cf.S-T.multiput_AvgTime
  service, name = parse_bean_name(bean_name)
  if service == 'Replication':
    replication_src = parse_replication_source(name)
    metric_name += '-' + replication_src
  return metric_name.replace('~', '-')

def task_metrics_view_config(task):
  result = {}
  service, cluster, job, task = str(task).split('/')
  return metric_view_config.TASK_METRICS_VIEW_CONFIG[service][job]

def job_metrics_view_config(job):
  result = {}
  service, cluster, job = str(job).split('/')
  return metric_view_config.JOB_METRICS_VIEW_CONFIG[service][job]

def get_all_metrics_config():
  inputs = (metric_view_config.JOB_METRICS_VIEW_CONFIG,
            metric_view_config.TASK_METRICS_VIEW_CONFIG,
           )

  metric_set = set()

  for input in inputs:
    for job_name, tasks in input.iteritems():
      for task, task_configs in tasks.iteritems():
        for view, view_config in  task_configs:
          for graph in view_config:
            for metric in graph:
              metric_set.add(metric)

  return list(metric_set)
