# -*- coding: utf-8 -*-

import dbutil
import json
import metric_view_config

# define operation metric suffix
OPERATION_HISTOGRAM_75th_TIME = 'histogram_75th_percentile'
OPERATION_HISTOGRAM_95th_TIME = 'histogram_95th_percentile'
OPERATION_HISTOGRAM_99th_TIME = 'histogram_99th_percentile'
OPERATION_HISTOGRAM_999th_TIME = 'histogram_999th_percentile'
OPERATION_HISTOGRAM_PERCENTILES = [OPERATION_HISTOGRAM_75th_TIME,
                                   OPERATION_HISTOGRAM_95th_TIME,
                                   OPERATION_HISTOGRAM_99th_TIME,
                                   OPERATION_HISTOGRAM_999th_TIME]

def form_perf_counter_endpoint_name(task):
  delimiter = '-'
  endpoint_name = delimiter.join((task.host, str(task.port)))
  return endpoint_name

def form_perf_counter_group_name(task, bean_name):
  return parse_bean_name(bean_name)[0]

def form_percentile_counter_name(endpoint, group, operationName):
  percentiles = []
  for suffix in OPERATION_HISTOGRAM_PERCENTILES:
    percentiles.append(make_latency_metric_query(endpoint, group, '%s_%s' % (operationName, suffix)))
  return percentiles

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

def tsdb_task_metrics_view_config(task):
  result = {}
  service, cluster, job, task = str(task).split('/')
  return metric_view_config.TASK_METRICS_VIEW_CONFIG[service][job]

def tsdb_job_metrics_view_config(job):
  result = {}
  service, cluster, job = str(job).split('/')
  return metric_view_config.JOB_METRICS_VIEW_CONFIG[service][job]

def make_metric_query(endpoint, group, key, unit=""):
  if unit:
    return "&m=sum:%s{host=%s,group=%s}&o=&yformat=%%25.0s%%25c %s" % (key, endpoint, group, unit)
  else:
    return "&m=sum:%s{host=%s,group=%s}&o=" % (key, endpoint, group)

def make_quota_query(cluster_name, user_id, key):
  return "&m=sum:%s{cluster=%s,user_id=%s}&o=" % (key, cluster_name, user_id)

def make_metrics_query_for_task(endpoint, task):
  metrics = []
  task_view_config = task_metrics_view_config(task)
  for view_tag, view_config in task_view_config:
    metrics_view = []
    for graph_config in view_config:
      group, key, unit = graph_config[0]
      graph = {
        'title' : '%s:%s' % (group, key),
        'query' : make_metric_query(endpoint, group, key, unit),
      }
      metrics_view.append(graph)
    metrics.append((view_tag, metrics_view))
  return metrics

def make_metrics_query_for_job(endpoints, job, tasks):
  metrics = []
  task_view_config = job_metrics_view_config(job)
  for view_tag, view_config in task_view_config:
    metrics_view = []
    for graph_config in view_config:
      group, key, unit = graph_config[0]
      metrics_view.append(make_metric_query_graph_for_endpoints(endpoints, group, key, unit))
    metrics.append((view_tag, metrics_view))
  return metrics

def make_metric_query_graph_for_endpoints(endpoints, group, key, unit=""):
  graph = {
    'title' : '%s:%s' % (group, key),
    'query' : [],
  }
  for endpoint in endpoints:
    graph['query'].append(make_metric_query(endpoint, group, key, unit))
  return graph

def get_peer_id_endpoint_map_and_cluster(region_servers):
  peer_id_endpoint_map = {}
  peer_id_cluster = {}
  for region_server in region_servers:
    endpoint = form_perf_counter_endpoint_name(region_server.task)
    replicationMetrics = json.loads(region_server.replicationMetrics)
    for peer_id in replicationMetrics.keys():
      if "peerClusterName" in replicationMetrics[peer_id]:
        peer_id_cluster[peer_id] = replicationMetrics[peer_id]["peerClusterName"]
      peer_id_endpoints = peer_id_endpoint_map.setdefault(peer_id, []) 
      peer_id_endpoints.append(endpoint)
  return (peer_id_endpoint_map, peer_id_cluster)

def make_metrics_query_for_replication(peer_id_endpoint_map, peer_id_cluster_map):
  metrics = []
  for peer_id in peer_id_endpoint_map.keys():
    endpoints = peer_id_endpoint_map[peer_id]
    peer_graphs = []
    for key_and_unit in metric_view_config.REPLICATION_METRICS_VIEW_CONFIG:
      key = key_and_unit[0]
      unit = key_and_unit[1]
      replication_key = key + '-' + peer_id
      peer_graphs.append(make_metric_query_graph_for_endpoints(endpoints, "Replication", replication_key, unit))
    cluster_name = "unknown-cluster"
    if peer_id in peer_id_cluster_map.keys():
      cluster_name = peer_id_cluster_map[peer_id]
    metrics.append((peer_id, cluster_name, peer_graphs))
  return metrics

def make_ops_metric_query(endpoint, group, name):
  return make_metric_query(endpoint, group, name, metric_view_config.DEFAULT_OPS_UNIT)

def make_latency_metric_query(endpoint, group, name):
  return make_metric_query(endpoint, group, name, metric_view_config.DEFAULT_LATENCY_UNIT)

# metrics is an array of counters, where the counter is formatted as :
# [operationName, CounterOfNumOps, CounterOfAvgTime]
def make_operation_metrics(endpoint, record, group):
  metrics = []
  if record.operationMetrics is not None and record.operationMetrics != '':
    operationMetrics = json.loads(record.operationMetrics)
    for operationName in operationMetrics.keys():
      # remove common prefix for 'coprocessor-operation'
      tokens = operationName.split('-')
      operationShowName = tokens[len(tokens) - 1]
      operationCounter = []
      operationCounter.append(operationShowName)

      operationNumOpsName = operationName + '_NumOps'
      numOpsCounter = {}
      numOpsCounter['title'] = operationNumOpsName
      numOpsCounter['query'] = []
      numOpsCounter['query'].append(make_ops_metric_query(endpoint, group, operationNumOpsName))
      operationCounter.append(numOpsCounter)

      operationAvgTimeName = operationName + '_AvgTime'
      avgTimeCounter = {}
      avgTimeCounter['title'] = operationAvgTimeName
      avgTimeCounter['query'] = []
      avgTimeCounter['query'].append(make_latency_metric_query(endpoint, group, operationAvgTimeName))
      operationCounter.append(avgTimeCounter)

      metrics.append(operationCounter)
  return metrics

# [op_name: [{op_num: [table1_op1_avg_query, table2_op1_avg_query]},
#            {op_avg: [table2 op1_ops, table2 op1_num]}],
# ]
def make_operation_metrics_for_tables_in_cluster(cluster):
  # we first read operation metrics for tables of the cluster
  tables = dbutil.get_table_by_cluster(cluster)
  clusterOperationMetrics = json.loads(cluster.hbasecluster.operationMetrics)
  operationCounterNameOfTables = {}
  metrics = {}
  for operationName in clusterOperationMetrics.keys():
    tokens = operationName.split('-')
    operationShowName = tokens[-1]
    numOpsCounterName = '%s_NumOps' % (operationShowName)
    avgTimeCounterName = '%s_AvgTime' % (operationShowName)
    metrics[operationShowName] = [{'title': numOpsCounterName, 'query': []},
                                  {'title': avgTimeCounterName, 'query': []}]  # reserved for num and avg graph

  for table in tables:
    if table.operationMetrics is not None and table.operationMetrics != '':
      tableOperationMetrics = json.loads(table.operationMetrics)
      endpoint = cluster.name
      group = table.name
      for operationName in tableOperationMetrics:
        if operationName not in metrics.keys():
          continue
        numOpsCounterName = '%s_NumOps' % (operationName)
        avgTimeCounterName = '%s_AvgTime' % (operationName)
        print type(endpoint)
        print type(group)
        print type(numOpsCounterName)
        metrics[operationName][0]['query'].append(make_ops_metric_query(endpoint, group, numOpsCounterName))
        metrics[operationName][1]['query'].append(make_latency_metric_query(endpoint, group, avgTimeCounterName))

  return metrics

# metric is an array of counters, where counter is formatted as:
# [operationName, CounterOfNumOps, CountersOfPercentile]
def generate_operation_metric_for_regionserver(regionserver):
  task = regionserver.task
  metric = []
  endpoint = form_perf_counter_endpoint_name(regionserver.task)
  group = 'HBase'
  for operationName in metric_view_config.REGION_SERVER_OPERATION_VIEW_CONFIG:
    counter = []
    # first append operationName
    counter.append(operationName)
    # then, append counter for NumOps
    num_ops_counter = {}
    num_ops_counter['title'] = operationName + '_histogram_num_ops'
    num_ops_counter['query'] = []
    num_ops_counter['query'].append(make_ops_metric_query(endpoint, group, num_ops_counter['title']))
    counter.append(num_ops_counter)

    # lastly, append counters for percentile
    percentile_counter = {}
    percentile_counter['title'] = 'Percentile-Comparision'
    percentile_counter['query'] = form_percentile_counter_name(endpoint, group, operationName)
    counter.append(percentile_counter)

    metric.append(counter)
  return metric
