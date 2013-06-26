# -*- coding: utf-8 -*-
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from django.shortcuts import render_to_response, redirect
from django.template import RequestContext, Context, loader
from django.utils import timezone
from django.http import HttpResponse
from django.db import transaction
from utils.quota_util import QuotaUpdater

import datetime
import dbutil
import json
import metric_helper
import time
import owl_config

class Namespace:
  def __init__(self, **kwargs):
    for name, value in kwargs.iteritems():
      setattr(self, name, value)


def index(request):
  # show all cluster
  clusters = dbutil.get_clusters_by_service()
  service = Namespace(name="all services")
  params = {
    'service': service,
    'clusters': clusters,
  }
  return respond(request, 'monitor/service.html', params)


#url: /service/$id/
def show_service(request, id):
  service = dbutil.get_service(id)
  clusters = dbutil.get_clusters_by_service(id)
  params = {
    'service': service,
    'clusters': clusters,
  }
  if service.name == 'hbase':
    metrics = {}
    metrics['endpoint'] = [dbutil.map_cluster_to_endpoint(cluster.name) for cluster in clusters]
    params['metrics'] = metrics
    return respond(request, 'monitor/hbase_service.html', params)
  else:
    return respond(request, 'monitor/service.html', params)

#url: /cluster/$id/
def show_cluster(request, id):
  # return task board by default
  return redirect('/monitor/cluster/%s/task/' % id)

#url: /cluster/$id/task/
def show_cluster_task_board(request, id):
  cluster = dbutil.get_cluster(id)
  tasks = dbutil.get_tasks_by_cluster(id)
  params = {'cluster': cluster,
            'tasks': tasks}
  if cluster.service.name == 'hdfs':
    return respond(request, 'monitor/hdfs_task_board.html', params)
  elif cluster.service.name == 'hbase':
    return respond(request, 'monitor/hbase_task_board.html', params)
  else:
    return respond(request, 'monitor/cluster.html', params)

#url: /cluster/$id/user/
def show_cluster_user_board(request, id):
  cluster = dbutil.get_cluster(id)
  if cluster.service.name == 'hdfs':
    return show_hdfs_user_board(request, cluster);
    # return empty paget for unsupported service
  return HttpResponse('')


def show_hdfs_user_board(request, cluster):
  if 'refresh' in request.GET:
    quota_updater = QuotaUpdater()
    quota_updater.update_cluster(cluster)
    return redirect('/monitor/cluster/%s/user/' % cluster.id)

  dirs = dbutil.get_quota_summary(cluster)
  params = {
    'cluster': cluster,
    'dirs': dirs,
  }
  return respond(request, 'monitor/hdfs_user_board.html', params)

#url: /cluster/$id/table/
def show_cluster_table_board(request, id):
  cluster = dbutil.get_cluster(id)
  if cluster.service.name != 'hbase':
    # return empty paget for unsupported service
    return HttpResponse('')
  read_requests_dist_by_table, write_requests_dist_by_table = dbutil.get_requests_distribution_groupby(cluster, 'table');
  params = {
    'chart_id': 'read_requests_on_table',
    'chart_title': 'read requests on table',
    'request_dist': read_requests_dist_by_table,
    'base_url': '/monitor/table/',
  }

  read_requests_dist_by_table_chart = loader.get_template('monitor/requests_dist_pie_chart.tpl').render(Context(params))

  params = {
    'chart_id': 'write_requests_on_table',
    'chart_title': 'write requests on table',
    'request_dist': write_requests_dist_by_table,
    'base_url': '/monitor/table/',
  }
  write_requests_dist_by_table_chart = loader.get_template('monitor/requests_dist_pie_chart.tpl').render(
    Context(params))

  tables = dbutil.get_items_on_cluster(cluster, 'table', order_by='-qps')
  system_tables = [table for table in tables if is_system_table(table)]
  user_tables = [table for table in tables if not is_system_table(table)]
  table_read_item_keys = '|'.join(['%s-readRequestsCountPerSec' % (table.name) for table in user_tables])
  table_write_item_keys ='|'.join(['%s-writeRequestsCountPerSec' % (table.name) for table in user_tables])
  params = {
    'cluster': cluster,
    'read_requests_dist_by_table_chart': read_requests_dist_by_table_chart,
    'write_requests_dist_by_table_chart': write_requests_dist_by_table_chart,
    'system_tables': system_tables,
    'user_tables': user_tables,
    'table_read_item_keys': table_read_item_keys,
    'table_write_item_keys': table_write_item_keys,
  }
  return respond(request, 'monitor/hbase_table_board.html', params)

def is_system_table(table):
  system_table_names = ('-ROOT-', '.META.', '_acl_')
  return table.name in system_table_names

#url: /cluster/$id/basic/
def show_cluster_basic_board(request, id):
  cluster = dbutil.get_cluster(id)
  if cluster.service.name != 'hbase':
    # return empty paget for unsupported service
    return HttpResponse('')

  basic_info = dbutil.get_hbase_basic_info(cluster)

  metrics = {}
  metrics['endpoint'] = dbutil.map_cluster_to_endpoint(cluster.name)
  group = 'Cluster'
  metrics['keys'] = '%s-readRequestsCountPerSec|%s-writeRequestsCountPerSec' % (group, group)

  params = {
    'cluster': cluster,
    'basic_info': basic_info,
    'metrics': metrics,
  }
  return respond(request, 'monitor/hbase_basic_board.html', params)

#url: /cluster/$id/regionserver/
def show_cluster_regionserver_board(request, id):
  cluster = dbutil.get_cluster(id)
  if cluster.service.name != 'hbase':
    # return empty paget for unsupported service
    return HttpResponse('')

  read_requests_dist_by_rs, write_requests_dist_by_rs = dbutil.get_requests_distribution_groupby(cluster, 'regionserver');
  params = {
    'chart_id': 'read_requests_on_rs',
    'chart_title': 'read requests on region server',
    'request_dist': read_requests_dist_by_rs,
    'base_url': '/monitor/regionserver/',
  }

  read_requests_dist_by_rs_chart = loader.get_template('monitor/requests_dist_pie_chart.tpl').render(Context(params))

  params = {
    'chart_id': 'write_requests_on_rs',
    'chart_title': 'write requests on region server',
    'request_dist': write_requests_dist_by_rs,
    'base_url': '/monitor/regionserver/',
  }
  write_requests_dist_by_rs_chart = loader.get_template('monitor/requests_dist_pie_chart.tpl').render(Context(params))

  regionservers = dbutil.get_items_on_cluster(cluster, 'regionserver', order_by='name')
  params = {
    'cluster': cluster,
    'read_requests_dist_by_rs_chart': read_requests_dist_by_rs_chart,
    'write_requests_dist_by_rs_chart': write_requests_dist_by_rs_chart,
    'regionservers': regionservers,
  }
  return respond(request, 'monitor/hbase_regionserver_board.html', params)

#url: /table/$table_id/
def show_table(request, id):
  table = dbutil.get_table(id)
  cluster = table.cluster

  read_requests_dist_by_rs, write_requests_dist_by_rs = dbutil.get_requests_distribution(table)
  params = {
    'chart_id': 'read_requests_on_rs',
    'chart_title': 'read requests on region',
    'request_dist': read_requests_dist_by_rs,
  }

  read_requests_dist_by_rs_chart = loader.get_template('monitor/requests_dist_column_chart.tpl').render(Context(params))

  params = {
    'chart_id': 'write_requests_on_rs',
    'chart_title': 'write requests on region',
    'request_dist': write_requests_dist_by_rs,
  }
  write_requests_dist_by_rs_chart = loader.get_template('monitor/requests_dist_column_chart.tpl').render(
    Context(params))

  counter = dbutil.get_counter('infra-hbase-' + cluster.name, table.name + '-Availability')

  metrics = {}
  metrics['endpoint'] = dbutil.map_cluster_to_endpoint(cluster.name)
  group = str(table)
  metrics['keys'] = '%s-readRequestsCountPerSec|%s-writeRequestsCountPerSec' % (group, group)
  params = {
    'cluster': cluster,
    'table': table,
    'counter': counter,
    'read_requests_dist_by_rs_chart': read_requests_dist_by_rs_chart,
    'write_requests_dist_by_rs_chart': write_requests_dist_by_rs_chart,
    'metrics': metrics,
  }

  return respond(request, 'monitor/hbase_table.html', params)

# metrics is an array of counters, where the counter is formatted as :
# [operationName, CounterOfNumOps, CounterOfAvgTime]
def generate_operation_metric_param(record, group):
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
      numOpsCounter['name'] = operationNumOpsName
      numOpsCounter['keys'] = '%s-%s' % (group, operationNumOpsName)
      operationCounter.append(numOpsCounter)
      operationAvgTimeName = operationName + '_AvgTime'
      avgTimeCounter = {}
      avgTimeCounter['name'] = operationAvgTimeName
      avgTimeCounter['keys'] = '%s-%s' % (group, operationAvgTimeName)
      operationCounter.append(avgTimeCounter)
      metrics.append(operationCounter)
  return metrics

def generate_operation_metrics_param_for_cluster(cluster):
  # we first read operation metrics for tables of the cluster
  tables = dbutil.get_table_by_cluster(cluster)
  operationCounterNameOfTables = {}
  for table in tables:
    if table.operationMetrics is not None and table.operationMetrics != '':
      tableOperationMetrics = json.loads(table.operationMetrics)
      for operationName in tableOperationMetrics:
        numOpsCounterName = '%s-%s_NumOps' % (table.name, operationName)
        avgTimeCounterName = '%s-%s_AvgTime' % (table.name, operationName)
        if operationName not in operationCounterNameOfTables:
          operationCounterNameOfTables[operationName] = []
          operationCounterNameOfTables[operationName].append(1)
          operationCounterNameOfTables[operationName].append(numOpsCounterName)
          operationCounterNameOfTables[operationName].append(avgTimeCounterName)
        else:
          operationCounterNameOfTables[operationName][0] = operationCounterNameOfTables[operationName][0] + 1
          operationCounterNameOfTables[operationName][1] = '%s|%s' % (operationCounterNameOfTables[operationName][1], numOpsCounterName)
          operationCounterNameOfTables[operationName][2] = '%s|%s' % (operationCounterNameOfTables[operationName][2], avgTimeCounterName)

  metrics = []
  if cluster.hbasecluster.operationMetrics is not None and cluster.hbasecluster.operationMetrics != '':
    clusterOperationMetrics = json.loads(cluster.hbasecluster.operationMetrics)
    for operationName in clusterOperationMetrics.keys():
      tokens = operationName.split('-')
      operationShowName = tokens[len(tokens) - 1]
      operationCounter = []
      operationCounter.append(operationShowName)
      operationCounter.append(operationCounterNameOfTables[operationName][0])
      operationCounter.append(operationCounterNameOfTables[operationName][1])
      operationCounter.append(operationCounterNameOfTables[operationName][2])
      metrics.append(operationCounter)
  return metrics

#url: /table/operation/$table_id
def show_table_operation(request, id):
  table = dbutil.get_table(id)
  cluster = table.cluster
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  group = str(table)
  params = {
    'cluster' : cluster,
    'table' : table,
    'metrics' : generate_operation_metric_param(table, group),
    'endpoint' : endpoint
  }
  return respond(request, 'monitor/hbase_table_operation.html', params)

#url: /cluster/operation/$cluster_id
def show_cluster_operation(request, id):
  cluster = dbutil.get_cluster(id)
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  group = 'Cluster'
  params = {
    'cluster' : cluster,
    'metrics' : generate_operation_metric_param(cluster.hbasecluster, group),
    'endpoint' : endpoint
  }
  return respond(request, 'monitor/hbase_cluster_operation.html', params)

#url: /cluster/operation/tablecomparsion
def show_cluster_operation_table_comparison(request, id):
  cluster = dbutil.get_cluster(id)
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  group = 'Cluster'
  params = {
    'cluster' : cluster,
    'metrics' : generate_operation_metrics_param_for_cluster(cluster),
    'endpoint' : endpoint
  }
  return respond(request, 'monitor/hbase_cluster_operation_table_comparsion.html', params)

#url: /regionserver/$rs_id/
def show_regionserver(request, id):
  rs = dbutil.get_regionserver(id)
  cluster = rs.cluster

  read_requests_dist_by_rs, write_requests_dist_by_rs = dbutil.get_requests_distribution(rs);
  params = {
    'chart_id': 'read_requests_on_rs',
    'chart_title': 'read requests on region',
    'request_dist': read_requests_dist_by_rs,
  }

  read_requests_dist_by_rs_chart = loader.get_template('monitor/requests_dist_column_chart.tpl').render(Context(params))

  params = {
    'chart_id': 'write_requests_on_rs',
    'chart_title': 'write requests on region',
    'request_dist': write_requests_dist_by_rs,
  }
  write_requests_dist_by_rs_chart = loader.get_template('monitor/requests_dist_column_chart.tpl').render(
    Context(params))

  metrics = {}
  metrics['endpoint'] = dbutil.map_cluster_to_endpoint(cluster.name)
  group = str(rs)
  metrics['keys'] = '%s-readRequestsCountPerSec|%s-writeRequestsCountPerSec' % (group, group)
  params = {
    'cluster': cluster,
    'regionserver': rs,
    'read_requests_dist_by_rs_chart': read_requests_dist_by_rs_chart,
    'write_requests_dist_by_rs_chart': write_requests_dist_by_rs_chart,
    'metrics': metrics,
  }
  return respond(request, 'monitor/hbase_regionserver.html', params)

#url: /job/$id/
def show_job(request, id):
  tasks = dbutil.get_tasks_by_job(id)
  job = dbutil.get_job(id)

  metrics = {}
  metrics['endpoint'] = [metric_helper.form_perf_counter_endpoint_name(task) for task in tasks]
  metrics['metrics_view_config'] = metric_helper.job_metrics_view_config(job)
  params = {
    'job': job,
    'tasks': tasks,
    'metrics': metrics,
  }

  return respond(request, 'monitor/job.html', params)

#url: /task/$id/
def show_task(request, id):
  task = dbutil.get_task(id)
  job = task.job
  tasks = dbutil.get_tasks_by_job(job)

  metrics = {}
  metrics['endpoint'] = metric_helper.form_perf_counter_endpoint_name(task)
  metrics['metrics_view_config'] = metric_helper.task_metrics_view_config(str(task))
  params = {
    'job': job,
    'task': task,
    'tasks': tasks,
    'metrics': metrics,
  }
  return respond(request, 'monitor/task.html', params)


def show_all_metrics(request):
  result = {}
  metrics = dbutil.get_all_metrics()
  if not metrics:
    return HttpResponse('', content_type='application/json; charset=utf8')

  result['timestamp'] = time.time()
  result['data'] = metrics
  # defaultly not format output
  indent = None
  if 'indent' in request.GET:
    # when indent is set, format json output with indent = 1
    indent = 1
  return HttpResponse(json.dumps(result, indent=indent),
                      content_type='application/json; charset=utf8')

def show_all_metrics_config(request):
  metrics_config = metric_helper.get_all_metrics_config()

  # defaultly not format output
  indent = None
  if 'indent' in request.GET:
    # when indent is set, format json output with indent = 1
    indent = 1
  return HttpResponse(json.dumps(metrics_config, indent=indent),
                      content_type='application/json; charset=utf8')

def get_time_range(request):
  start_time = datetime.datetime.today() + datetime.timedelta(hours=-1)
  end_time = datetime.datetime.today()
  if 'start_time' in request.COOKIES:
    start_time = datetime.datetime.strptime(request.COOKIES['start_time'], '%Y-%m-%d-%H-%M')

  if 'start_time' in request.GET:
    start_time = datetime.datetime.strptime(request.GET['start_time'], '%Y-%m-%d-%H-%M')

  if 'end_time' in request.COOKIES:
    end_time = datetime.datetime.strptime(request.COOKIES['end_time'], '%Y-%m-%d-%H-%M')

  if 'end_time' in request.GET:
    end_time = datetime.datetime.strptime(request.GET['end_time'], '%Y-%m-%d-%H-%M')
  return start_time, end_time


@transaction.commit_on_success
@csrf_exempt
@require_http_methods(["POST"])
def add_counter(request):
  counters = json.loads(request.POST.keys()[0])
  remote_ip = request.META['REMOTE_ADDR']
  update_time = datetime.datetime.utcfromtimestamp(time.time()).replace(tzinfo=timezone.utc)
  for dict in counters:
    group = dict['group']
    endpoint = remote_ip
    if 'endpoint' in dict:
      endpoint = dict['endpoint']
    label = ''
    if 'label' in dict:
      label = dict['label']
    name = dict['name']
    counter, create = dbutil.get_or_create_counter(group, name)

    counter.host = endpoint
    counter.value = (float)(dict['value'])
    counter.unit = dict['unit']
    counter.last_update_time = update_time
    counter.label = label
    counter.save()
  return HttpResponse("ok")


def show_all_counters(request):
  result = {}
  metrics = dbutil.get_all_counters()
  if not metrics:
    return HttpResponse('', content_type='application/json; charset=utf8')

  result['timestamp'] = time.time()
  result['data'] = metrics
  # defaultly not format output
  indent = None
  if 'indent' in request.GET:
  # when indent is set, format json output with indent = 1
    indent = 1
  return HttpResponse(json.dumps(result, indent=indent),
                      content_type='application/json; charset=utf8')


def respond(request, template, params=None):
  """Helper to render a response, passing standard stuff to the response.
  Args:
  request: The request object.
  template: The template name; '.html' is appended automatically.
  params: A dict giving the template parameters; modified in-place.
  Returns:
  Whatever render_to_response(template, params) returns.
  Raises:
  Whatever render_to_response(template, params) raises.
  """
  params['request'] = request
  params['user'] = request.user
  params['chart_url_prefix'] = owl_config.CHART_URL_PREFIX
  params['supervisor_port'] = owl_config.SUPERVISOR_PORT
  params.update(request.GET)
  response = render_to_response(template, params,
                                context_instance=RequestContext(request))
  return response
