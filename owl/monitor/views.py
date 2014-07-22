# -*- coding: utf-8 -*-
from django.conf import settings
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from django.shortcuts import render_to_response, redirect
from django.template import RequestContext, Context, loader
from django.utils import timezone
from django.http import HttpResponse
from django.db import transaction
from utils.quota_util import QuotaUpdater

from models import Table

import datetime
import dbutil
import json
import logging
import metric_helper
import time
import owl_config

logger = logging.getLogger(__name__)

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

    tsdb_read_query = []
    tsdb_write_query = []
    for cluster in clusters:
      tsdb_read_query.append(metric_helper.make_metric_query(cluster.name, 'Cluster', 'readRequestsCountPerSec'))
      tsdb_write_query.append(metric_helper.make_metric_query(cluster.name, 'Cluster', 'writeRequestsCountPerSec'))

    params.update({
      'tsdb_read_query': tsdb_read_query,
      'tsdb_write_query': tsdb_write_query,
    })

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
  elif cluster.service.name == 'storm':
    return respond(request, 'monitor/storm_task_board.html', params)
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

  tsdb_read_query = []
  tsdb_write_query = []
  for table in user_tables:
    tsdb_read_query.append(metric_helper.make_metric_query(cluster.name, table.name, 'readRequestsCountPerSec'))
    tsdb_write_query.append(metric_helper.make_metric_query(cluster.name, table.name, 'writeRequestsCountPerSec'))

  params = {
    'cluster': cluster,
    'read_requests_dist_by_table_chart': read_requests_dist_by_table_chart,
    'write_requests_dist_by_table_chart': write_requests_dist_by_table_chart,
    'system_tables': system_tables,
    'user_tables': user_tables,
    'table_read_item_keys': table_read_item_keys,
    'table_write_item_keys': table_write_item_keys,
    'tsdb_read_query': tsdb_read_query,
    'tsdb_write_query': tsdb_write_query,
  }
  return respond(request, 'monitor/hbase_table_board.html', params)

#url: /cluster/$id/total/
def show_quota_total_board(request, id):
  cluster = dbutil.get_cluster(id)
  if cluster.service.name != 'hdfs':
    return HttpResponse('')

  tsdb_quota_total, tsdb_space_quota_total = dbutil.get_quota_distribution(cluster)
  params = {
    'chart_id': 'used_quota_total',
    'chart_title': 'total name quota on users',
    'request_dist': tsdb_quota_total,
    'base_url': '/monitor/user/',
  }
  tsdb_quota_total_chart = loader.get_template('monitor/requests_dist_pie_chart.tpl').render(Context(params))

  params = {
    'chart_id': 'used_space_quota_total',
    'chart_title': 'total used space on users',
    'request_dist': tsdb_space_quota_total,
    'base_url': '/monitor/user/',
  }
  tsdb_space_quota_total_chart = loader.get_template('monitor/requests_dist_pie_chart.tpl').render(Context(params))

  tsdb_quota_total_query = [metric_helper.make_quota_query(cluster.name, 'used_quota_total', 'used_quota')]
  tsdb_space_quota_total_query = [metric_helper.make_quota_query(cluster.name,
    'used_space_quota_total', 'used_space_quota')]

  params = {
    'cluster': cluster,
    'tsdb_quota_total_chart': tsdb_quota_total_chart,
    'tsdb_space_quota_total_chart': tsdb_space_quota_total_chart,
    'tsdb_quota_total_query': tsdb_quota_total_query,
    'tsdb_space_quota_total_query': tsdb_space_quota_total_query,
  }
  return respond(request, 'monitor/quota_total_board.html', params)

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
  hdfs_cluster = dbutil.get_hdfs_cluster_by_name(cluster.name)

  group = 'Cluster'
  tsdb_read_query = [metric_helper.make_metric_query(cluster.name, group, 'readRequestsCountPerSec')]
  tsdb_write_query = [metric_helper.make_metric_query(cluster.name, group, 'writeRequestsCountPerSec')]

  params = {
    'cluster': cluster,
    'hdfs_cluster': hdfs_cluster,
    'basic_info': basic_info,
    'tsdb_read_query': tsdb_read_query,
    'tsdb_write_query': tsdb_write_query,
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

#url: /cluster/$id/replication/
def show_cluster_replication(request, id):
  cluster = dbutil.get_cluster(id)
  region_servers = dbutil.get_regionservers_with_active_replication_metrics_by_cluster(cluster) 
  (peer_id_endpoint_map, peer_id_cluster_map) = metric_helper.get_peer_id_endpoint_map_and_cluster(region_servers)
  params = {
    'cluster' : cluster,
    'replication_metrics' : metric_helper.make_metrics_query_for_replication(peer_id_endpoint_map, peer_id_cluster_map),
  }
  return respond(request, 'monitor/hbase_replication.html', params)

#url: /cluster/$id/?type="spout or bolt"
def show_cluster_storm_builtin_metrics(request, id):
  cluster = dbutil.get_cluster(id)
  storm_tasks = dbutil.get_storm_task_by_cluster(cluster)
  type = request.GET.get('type')
  type_dict = {
    "Spout": "STORM_BUILTIN_SPOUT_METRICS",
    "Bolt": "STORM_BUILTIN_BOLT_METRICS",
  }

  # builtin metrics format is <storm_id, STORM_BUILTIN_SPOUT_METRICS|STORM_BUILTIN_BOLT_METRICS, <key, value>>>
  storm_metrics = [];
  for storm_task in storm_tasks:
    if storm_task.job.name != 'metricserver':
      continue
    try:
      json_metrics = json.loads(storm_task.last_metrics_raw)
    except:
      logger.warning("Failed to parse metrics of task: %s", storm_task)
      return HttpResponse('')

    for storm_id, topology_metrics in json_metrics.iteritems():
      element = {"storm_id": storm_id}
      for group_name, group_metrics in topology_metrics.iteritems():
        if group_name == type_dict.get(type):
          for metrics_name, metrics in group_metrics.iteritems():
            metrics_name = metrics_name.lstrip('_')
            metrics_name = metrics_name.replace('-', '_')
            element[metrics_name] = metrics
      storm_metrics.append(element)

  params = {
    'cluster' : cluster,
    'storm_metrics' : storm_metrics,
    }

  if type == "Spout":
    return respond(request, 'monitor/storm_spout_board.html', params)
  elif type == "Bolt":
    return respond(request, 'monitor/storm_bolt_board.html', params)
  else:
    return HttpResponse('Unsupported type: ' + type)

#url: /cluster/$id/system_metrics/
def show_cluster_storm_system_metrics(request, id):
  cluster = dbutil.get_cluster(id)
  storm_tasks = dbutil.get_storm_task_by_cluster(cluster)

  # system metrics format is <storm_id, STORM_SYSTEM_*, <key, value>>>;
  # and key may in format: "GC/*", "memory/heap:*", ""memory/nonHeap:*" or ".*";
  storm_metrics = []
  for storm_task in storm_tasks:
    try:
      json_metrics = json.loads(storm_task.last_metrics_raw)
    except:
      logger.warning("Failed to parse metrics of task: %s", storm_task.last_metrics_raw)
      return HttpResponse('')

    for storm_id, topology_metrics in json_metrics.iteritems():
      topology_element = []
      for group_name, group_metrics in topology_metrics.iteritems():
        if group_name.find("STORM_SYSTEM_") != 0:
          continue
        group_name = group_name.lstrip("STORM_SYSTEM_")
        element = {"worker_endpoint" : group_name};
        gc_value = ""
        memory_heap_value = ""
        memory_non_heap_value = ""
        for metrics_name, metrics in group_metrics.iteritems():
          if metrics_name.find("GC/") == 0:
            if len(gc_value) != 0:
              gc_value += ", \n"
            gc_value += metrics_name.lstrip("GC/") + ":" + str(metrics)

          if metrics_name.find("memory/heap:") == 0:
            if len(memory_heap_value) != 0:
              memory_heap_value += ", \n"
            memory_heap_value += metrics_name.lstrip("memory/heap:") + ":" + str(metrics)

          if metrics_name.find("memory/nonHeap:") == 0:
            if len(memory_non_heap_value) != 0:
              memory_non_heap_value += ", \n"
            memory_non_heap_value += metrics_name.lstrip("memory/nonHeap:") + ":" + str(metrics)

          if metrics_name == "startTimeSecs":
            element["start_time_sec"] = metrics
          if metrics_name == "uptimeSecs":
            element["uptime_sec"] = metrics
        element["GC"] = gc_value
        element["memory_heap"] = memory_heap_value
        element["memory_non_heap"] = memory_non_heap_value
        topology_element.append(element)

      metrics = {
        "storm_id" : storm_id,
        "topology_metrics" : topology_element
      }
      storm_metrics.append(metrics)

  params = {
    'cluster' : cluster,
    'storm_metrics' : storm_metrics,
    }

  return respond(request, 'monitor/storm_system_metrics_board.html', params)

#url: /cluster/$id/user_metrics/
def show_cluster_storm_user_metrics(request, id):
  cluster = dbutil.get_cluster(id)
  storm_tasks = dbutil.get_storm_task_by_cluster(cluster)

  # user metrics format is <storm_id, component_id:task_id, <key, value>>>;
  storm_metrics = {}
  for storm_task in storm_tasks:
    if storm_task.job.name != 'metricserver':
      continue
    try:
      json_metrics = json.loads(storm_task.last_metrics_raw)
    except:
      logger.warning("Failed to parse metrics of task: %s", storm_task)
      return HttpResponse('')

    for storm_id, topology_metrics in json_metrics.iteritems():
      topology_metrics_dict = storm_metrics.setdefault(storm_id, {})
      for group_name, group_metrics in topology_metrics.iteritems():
        if group_name.find("STORM_SYSTEM_") == 0 or group_name == "STORM_BUILTIN_SPOUT_METRICS" or group_name == "STORM_BUILTIN_BOLT_METRICS":
          continue
        group_component_id = group_name.split(":")[0]
        group_task_id = group_name.split(":")[1]
        group_metrics_dict = topology_metrics_dict.setdefault(group_component_id, {})
        task_metrics_dict = group_metrics_dict.setdefault(group_task_id, {});

        for metrics_name, metrics in group_metrics.iteritems():
          task_metrics_dict[metrics_name] = metrics
  # after upper handle, storm_metrics in format: <storm_id, <component_id, <task_id, <key, value>>>>

  format_storm_metrics = {}
  for storm_id in storm_metrics:
    topology_metrics = storm_metrics.get(storm_id)
    format_topology_metrics = format_storm_metrics.setdefault(storm_id, {})
    for component_id in topology_metrics:
      group_metrics = topology_metrics.get(component_id)
      format_group_metrics = format_topology_metrics.setdefault(component_id, [])

      for task_id in group_metrics:
        metrics = group_metrics.get(task_id)
        key_set, value_set = add_key_set_for_format_group_metrics(format_group_metrics, metrics.keys())
        format_metrics_list = [task_id]
        for key in key_set:
          if key == "TaskID":
            continue
          format_metrics_list.append(metrics.get(key, " "))
        value_set.append(format_metrics_list)

  # after upper handle, format_storm_metrics in format:
  # <storm_id, <component_id,[<"key_set": [key1, key2, ...... ,keyn], "value_sets":
  # [[v11, v12, ...... v1n], ...... ,[vm1, vm2, ...... vmn]],> ...... <"key_set": [], "value_sets": []>] > >
  params = {
    'cluster' : cluster,
    'storm_metrics' : format_storm_metrics,
    }

  return respond(request, 'monitor/storm_user_board.html', params)

def add_key_set_for_format_group_metrics(format_group_metrics, task_key_set):
  key_set = task_key_set[:]
  key_set.sort()
  key_set.insert(0, "TaskID")

  for group_metrics in format_group_metrics:
    if cmp(key_set, group_metrics["key_set"]) == 0:
      return (key_set, group_metrics["value_set"])

  new_group_metrics = {
    "key_set": key_set,
    "value_set": [],
  }
  format_group_metrics.append(new_group_metrics)

  return (new_group_metrics["key_set"], new_group_metrics["value_set"])

#url: /topology/$storm_id/?topology_id=xxx
def show_storm_topology(request, id):
  cluster = dbutil.get_cluster(id)

  storm_id = request.GET.get('topology_id')
  spout_keys = ["__ack-count", "__fail-count", "__emit-count", "__transfer-count", "__complete-latency",]
  bolt_keys = ["__ack-count", "__fail-count", "__emit-count", "__transfer-count", "__process-latency", "__execute-count", "__execute-latency",]

  storm_metrics = {"storm_id" : storm_id}
  storm_graphs = []
  for key in spout_keys:
    title = storm_id + ":Spout:" + key
    query = ("&m=sum:%s{host=%s,group=STORM_BUILTIN_SPOUT_METRICS}&o=" % (key, dbutil.format_storm_name(storm_id)))
    graph = {
      "title" : title,
      "query" : query,
    }
    storm_graphs.append(graph)

  for key in bolt_keys:
    title = storm_id + ":Bolt:" + key
    query = ("&m=sum:%s{host=%s,group=STORM_BUILTIN_BOLT_METRICS}&o=" % (key, dbutil.format_storm_name(storm_id)))
    graph = {
      "title" : title,
      "query" : query,
    }
    storm_graphs.append(graph)

  storm_metrics["graphs"] = storm_graphs
  params = {
    'cluster' : cluster,
    'storm_metrics' : storm_metrics,
  }

  return respond(request, 'monitor/storm_topology.html', params)


def is_test_table(table):
  if 'tst' in table.cluster.name:
    return True
  if 'test' in table.cluster.name:
      return True

  if 'longhaul' in table.name:
   return True
  if 'test' in table.name:
     return True

  return False

#url: /table
def show_all_tables(request):
  tables = dbutil.get_all_tables()
  tables = [table for table in tables if not is_system_table(table)]
  tables = [table for table in tables if not is_test_table(table)]
  params = {
    'tables': tables,
  }
  return respond(request, 'monitor/hbase_tables.html', params)

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

  memstore_size_dist_by_region, storefile_size_dist_by_region = dbutil.get_data_distribution(table)

  params = {
    'chart_id': 'memstore_size_dist_by_region',
    'chart_title': 'memstore size on region',
    'request_dist': memstore_size_dist_by_region,
    }
  memstore_size_dist_by_region_chart = loader.get_template('monitor/requests_dist_column_chart.tpl').render(Context(params))

  params = {
    'chart_id': 'storefile_size_dist_by_region',
    'chart_title': 'storefile size on region',
    'request_dist': storefile_size_dist_by_region,
    }
  storefile_size_dist_by_region_chart = loader.get_template('monitor/requests_dist_column_chart.tpl').render(Context(params))

  group = str(table)
  tsdb_read_query = [metric_helper.make_metric_query(cluster.name, group, 'readRequestsCountPerSec')]
  tsdb_write_query = [metric_helper.make_metric_query(cluster.name, group, 'writeRequestsCountPerSec')]

  params = {
    'cluster': cluster,
    'table': table,
    'read_requests_dist_by_rs_chart': read_requests_dist_by_rs_chart,
    'write_requests_dist_by_rs_chart': write_requests_dist_by_rs_chart,
    'memstore_size_dist_by_region_chart': memstore_size_dist_by_region_chart,
    'storefile_size_dist_by_region_chart': storefile_size_dist_by_region_chart,
    'tsdb_read_query': tsdb_read_query,
    'tsdb_write_query': tsdb_write_query,
  }

  return respond(request, 'monitor/hbase_table.html', params)

#url: /table/operation/$table_id
def show_table_operation(request, id):
  table = dbutil.get_table(id)
  cluster = table.cluster
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  group = str(table)
  params = {
    'cluster' : cluster,
    'table' : table,
    'tsdb_metrics' : metric_helper.make_operation_metrics(endpoint, table, group),
    'endpoint' : endpoint
  }
  return respond(request, 'monitor/hbase_table_operation.html', params)

#url: /table/count_rows
def show_table_count_rows(request):
  tables_to_count = Table.objects.filter(is_count_rows=True)
  tables_not_to_count = Table.objects.filter(is_count_rows=False)
  params = {
    'count_period': settings.COUNT_PERIOD,
    'count_start_hour': settings.COUNT_START_HOUR,
    'count_end_hour': settings.COUNT_END_HOUR,
    'tables_to_count': tables_to_count,
    'tables_not_to_count': tables_not_to_count
  }
  return respond(request, 'monitor/hbase_table_count_rows.html', params)

#url: /table/add_table_count_rows/$table_id
def add_table_count_rows(request, id):
  table = dbutil.get_table(id)
  table.is_count_rows = True
  table.save()
  return HttpResponse()

#url: /table/cancel_table_count_rows/$table_id
def cancel_table_count_rows(request, id):
  table = dbutil.get_table(id)
  table.is_count_rows = False
  table.save()
  return HttpResponse()

#url: /regionserver/operation/$rs_id
def show_regionserver_operation(request, id):
  regionserver = dbutil.get_regionserver(id)
  cluster = regionserver.cluster
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  params = {
    'cluster' : cluster,
    'regionserver' : regionserver,
    'tsdb_metrics' : metric_helper.generate_operation_metric_for_regionserver(regionserver),
    'endpoint' : endpoint
  }
  return respond(request, 'monitor/hbase_regionserver_operation.html', params)

#url: /cluster/operation/$cluster_id
def show_cluster_operation(request, id):
  cluster = dbutil.get_cluster(id)
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  group = 'Cluster'
  params = {
    'cluster' : cluster,
    'tsdb_metrics' : metric_helper.make_operation_metrics(endpoint, cluster.hbasecluster, group),
    'endpoint' : endpoint
  }

  return respond(request, 'monitor/hbase_cluster_operation.html', params)

#url: /cluster/operation/tablecomparsion
def show_cluster_operation_table_comparison(request, id):
  cluster = dbutil.get_cluster(id)
  endpoint = dbutil.map_cluster_to_endpoint(cluster.name)
  params = {
    'cluster' : cluster,
    'tsdb_metrics' : metric_helper.make_operation_metrics_for_tables_in_cluster(cluster),
    'endpoint' : endpoint
  }
  print params['tsdb_metrics']
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

  group = str(rs)
  tsdb_read_query = [metric_helper.make_metric_query(cluster.name, group, 'readRequestsCountPerSec')]
  tsdb_write_query = [metric_helper.make_metric_query(cluster.name, group, 'writeRequestsCountPerSec')]

  params = {
    'cluster': cluster,
    'regionserver': rs,
    'read_requests_dist_by_rs_chart': read_requests_dist_by_rs_chart,
    'write_requests_dist_by_rs_chart': write_requests_dist_by_rs_chart,
    'tsdb_read_query': tsdb_read_query,
    'tsdb_write_query': tsdb_write_query,
  }
  return respond(request, 'monitor/hbase_regionserver.html', params)

#url: /user/$user_id
def show_user_quota(request, id):
  quota = dbutil.get_quota(id)
  cluster = quota.cluster

  used_quota_query = [metric_helper.make_quota_query(cluster.name, quota.name, 'used_quota')]
  used_space_quota_query = [metric_helper.make_quota_query(cluster.name, quota.name, 'used_space_quota')]

  params = {
    'cluster': cluster,
    'used_quota_query': used_quota_query,
    'used_space_quota_query': used_space_quota_query,
  }
  return respond(request, 'monitor/quota_user.html', params)

#url: /job/$id/
def show_job(request, id):
  tasks = dbutil.get_healthy_tasks_by_job(id)
  job = dbutil.get_job(id)

  endpoints = [metric_helper.form_perf_counter_endpoint_name(task) for task in tasks]
  tsdb_metrics = metric_helper.make_metrics_query_for_job(endpoints, job, tasks)
  print tsdb_metrics
  params = {
    'job': job,
    'tasks': tasks,
    'tsdb_metrics': tsdb_metrics,
  }

  return respond(request, 'monitor/job.html', params)

#url: /task/$id/
def show_task(request, id):
  task = dbutil.get_task(id)
  job = task.job
  tasks = dbutil.get_tasks_by_job(job)

  tsdb_metrics = metric_helper.make_metrics_query_for_task(
    metric_helper.form_perf_counter_endpoint_name(task),
    task)

  params = {
    'job': job,
    'task': task,
    'tasks': tasks,
    'tsdb_metrics': tsdb_metrics,
  }
  return respond(request, 'monitor/task.html', params)


def show_all_metrics(request):
  result = {}
  metrics = dbutil.get_all_metrics()
  if not metrics:
    return HttpResponse('', content_type='application/json; charset=utf8')

  result['timestamp'] = int(time.time())
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
  counters = json.loads(request.body)
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
  params['tsdb_url_prefix'] = owl_config.TSDB_ADDR
  params['supervisor_port'] = owl_config.SUPERVISOR_PORT
  params['start_date'] = (datetime.datetime.now() - datetime.timedelta(minutes=15)).strftime('%Y/%m/%d-%H:%M:%S')
  params['quota_start_date'] = (datetime.datetime.now() - datetime.timedelta(hours=20)).strftime('%Y/%m/%d-%H:%M:%S')
  params.update(request.GET)
  response = render_to_response(template, params,
                                context_instance=RequestContext(request))
  return response
