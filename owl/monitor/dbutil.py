# -*- coding: utf-8 -*-
import datetime
import hashlib
import json
import logging
import socket
import struct
import time

import MySQLdb
from DBUtils.PooledDB import PooledDB

from django.conf import settings
from django.utils import timezone

from models import Service, Cluster, Job, Task, Status
from models import Table, RegionServer, HBaseCluster, Region
from models import Counter
from django.db.models import Sum
import metric_helper

logger = logging.getLogger(__name__)

db_settings = settings.DATABASES['default']
# we use db connection pool to execute batch update
DBConnectionPool = PooledDB(MySQLdb, maxusage = 10, mincached = 5,
                            db = db_settings['NAME'],
                            host = db_settings['HOST'],
                            port = int(db_settings['PORT']),
                            user = db_settings['USER'],
                            passwd = db_settings['PASSWORD'],
                            charset = 'utf8')

def get_services():
  return Service.objects.filter(active=True).all()


def get_service(id):
  try:
    return Service.objects.get(id=id, active=True)
  except Service.DoesNotExist:
    return None


def get_clusters_by_service(service_id=None):
  filters = {"active": True}
  if service_id: filters["service"] = service_id
  return Cluster.objects.filter(**filters).all().order_by('service', 'name')


def get_cluster(id):
  try:
    return Cluster.objects.get(id=id, active=True)
  except Cluster.DoesNotExist:
    return None


def get_jobs_by_cluster(cluster_id):
  return Job.objects.filter(cluster=cluster_id, active=True).all()


def get_job(id):
  try:
    return Job.objects.get(id=id, active=True)
  except Job.DoesNotExist:
    return None


def get_tasks_by_job(job_id):
  return Task.objects.filter(job=job_id, active=True).all()

def get_healthy_tasks_by_job(job_id):
  return filter(lambda x: x.health, Task.objects.filter(job=job_id, active=True).all())

def get_tasks_by_cluster(cluster_id):
  return Task.objects.filter(job__cluster=cluster_id, active=True).order_by('job', 'id')


def get_tasks_by_service(service_id=None):
  filters = {"active": True}
  if service_id: filters["job__cluster__service"] = service_id
  return Task.objects.filter(**filters).all()

def get_task_by_host_and_port(host, port):
  try:
    return Task.objects.get(host = host, port = port)
  except:
    host = socket.gethostbyname(host)
    return Task.objects.get(host = host, port = port)

def get_task(id):
  try:
    return Task.objects.get(id=id, active=True)
  except Task.DoesNotExist:
    return None

def generate_perf_counter_for_task(result):
  tasks = get_alive_tasks()
  for task in tasks:
    if not task.health:
      continue
    result.update(generate_perf_counter(task))
  return result

def get_alive_tasks():
  return Task.objects.filter(active=True, last_status=Status.OK).all()

def getTableAvailability(cluster, table):
  group = 'infra-hbase-' + cluster
  name = table + '-Availability'
  try:
    return Counter.objects.get(group=group, name=name,
                               last_update_time__gt=counter_alive_threshold()).value
  except Counter.DoesNotExist:
    return -1.0

def generate_perf_counter(task):
  result = {}
  try:
    last_metrics = json.loads(task.last_metrics)
  except:
    print 'Failed to parse metrics of task:', task
    print task.last_metrics
    return result

  endpoint = result.setdefault(metric_helper.form_perf_counter_endpoint_name(task), {})
  for bean_name, bean_metrics in last_metrics.iteritems():
    group_name = metric_helper.form_perf_counter_group_name(task, bean_name)
    group = endpoint.setdefault(group_name, {})
    for metric_name, metric_value in bean_metrics.iteritems():
      metric_type = type(metric_value)
      if not metric_type is int and not metric_type is float:
        continue
      key_name = metric_helper.form_perf_counter_key_name(bean_name, metric_name)
      counter = group.setdefault(key_name, {})
      counter['type'] = 0
      counter['unit'] = ''
      counter['value'] = metric_value
  return result

# map cluster name to endpoint
def map_cluster_to_endpoint(cluster_name):
#  ip_int = int(hashlib.md5(cluster_name).hexdigest()[:8], 16)
#  return socket.inet_ntoa(struct.pack("!I", ip_int))
  # perf counter system support non-ip
  return cluster_name

# generate NumOps, AvgTime, MaxTime, MinTime counter for 'group'
def generate_perf_counter_of_operation_metrics(record, group):
    if record.operationMetrics is None or record.operationMetrics == '':
      return
    operationMetrics = json.loads(record.operationMetrics)
    for operationName in operationMetrics.keys():
      operation = operationMetrics[operationName]
      # report NumOps
      operationNumOpsName = operationName + '_NumOps'
      counter = group.setdefault(operationNumOpsName, {})
      counter['type'] = 0
      counter['unit'] = 'ops'
      counter['value'] = operation['NumOps']
      # report AvgTime
      operationAvgTimeName = operationName + '_AvgTime'
      counter = group.setdefault(operationAvgTimeName, {})
      counter['type'] = 0
      counter['unit'] = 'us'
      counter['value'] = operation['AvgTime']
      # report MinTime
      operationMinTimeName = operationName + '_MinTime'
      counter = group.setdefault(operationMinTimeName, {})
      counter['type'] = 0
      counter['unit'] = 'us'
      counter['value'] = operation['MinTime']
      # report MaxTime
      operationMaxTimeName = operationName + '_MaxTime'
      counter = group.setdefault(operationMaxTimeName, {})
      counter['type'] = 0
      counter['unit'] = 'us'
      counter['value'] = operation['MaxTime']

def generate_perf_counter_for_table(result):
  tables = Table.objects.filter(last_attempt_time__gte = alive_time_threshold())
  for table in tables:
    endpoint_name = map_cluster_to_endpoint(table.cluster.name)
    endpoint = result.setdefault(endpoint_name, {})
    group = endpoint.setdefault(str(table), {})
    counter = group.setdefault('readRequestsCountPerSec', {})
    counter['type'] = 0
    counter['unit'] = 'qps'
    counter['value'] = table.readRequestsCountPerSec
    counter = group.setdefault('writeRequestsCountPerSec', {})
    counter['type'] = 0
    counter['unit'] = 'qps'
    counter['value'] = table.writeRequestsCountPerSec
    # report operation perf counter for table
    generate_perf_counter_of_operation_metrics(table, group)

  return result

def generate_perf_counter_for_regionserver(result):
  regionservers = RegionServer.objects.filter(last_attempt_time__gte = alive_time_threshold())
  for regionserver in regionservers:
    endpoint_name = map_cluster_to_endpoint(regionserver.cluster.name)
    endpoint = result.setdefault(endpoint_name, {})
    group = endpoint.setdefault(str(regionserver), {})
    counter = group.setdefault('readRequestsCountPerSec', {})
    counter['type'] = 0
    counter['unit'] = 'qps'
    counter['value'] = regionserver.readRequestsCountPerSec
    counter = group.setdefault('writeRequestsCountPerSec', {})
    counter['type'] = 0
    counter['unit'] = 'qps'
    counter['value'] = regionserver.writeRequestsCountPerSec
  return result

def generate_perf_counter_for_cluster(result):
  hbase_clusters = HBaseCluster.objects.all()
  for hbase_cluster in hbase_clusters:
    last_update_time = hbase_cluster.cluster.last_attempt_time
    # filter not recently updated cluster
    if last_update_time < alive_time_threshold():
      continue
    endpoint_name = map_cluster_to_endpoint(hbase_cluster.cluster.name)
    endpoint = result.setdefault(endpoint_name, {})
    group = endpoint.setdefault('Cluster', {})
    counter = group.setdefault('readRequestsCountPerSec', {})
    counter['type'] = 0
    counter['unit'] = 'qps'
    counter['value'] = hbase_cluster.readRequestsCountPerSec
    counter = group.setdefault('writeRequestsCountPerSec', {})
    counter['type'] = 0
    counter['unit'] = 'qps'
    counter['value'] = hbase_cluster.writeRequestsCountPerSec
    # report operation perf counter for cluster
    generate_perf_counter_of_operation_metrics(hbase_cluster, group)
  return result

def get_all_metrics():
  result = {}
  generate_perf_counter_for_task(result)
  generate_perf_counter_for_table(result)
  generate_perf_counter_for_regionserver(result)
  generate_perf_counter_for_cluster(result)
  return result


def get_or_create_counter(group, name):
  return Counter.objects.get_or_create(group=group, name=name)

def get_counter(group, name):
  try:
    return Counter.objects.get(group=group, name=name)
  except Counter.DoesNotExist:
    return None

def counter_alive_threshold():
  return datetime.datetime.utcfromtimestamp(time.time() - 15).replace(tzinfo=timezone.utc)

def get_counters_by_group(group):
  return Counter.objects.filter(last_update_time__gt=counter_alive_threshold(), group=group).all()

def get_counters_by_group_and_label(group, label):
  return Counter.objects.filter(group=group, label=label).all()

def get_all_counters():
  result = {}
  counters = Counter.objects.filter(last_update_time__gt=counter_alive_threshold()).all()
  for counter in counters:
    endpoint = result.setdefault(counter.host, {})
    group = endpoint.setdefault(counter.group, {})
    key = group.setdefault(counter.name, {})
    key['type'] = 0
    key['unit'] = counter.unit
    key['value'] = counter.value
  return result


def get_metric(group, metric):
  try:
    return Metric.objects.get(group=group, metric=metric)
  except Metric.DoesNotExist:
    return None

def aggregate_metrics(records):
  agg_records = []
  first = None
  for record in records:
    if first is None:
      first = record
    else:
      if (record.time - first.time).seconds < 10:
        first.value += record.value
      else:
        agg_records.append(first)
        first = record
  print len(records), len(agg_records)
  return agg_records


def select_by_step(metrics, step):
  select_metrics = []
  for i in range(0, len(metrics), step):
    select_metrics.append(metrics[i])
  return select_metrics

def get_table(id):
  try:
    return Table.objects.get(id = id)
  except Table.DoesNotExist:
    return None

def get_table_by_cluster(cluster):
  return Table.objects.filter(cluster = cluster)

# attr should be 'regionserver' or 'table'
def get_items_on_cluster(cluster, attr, order_by):
  # return alive items order by sum of read and write qps
  return getattr(cluster, attr+'_set').filter(last_attempt_time__gte = alive_time_threshold()).\
      extra(select = {'qps':'readRequestsCountPerSec + writeRequestsCountPerSec'},
            order_by = (order_by, ))

def get_regionserver(id):
  try:
    return RegionServer.objects.get(id = id)
  except RegionServer.DoesNotExist:
    return None

def get_regionservers_with_active_replication_metrics_by_cluster(cluster):
  return RegionServer.objects.filter(cluster = cluster,
                                     last_attempt_time__gte = alive_time_threshold(),
                                     replication_last_attempt_time__gte = alive_time_threshold())

def get_region_by_regionserver_and_encodename(region_server, encodeName):
  try:
    return Region.objects.get(region_server = region_server, encodeName = encodeName)
  except Region.DoesNotExist:
    return None

def get_region_by_table(tableObj):
  # must use last_attemp_time to filter deleted-regions
  return Region.objects.filter(table = tableObj).filter(last_attempt_time__gte = alive_time_threshold()).all()

# attr should be 'regionserver' or 'table'
def get_requests_distribution_groupby(cluster, attr):
  items = getattr(cluster, attr+'_set').filter(last_attempt_time__gte = alive_time_threshold()).all()
  read_requests_dist = {}
  write_requests_dist = {}
  for item in items:
    read_requests_dist[str(item)] = (item.id, item.readRequestsCountPerSec)
    write_requests_dist[str(item)] = (item.id, item.writeRequestsCountPerSec)

  return (read_requests_dist, write_requests_dist)

def get_requests_distribution(owner):
  read_requests_dist = []
  write_requests_dist = []
  for region in owner.region_set.filter(last_attempt_time__gte = alive_time_threshold()).order_by('name'):
    read_requests_dist.append((str(region), region.readRequestsCountPerSec))
    write_requests_dist.append((str(region), region.writeRequestsCountPerSec))

  return (read_requests_dist, write_requests_dist)

def alive_time_threshold():
  threshold_in_secs = 60
  return datetime.datetime.utcfromtimestamp(time.time() - threshold_in_secs).replace(tzinfo=timezone.utc)

def get_hbase_basic_info(cluster):
  cluster_info = {}
  try:
    hbase_cluster_record = cluster.hbasecluster
    cluster_info['hbase_entry'] = cluster.entry

    cluster_info['hdfs_entry'] = get_hdfs_entry(cluster.name)
    cluster_info['zk_entry'] = get_zk_entry(cluster)

    cluster_info['read_qps'] = hbase_cluster_record.readRequestsCountPerSec
    cluster_info['write_qps'] = hbase_cluster_record.writeRequestsCountPerSec
  except Exception as e:
    logger.warning("Failed to get hbase cluster for cluster %r, %r", cluster, e)

  return cluster_info

def get_hdfs_entry(cluster_name):
  try:
    service_record = Service.objects.get(name='hdfs')
    hdfs_cluster_record = Cluster.objects.filter(service = service_record,
                                                 name = cluster_name)
    return hdfs_cluster_record[0].entry
  except Exception as e:
    logger.warning("Failed to get hdfs entry for cluster %r, %r", cluster_name, e)

  return ""

# parse zk address from hbase master's metrics
def get_zk_entry(cluster):
  try:
    master_task = cluster.job_set.filter(name='master')[0].task_set.all()
    for task in master_task:
      if not task.health:
        continue
      metric = json.loads(task.last_metrics)
      zk_metrics = metric['hadoop:service=Master,name=Master']['ZookeeperQuorum']
      return zk_metrics

  except Exception as e:
    logger.warning("Failed to get zk entry for cluster %r: %r", cluster.name, e)
  return ""

def quota_alive_threshold():
  threshold_in_secs = 60*60*24
  return datetime.datetime.utcfromtimestamp(time.time() - threshold_in_secs).replace(tzinfo=timezone.utc)

def get_quota_summary(cluster):
  try:
    return cluster.quota_set.filter(last_update_time__gte = quota_alive_threshold()).order_by('name')

  except Exception as e:
    logger.warning("Failed to get quota for cluster %r: %r", cluster.name, e)
    return []

def update_regions_for_region_server_metrics(regions):
  all_update_metrics = []
  for region in regions:
    update_metrics = []
    update_metrics.append(str(region.last_operation_attempt_time).split('.')[0])
    update_metrics.append(str(region.operationMetrics))
    update_metrics.append(str(region.id))
    all_update_metrics.append(update_metrics)

  conn = None
  try:
    conn=DBConnectionPool.connection()
    cur=conn.cursor()

    cur.executemany('update monitor_region set last_operation_attempt_time=%s, operationMetrics=%s where id=%s', all_update_metrics)
    conn.commit()
    cur.close()
  except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
  finally:
    if conn is not None:
      conn.close()

def update_regions_for_master_metrics(regions):
  all_update_metrics = []
  for region in regions:
    update_metrics = []
    update_metrics.append(str(region.readRequestsCountPerSec))
    update_metrics.append(str(region.writeRequestsCountPerSec))
    update_metrics.append(str(region.last_attempt_time).split('.')[0])
    update_metrics.append(str(region.memStoreSizeMB))
    update_metrics.append(str(region.storefileSizeMB))
    update_metrics.append(str(region.readRequestsCount))
    update_metrics.append(str(region.writeRequestsCount))
    update_metrics.append(str(region.requestsCount))
    update_metrics.append(str(region.region_server.id))
    update_metrics.append(str(region.id))
    all_update_metrics.append(update_metrics)

  conn = None
  try:
    conn=DBConnectionPool.connection()
    cur=conn.cursor()

    cur.executemany('update monitor_region set readRequestsCountPerSec=%s, writeRequestsCountPerSec=%s, last_attempt_time=%s, memStoreSizeMB=%s, storefileSizeMB=%s, readRequestsCount=%s, writeRequestsCount=%s, requestsCount=%s, region_server_id=%s where id=%s', all_update_metrics)
    conn.commit()
    cur.close()
  except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
  finally:
    if conn is not None:
      conn.close()


