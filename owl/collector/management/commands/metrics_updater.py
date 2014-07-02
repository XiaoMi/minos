import Queue
import datetime
import json
import logging
import os
import socket
import time
import traceback

from collect_utils import METRIC_TASK_TYPE, STATUS_TASK_TYPE, AGGREGATE_TASK_TYPE
from collect_utils import QueueTask
from django.db import connection
from monitor import dbutil
from monitor import metric_helper
from monitor.models import Region, RegionServer, Table, HBaseCluster

REGION_SERVER_DYNAMIC_STATISTICS_BEAN_NAME = "hadoop:service=RegionServer," \
  "name=RegionServerDynamicStatistics"
REGION_SERVER_BEAN_NAME = "hadoop:service=RegionServer,name=RegionServer"
REGION_SERVER_REPLICATION_BEAN_NAME_PREFIX = "hadoop:service=Replication," \
  "name=ReplicationSource for"

BOOL_METRIC_MAP = {
    "tag.IsOutOfSync": "true",
    "tag.HAState": "active",
}

HBASE_AGGREGATED_METRICS_KEY = ['memStoreSizeMB',
                                'storefileSizeMB',
                                'readRequestsCount',
                                'writeRequestsCount',
                                'readRequestsCountPerSec',
                                'writeRequestsCountPerSec',
                               ]

logger = logging.getLogger(__name__)

# global functions for subprocesses to handling metrics
def reset_aggregated_metrics(record):
  for key in HBASE_AGGREGATED_METRICS_KEY:
    setattr(record, key, 0)

def aggregate_metrics(from_record, to_record):
  for key in HBASE_AGGREGATED_METRICS_KEY:
    old_value = getattr(to_record, key)
    setattr(to_record, key, old_value + getattr(from_record, key))

def analyze_hbase_region_server_metrics(metric_task, metrics):
  region_server_name = None
  region_operation_metrics_dict = {}
  replication_metrics_dict = {}
  for bean in metrics['beans']:
    try:
      # because root and meta region have the names, we must use region server
      # name and region name to locate a region
      if bean['name'] == REGION_SERVER_BEAN_NAME:
        region_server_name = bean['ServerName']
      elif bean['name'] == REGION_SERVER_DYNAMIC_STATISTICS_BEAN_NAME:
        for metricName in bean.keys():
          if Region.is_region_operation_metric_name(metricName):
            encodeName = Region.get_encode_name_from_region_operation_metric_name(metricName)
            region_operation_metrics = region_operation_metrics_dict.setdefault(encodeName, {})
            region_operation_metrics[metricName] = bean[metricName]
      elif bean['name'].startswith(REGION_SERVER_REPLICATION_BEAN_NAME_PREFIX):
        peerId = metric_helper.parse_replication_source(bean['name'])
        replication_metrics = replication_metrics_dict.setdefault(peerId, {})
        for metricName in bean.keys():
          replication_metrics[metricName] = bean[metricName]
    except Exception as e:
      logger.warning("%r failed to analyze metrics: %r", metric_task, e)
      continue

  region_server = None
  if region_server_name is None:
    return
  else:
    try:
      region_server = RegionServer.objects.get(name = region_server_name)
    except RegionServer.DoesNotExist:
      logger.warning("%r failed to find region_server with region_server_name=%s",
        metric_task, region_server_name)
      return

  # save replication metrics for region server
  region_server.replication_last_attempt_time = metric_task.last_attempt_time
  region_server.replicationMetrics = json.dumps(replication_metrics_dict)
  region_server.save()

  region_record_need_save = []
  for encodeName, operationMetrics in region_operation_metrics_dict.iteritems():
    region_record = dbutil.get_region_by_regionserver_and_encodename(
      region_server, encodeName)
    # we must wait region saved after analyzing master task
    if region_record is None:
      continue
    region_record.analyze_from_region_server_operation_metrics(operationMetrics,
      metric_task.last_attempt_time)
    # we first buffer the regions needed to update, then do batch update
    region_record_need_save.append(region_record)

  # we do batch update
  begin = datetime.datetime.now()
  dbutil.update_regions_for_region_server_metrics(region_record_need_save)
  logger.info("%r batch save region record for region_server, " \
    "saved regions=%d, consume=%s",
    metric_task, len(region_record_need_save),
    str((datetime.datetime.now() - begin).total_seconds()))

def get_host_and_port_from_region_server_name(rs_name):
  # rs name format is formatted as : host_name,port,start_code.
  # for some cluster, the format may be : host_ip,port,start_code.
  # we will try to convert host_ip to coprresonding host_name
  # because we always try to save host_name and port to identity a task
  # except that we can't get host_name from host_ip
  tokens = rs_name.split(',')
  host = tokens[0] # may be host_name or host_ip
  host_name = None
  try:
    host_name = socket.gethostbyaddr(host)[0]
  except:
    logger.warning("can't get host_name for host=%s", host)
    host_name = host
  # jmx port is rs_port + 1, host and jmx port will identify a task
  port = int(tokens[1]) + 1
  return [host_name, port]

def analyze_hbase_master_metrics(metric_task, metrics):
  cluster = metric_task.job.cluster
  hbase_cluster_record, created = HBaseCluster.objects.get_or_create(cluster=cluster)
  reset_aggregated_metrics(hbase_cluster_record)
  tables = {}
  region_record_need_save = []
  for bean in metrics['beans']:
    try:
      if 'RegionServers' not in bean:
        continue
      for rs_metrics in bean['RegionServers']:
        rs_name = rs_metrics['key']
        [rs_hostname, rs_port] = get_host_and_port_from_region_server_name(rs_name)
        rs_task = dbutil.get_task_by_host_and_port(rs_hostname, rs_port)
        rs_record, created = RegionServer.objects.get_or_create(cluster = cluster,
                                                                task = rs_task)
        # region server name includes startTime, which means the same region server
        # will lead different RegionServer records if the region server restarts.
        # Therefore, we won't create region server by its name.
        rs_record.name = rs_name

        rs_value = rs_metrics['value']
        rs_record.last_attempt_time = metric_task.last_attempt_time
        rs_record.load = int(rs_value['load'])
        rs_record.numberOfRegions = int(rs_value['numberOfRegions'])
        reset_aggregated_metrics(rs_record)

        # we read out all regions belong to this region server and build a map
        all_regions_in_rs = Region.objects.filter(region_server = rs_record)
        all_regions_in_rs = dbutil.get_alive_regions_by_rs(rs_record)
        all_regions_map = {}
        logger.info("%r Finish get region: %d", metric_task, len(all_regions_in_rs))
        for region in all_regions_in_rs:
          all_regions_map[region.name] = region

        regionsLoad = rs_value['regionsLoad']
        for region_metrics in regionsLoad:
          region_value = region_metrics['value']
          region_name = region_value['nameAsString']
          try:
            table_name = region_name.split(',')[0]
          except Exception as e:
            logger.warning("%r failed to get region name: %r, %s",
              metric_task, e, region_name)
            continue

          region_metrics = {}

          if table_name not in tables:
            table_record, created = Table.objects.get_or_create(cluster = cluster,
              name = table_name)
            reset_aggregated_metrics(table_record)
            tables[table_name] = table_record

          table_record = tables[table_name]

          region_record = None
          if region_name in all_regions_map:
            region_record = all_regions_map[region_name]
          else:
            # if region record not in buffer, we get_or_create from db
            begin = datetime.datetime.now()
            region_record, created = Region.objects.get_or_create(table = table_record,
              name = region_name, encodeName = Region.get_encode_name(region_name),
              defaults={"region_server":rs_record})
            logger.info("%r get_or_create region in region_server from mysql, " \
              "consume=%s, region_name=%s, buffered_rs=%s, get_rs=%s",
              metric_task, str((datetime.datetime.now() - begin).total_seconds()),
              region_name, rs_record.name, region_record.region_server.name)


          logger.info("%r Finish analyze regionsLoad", metric_task)

          region_record.region_server = rs_record
          region_record.analyze_region_record(region_value,
            metric_task.last_attempt_time)
          # we buffer the regions needed update for batch update
          region_record_need_save.append(region_record)
          aggregate_metrics(region_record, rs_record)
          aggregate_metrics(region_record, table_record)
          aggregate_metrics(region_record, hbase_cluster_record)

        rs_record.save()

      for table_record in tables.itervalues():
        table_record.last_attempt_time = metric_task.last_attempt_time
        table_record.availability = dbutil.getTableAvailability(
          table_record.cluster.name, table_record.name)
        table_record.save()

      hbase_cluster_record.save()

      # do batch update
      begin = datetime.datetime.now()
      dbutil.update_regions_for_master_metrics(region_record_need_save)
      logger.info("%r batch save region record for master, " \
        "saved regions=%d, consume=%s",
        metric_task, len(region_record_need_save),
        str((datetime.datetime.now() - begin).total_seconds()))
    except Exception as e:
      traceback.print_exc()
      logger.warning("%r failed to analyze metrics: %r", metric_task, e)
      continue

def analyze_metrics(metric_task, metrics):
  if 'beans' not in metrics:
    return
  # analyze hbase metric
  if metric_task.job.cluster.service.name == 'hbase':
    start_time = time.time()
    if metric_task.job.name == 'master':
      analyze_hbase_master_metrics(metric_task, metrics)
    elif metric_task.job.name == 'regionserver':
      analyze_hbase_region_server_metrics(metric_task, metrics)

    logger.info("%r spent %f seconds for analyzing metrics for hbase",
      metric_task, time.time() - start_time)

def update_metrics_in_process(output_queue, metric_task):
  try:
    logger.info("Updating metrics in process %d", os.getpid())
    # get the metrics raw data from task.last_metrics_raw
    metricsRawData = metric_task.last_metrics_raw

    start_time = time.time()
    # analyze the metric if needed
    if metric_task.need_analyze:
      if metricsRawData:
        metrics = json.loads(metricsRawData)
        metrics_saved = {}
        for bean_output in metrics["beans"]:
          bean_name = bean_output["name"]
          for metric_name, metric_value in bean_output.iteritems():
            if metric_name in ["name", "modelerType"]:
              continue
            metric_type = type(metric_value)
            # Do some hadoop/hbase specific work :)
            if metric_name in BOOL_METRIC_MAP:
              metric_value = int(metric_value == BOOL_METRIC_MAP[metric_name])
            elif metric_type is list or metric_type is dict:
              # Just store the length.
              metric_value = len(metric_value)
            elif metric_type is bool:
              metric_value = int(metric_value)
            elif metric_value is None:
              metric_value = 0
            elif not (metric_type is int or metric_type is float
                      or metric_type is unicode or metric_type is str):
              logger.warning("Unexpected metric type %s/%s: %r/%r",
                  bean_name, metric_name, metric_type, metric_value)
              continue

            group = metrics_saved.setdefault(bean_name, {})
            group[metric_name] = metric_value
        metric_task.last_metrics = json.dumps(metrics_saved)

        analyze_metrics(metric_task, metrics)

    metric_task.save()
    logger.info("%r spent %f seconds for saving task status",
      metric_task, time.time() - start_time)
    # just put the corresponding metric_source id back to the output queue
    output_queue.put(QueueTask(METRIC_TASK_TYPE, metric_task.metric_source_id))
  except Exception, e:
    logger.warning("%r failed to update metric: %r", metric_task, e)
    traceback.print_exc()
