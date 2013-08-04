import argparse
import ConfigParser
import datetime
import json
import logging
import os
import random
import sys
import threading
import time
import urllib2

import deploy_utils

from optparse import make_option
from os import path

from django.db import transaction
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from monitor import dbutil
from monitor.models import Status
from monitor.models import Service, Cluster, Job, Task, RegionServer, Table, Region, HBaseCluster

from twisted.internet import reactor
from twisted.web import client

import socket

# now, we use multi-threads to read/write from/to db; if expect only one-thread, using the following config
#reactor.suggestThreadPoolSize(1)

# For debugging
import gc
import resource
import traceback

BOOL_METRIC_MAP = {
    "tag.IsOutOfSync": "true",
    "tag.HAState": "active",
}

REGION_SERVER_DYNAMIC_STATISTICS_BEAN_NAME = 'hadoop:service=RegionServer,name=RegionServerDynamicStatistics'
REGION_SERVER_BEAN_NAME = 'hadoop:service=RegionServer,name=RegionServer'

# TODO: move these suffix definition to monitor/metric_help.py
OPERATION_NUM_OPS = 'NumOps'
OPERATION_AVG_TIME = 'AvgTime'
OPERATION_MIN_TIME = 'MinTime'
OPERATION_MAX_TIME = 'MaxTime'
OPERATION_TOTAL_TIME = 'TotalTime'

logger = logging.getLogger(__name__)

class CollectorConfig:
  class Service:
    def __init__(self, options, config, name):
      # Parse service config.
      self.name = name
      self.jobs = config.get(name, "jobs").split()
      self.clusters = {}
      for cluster_name in config.get(name, "clusters").split():
        args = argparse.Namespace()
        args.service = self.name
        args.cluster = cluster_name
        # Parse cluster config.
        self.clusters[cluster_name] = deploy_utils.get_service_config(
            args, {}, False)
      self.metric_url = config.get(name, "metric_url")
      self.need_analyze = True # analyze for default
      if config.has_option(name, "need_analyze"):
        self.need_analyze = config.getboolean(name, "need_analyze")

  def __init__(self, args, options):
    # Parse collector config.
    config_path = os.path.join(deploy_utils.get_config_dir(), 'owl/collector.cfg')
    self.args = args
    self.options = options
    self.config = self.parse_config_file(config_path)
    self.services = {}
    for service_name in self.config.get("collector", "services").split():
      self.services[service_name] = CollectorConfig.Service(
          options, self.config, service_name)
    self.period = self.config.getint("collector", "period")

  def parse_config_file(self, config_path):
    config_parser = ConfigParser.SafeConfigParser()
    config_parser.optionxform = str
    logger.info("Parsing config file: %s", config_path)
    if not config_parser.read(config_path):
      logger.critical("Can't parse config file: %s", config_path)
      sys.exit(1)
    logger.info("Successfully parsed config file")
    return config_parser


class MetricObjectCache:
  cache = {}
  lock = threading.Lock()

  @classmethod
  def get(cls, group_name, metric_name):
    with cls.lock:
      # The group doesn't necessarily exist, set it to an empty dictionary if it
      # doesn't exist.
      group = cls.cache.setdefault(group_name, {})
      metric = group.get(metric_name, None)
      if not metric:
        logger.info(
            "cache missing for %s/%s, try to get or create from database",
            group_name, metric_name)
        # get_or_create is not thread safe. But in the Metric table, we set
        # group-metric to be unique. So only one save will be success, the
        # others will throw IntegrityError and get_or_create will catch it and
        # try to get again
        # NOTE: get_or_create seems to have problem with its internally using
        # of savepoint functions, now we call get_or_create after acquiring lock
        # and make sure it's executed sequentially.
        start_time = time.time()
        metric, created = Metric.objects.get_or_create(
            group=group_name, metric=metric_name)
        logger.info("%s/%s spent %f seconds, created: %r",
            group_name, metric_name, time.time() - start_time, created)
        group[metric_name] = metric
      return metric


class MetricSource:
  def __init__(self, collector_config, task):
    self.collector_config = collector_config
    self.task = task
    self.url = "http://%s:%d%s" % (
        task.host, task.port, task.job.cluster.service.metric_url)
    self.need_analyze = collector_config.services[task.job.cluster.service.name].need_analyze
    self.aggregated_metrics_key = ['memStoreSizeMB',
                                   'storefileSizeMB',
                                   'readRequestsCount',
                                   'writeRequestsCount',
                                   'readRequestsCountPerSec',
                                   'writeRequestsCountPerSec',
                                  ]

  def schedule_next_fetch(self):
    next_time = self.start_time + self.collector_config.period
    end_time = time.time()
    if end_time < next_time:
      wait_time = next_time - end_time
      logger.info(
          "%r waiting %f seconds for %s..." , self.task, wait_time, self.url)

      # reactor.callLater is NOT thread-safe but reactor.callFromThread is, so
      # we put the callLater to the main loop.
      reactor.callFromThread(reactor.callLater, wait_time, self.fetch_metrics)
    else:
      # We are behind the schedule, fetch the metrics right away.
      reactor.callFromThread(self.fetch_metrics)

  def fetch_metrics(self):
    logger.info("%r fetching %s...", self.task, self.url)
    self.start_time = time.time()
    # Always use utc time with timezone info, see:
    # https://docs.djangoproject.com/en/1.4/topics/i18n/timezones/#naive-and-aware-datetime-objects
    self.task.last_attempt_time = datetime.datetime.utcfromtimestamp(
        self.start_time).replace(tzinfo=timezone.utc)
    client.getPage(str(self.url),
        timeout=self.collector_config.period - 1,
        followRedirect=False).addCallbacks(
            callback=self.success_callback,
            errback=self.error_callback)

  def success_callback(self, data):
    logger.info("%r fetched %d bytes", self.task, len(data))
    try:
      # Save the raw data before passing it, in case the data is invalid and
      # throws an exception.
      self.task.last_metrics_raw = data
      self.task.last_status = Status.OK
      self.task.last_message = "Success"
      self.task.last_success_time = self.task.last_attempt_time
      self.update_metrics(data)

    except Exception as e:
      logger.warning("%r failed to process result: %r", self.task, e)
      self.schedule_next_fetch()

  def error_callback(self, error):
    logger.warning("%r failed to fetch: %r", self.task, error)
    try:
      self.task.last_status = Status.ERROR
      self.task.last_message = "Error: %r" % error
      self.update_metrics(None)
    except Exception as e:
      logger.warning("%r failed to process error: %r", self.task, e)
      self.schedule_next_fetch()

  def analyze_metrics(self, metrics):
    if 'beans' not in metrics:
      return
    # analyze hbase metric
    if self.task.job.cluster.service.name == 'hbase':
      if self.task.job.name == 'master':
        self.analyze_hbase_master_metrics(metrics)
      elif self.task.job.name == 'regionserver':
        self.analyze_hbase_region_server_metrics(metrics)

  def analyze_hbase_region_server_metrics(self, metrics):
    region_server_name = None
    region_operation_metrics_dict = {}
    for bean in metrics['beans']:
      try:
        # because root and meta region have the names, we must use region server
        # name and region name to locate a region
        if bean['name'] == REGION_SERVER_BEAN_NAME:
          region_server_name = bean['ServerName']
          continue

        if bean['name'] != REGION_SERVER_DYNAMIC_STATISTICS_BEAN_NAME:
          continue
        for metricName in bean.keys():
          if Region.is_region_operation_metric_name(metricName):
            encodeName = Region.get_encode_name_from_region_operation_metric_name(metricName)
            region_operation_metrics = region_operation_metrics_dict.setdefault(encodeName, {})
            region_operation_metrics[metricName] = bean[metricName]
        break
      except Exception as e:
        logger.warning("%r failed to analyze metrics: %r", self.task, e)
        continue

    region_server = None
    if region_server_name is None:
      return
    else:
      try:
        region_server = RegionServer.objects.get(name = region_server_name)
      except RegionServer.DoesNotExist:
        logger.warning("%r failed to find region_server with region_server_name=%s", self.task, region_server_name)
        return

    region_record_need_save = []
    for encodeName, operationMetrics in region_operation_metrics_dict.iteritems():
      region_record = dbutil.get_region_by_regionserver_and_encodename(region_server, encodeName)
      # we must wait region saved after analyzing master task
      if region_record is None:
        continue
      region_record.analyze_from_region_server_operation_metrics(operationMetrics,
                                                                 self.task.last_attempt_time)
      # we first buffer the regions needed to update, then do batch update
      region_record_need_save.append(region_record)

    # we do batch update
    begin = datetime.datetime.now()
    dbutil.update_regions_for_region_server_metrics(region_record_need_save)
    logger.info("%r batch save region record for region_server, saved regions=%d, consume=%s",
        self.task, len(region_record_need_save), str((datetime.datetime.now() - begin).total_seconds()))

  def get_host_and_port_from_region_server_name(self, rs_name):
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

  def analyze_hbase_master_metrics(self, metrics):
    cluster = self.task.job.cluster
    hbase_cluster_record, created = HBaseCluster.objects.get_or_create(cluster = cluster)
    self.reset_aggregated_metrics(hbase_cluster_record)
    tables = {}
    region_record_need_save = []
    for bean in metrics['beans']:
      try:
        if 'RegionServers' not in bean:
          continue
        for rs_metrics in bean['RegionServers']:
          rs_name = rs_metrics['key']
          [rs_hostname, rs_port] = self.get_host_and_port_from_region_server_name(rs_name)
          rs_task = dbutil.get_task_by_host_and_port(rs_hostname, rs_port)
          rs_record, created = RegionServer.objects.get_or_create(cluster = cluster,
                                                                  task = rs_task)
          # region server name includes startTime, which means the same region server
          # will lead different RegionServer records if the region server restarts.
          # Therefore, we won't create region server by its name.
          rs_record.name = rs_name

          rs_value = rs_metrics['value']
          rs_record.last_attempt_time = self.task.last_attempt_time
          rs_record.load = int(rs_value['load'])
          rs_record.numberOfRegions = int(rs_value['numberOfRegions'])
          self.reset_aggregated_metrics(rs_record)

          # we read out all regions belong to this region server and build a map
          all_regions_in_rs = Region.objects.filter(region_server = rs_record)
          all_regions_map = {}
          for region in all_regions_in_rs:
            all_regions_map[region.name] = region

          regionsLoad = rs_value['regionsLoad']
          for region_metrics in regionsLoad:
            region_value = region_metrics['value']
            region_name = region_value['nameAsString']
            table_name, startkey, region_id = region_name.split(',')
            region_metrics = {}

            if table_name not in tables:
              table_record, created = Table.objects.get_or_create(cluster = cluster,
                                                                  name = table_name)
              self.reset_aggregated_metrics(table_record)
              tables[table_name] = table_record

            table_record = tables[table_name]

            region_record = None
            if region_name in all_regions_map:
              region_record = all_regions_map[region_name]
            else:
              # if region record not in buffer, we get_or_create from db
              begin = datetime.datetime.now()
              region_record, created = Region.objects.get_or_create(table = table_record,
                                                                    name = region_name,
                                                                    encodeName = Region.get_encode_name(region_name),
                                                                    defaults={"region_server":rs_record})
              logger.info("%r get_or_create region in region_server from mysql, consume=%s, region_name=%s, buffered_rs=%s, get_rs=%s",
                self.task, str((datetime.datetime.now() - begin).total_seconds()), region_name, rs_record.name, region_record.region_server.name)


            region_record.region_server = rs_record
            region_record.analyze_region_record(region_value, self.task.last_attempt_time)
            # we buffer the regions needed update for batch update
            region_record_need_save.append(region_record)
            self.aggregate_metrics(region_record, rs_record)
            self.aggregate_metrics(region_record, table_record)
            self.aggregate_metrics(region_record, hbase_cluster_record)

          rs_record.save()

        for table_record in tables.itervalues():
          table_record.last_attempt_time = self.task.last_attempt_time
          table_record.availability = dbutil.getTableAvailability(table_record.cluster.name, table_record.name)
          table_record.save()

        hbase_cluster_record.save()

        # do batch update
        begin = datetime.datetime.now()
        dbutil.update_regions_for_master_metrics(region_record_need_save)
        logger.info("%r batch save region record for master, saved regions=%d, consume=%s", self.task,
            len(region_record_need_save), str((datetime.datetime.now() - begin).total_seconds()))
      except Exception as e:
        traceback.print_exc()
        logger.warning("%r failed to analyze metrics: %r", self.task, e)
        continue

  def reset_aggregated_metrics(self, record):
    for key in self.aggregated_metrics_key:
      setattr(record, key, 0)

  def aggregate_metrics(self, from_record, to_record):
    for key in self.aggregated_metrics_key:
      old_value = getattr(to_record, key)
      setattr(to_record, key, old_value + getattr(from_record, key))

  def update_metrics(self, metricsRawData):
    try:
      reactor.callInThread(self.update_metrics_in_thread, metricsRawData)
    except Eception, e:
      self.schedule_next_fetch()

  def update_metrics_in_thread(self, metricsRawData):
    try:
      logger.info("%r updating metrics, "
          "%d task in queue, %d workers, %d total threads",
          self.task,
          reactor.getThreadPool().q.qsize(),
          len(reactor.getThreadPool().working),
          len(reactor.getThreadPool().threads))

      start_time = time.time()
      # analyze the metric if needed
      if self.need_analyze:
        if metricsRawData:
          metrics = json.loads(metricsRawData)
          metrics_saved = {}
          for bean_output in metrics["beans"]:
            bean_name = bean_output["name"]
            for metric_name, metric_value in bean_output.iteritems():
              if metric_name in ["name", "modelerType"]: continue

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

              # TODO: comment this out temporarily, remove it forever if we don't
              # want to use it.
              #metric = MetricObjectCache.get(bean_name, metric_name)
              group = metrics_saved.setdefault(bean_name, {})
              group[metric_name] = metric_value
          self.task.last_metrics = json.dumps(metrics_saved)

          self.analyze_metrics(metrics)

      self.task.save()
      logger.info("%r spent %f seconds for saving task status",
                  self.task, time.time() - start_time)
    except Exception, e:
      logger.warning("%r failed to update metric: %r", self.task, e)
      traceback.print_exc()
    finally:
      self.schedule_next_fetch()

# Region operation include : get, multiput, multidelete, checkAndPut, BulkDelete etc.
# one region operation include operation_NumOps, operation_AvgTime, operation_MaxTime and
# operation.MinTime. We aggregate operation metrics of regions to compute operation metrics
# for table and cluster
class RegionOperationMetricAggregator:
  def __init__(self, collector_config):
    self.collector_config = collector_config

  def aggregate_region_operation_metric(self):
    reactor.callInThread(self.aggregate_region_operation_metric_in_thread)

  def aggregate_region_operation_metric_in_thread(self):
    try:
      self.last_aggregate_time = time.time()
      self.aggregate_region_operation_metric_for_table_and_cluster()
    except Exception as e:
      logger.warning("failed to aggregate region operation metric:%r", e)
    finally:
      self.schedule_next_aggregation()
    return

  def make_empty_operation_metric(self):
    operationMetric = {}
    operationMetric[OPERATION_NUM_OPS] = 0
    operationMetric[OPERATION_TOTAL_TIME] = 0
    operationMetric[OPERATION_MAX_TIME] = 0
    operationMetric[OPERATION_MIN_TIME] = sys.maxint
    return operationMetric

  def aggregate_one_region_operation_metric(self, aggregateMetric, deltaMetric):
    if OPERATION_NUM_OPS in deltaMetric:
      aggregateMetric[OPERATION_NUM_OPS] += deltaMetric[OPERATION_NUM_OPS]
      aggregateMetric[OPERATION_TOTAL_TIME] += deltaMetric[OPERATION_AVG_TIME] * deltaMetric[OPERATION_NUM_OPS]
      if aggregateMetric[OPERATION_MAX_TIME] < deltaMetric[OPERATION_MAX_TIME]:
        aggregateMetric[OPERATION_MAX_TIME] = deltaMetric[OPERATION_MAX_TIME]
      if aggregateMetric[OPERATION_MIN_TIME] > deltaMetric[OPERATION_MIN_TIME]:
        aggregateMetric[OPERATION_MIN_TIME] = deltaMetric[OPERATION_MIN_TIME]

  def compute_avg_time_and_num_ops_after_aggregation(self, operationMetrics):
    for operationName in operationMetrics.keys():
      if operationMetrics[operationName][OPERATION_NUM_OPS] > 0:
        # now, region operation metric will be collect every 10 seconds, the orignal ops is the sum of ops during 10 seconds
        operationMetrics[operationName][OPERATION_AVG_TIME] = \
          operationMetrics[operationName][OPERATION_TOTAL_TIME] / operationMetrics[operationName][OPERATION_NUM_OPS]
        operationMetrics[operationName][OPERATION_NUM_OPS] = operationMetrics[operationName][OPERATION_NUM_OPS] / 10
      else:
        operationMetrics[operationName][OPERATION_AVG_TIME] = 0

  def aggregate_region_operation_metric_for_table_and_cluster(self):
    allClusterOperationMetric = {}
    # because the number of regions could be huge. We read out region operation metrics
    # by table, then table operation metrics and cluster operation metrics could be aggregated
    tables = Table.objects.all()
    for table in tables:
      clusterName = table.cluster.name
      clusterOperationMetric = allClusterOperationMetric.setdefault(clusterName, {})
      tableOperationMetric = {}
      regions = dbutil.get_region_by_table(table)
      logger.info(
          "TableOperationMetricAggregation aggregate %d regions metric for table %s, cluster %s" ,
           len(regions), table.name, clusterName)

      for region in regions:
        if region.operationMetrics is None or region.operationMetrics == '':
          continue;
        regionOperationMetrics = json.loads(region.operationMetrics)
        for regionOperationName in regionOperationMetrics.keys():
          regionOperation = regionOperationMetrics[regionOperationName]
          self.aggregate_one_region_operation_metric(tableOperationMetric.setdefault(regionOperationName,
                                                    self.make_empty_operation_metric()), regionOperation)
          self.aggregate_one_region_operation_metric(clusterOperationMetric.setdefault(regionOperationName,
                                                     self.make_empty_operation_metric()), regionOperation)

      # compute avgTime for table operation metrics
      self.compute_avg_time_and_num_ops_after_aggregation(tableOperationMetric)
      table.operationMetrics = json.dumps(tableOperationMetric)
      table.save()

    # compute avgTime for clusetr operation metrics
    clusters = HBaseCluster.objects.all()
    for cluster in clusters:
      clusterName = cluster.cluster.name
      if clusterName in allClusterOperationMetric:
        clusterOperationMetric = allClusterOperationMetric[clusterName]
        self.compute_avg_time_and_num_ops_after_aggregation(clusterOperationMetric)
        cluster.operationMetrics = json.dumps(clusterOperationMetric)
        cluster.save()
    return

  def schedule_next_aggregation(self):
    next_time = self.last_aggregate_time + self.collector_config.period
    end_time = time.time()
    if end_time < next_time:
      wait_time = next_time - end_time
      logger.info(
          "RegionOperationMetricAggregator waiting %f seconds for aggregation" , wait_time)

      reactor.callFromThread(reactor.callLater, wait_time, self.aggregate_region_operation_metric)
    else:
      reactor.callFromThread(self.aggregate_region_operation_metric)
    return

class StatusUpdater:
  """Update status of all active clusters and jobs, which are inferred from
  tasks' status."""

  def __init__(self, collector_config):
    self.collector_config = collector_config

  def get_latest_metric(self, task, group_name, metric_name):
    try:
      metric = json.loads(task.last_metrics)
      return metric[group_name][metric_name]
    except Exception as e:
      logger.warning("%r failed to get metric: %r", task, e)
      return 0

  def is_namenode_active(self, task):
    try:
      metric = self.get_latest_metric(
          task, "Hadoop:service=NameNode,name=FSNamesystem", "tag.HAState")
      return bool(metric)
    except Exception as e:
      logger.warning("%r failed to get metric: %r", task, e)
      return False

  def is_master_active(self, task):
    try:
      metric = self.get_latest_metric(
          task, "hadoop:service=Master,name=Master", "IsActiveMaster")
      return bool(metric)
    except Exception as e:
      logger.warning("%r failed to get metric: %r", task, e)
      return False

  def update_hdfs_cluster_status(self, cluster):
    job = cluster.jobs["journalnode"]
    if (job.running_tasks_count < 2 or
        job.running_tasks_count < (job.total_tasks_count / 2 + 1)):
      job.last_status = Status.ERROR
      job.last_message = "Too few running journalnodes!"

    job = cluster.jobs["namenode"]
    if job.running_tasks_count < 1:
      job.last_status = Status.ERROR
      job.last_message = "No running namenodes!"
    else:
      active = 0
      for task in job.running_tasks.itervalues():
        if self.is_namenode_active(task):
          # update cluster entry
          cluster.entry = '%s:%d' % (task.host, task.port)
          cluster.version = self.get_latest_metric(task,
                                                   'Hadoop:service=NameNode,name=NameNodeInfo',
                                                   'Version')
          active += 1
      if active > 1:
        job.last_status = Status.ERROR
        job.last_message = "Too many active namenodes!"
      elif active < 1:
        job.last_status = Status.ERROR
        job.last_message = "No active namenodes!"
      elif job.running_tasks_count < 2:
        job.last_status = Status.WARN
        job.last_message = "Less than 2 running namenodes, no HA guarantee"

    job = cluster.jobs["datanode"]
    if job.running_tasks_count < 3:
      job.last_status = Status.ERROR
      job.last_message = "Too few running datanodes!"
    cluster.last_status = max([job.last_status for job in cluster.jobs.itervalues()])

  def update_hbase_cluster_status(self, cluster):
    job = cluster.jobs["master"]
    if job.running_tasks_count < 1:
      job.last_status = Status.ERROR
      job.last_message = "No running masters!"
    else:
      active = 0
      for task in job.running_tasks.itervalues():
        if self.is_master_active(task):
          # update cluster entry
          cluster.entry = '%s:%d' % (task.host, task.port)
          version = self.get_latest_metric(task,
                                           'hadoop:service=HBase,name=Info',
                                           'version')
          revision = self.get_latest_metric(task,
                                           'hadoop:service=HBase,name=Info',
                                           'revision')
          cluster.version = '%s, r%s' % (version, revision)
          active += 1
      if active > 1:
        job.last_status = Status.ERROR
        job.last_message = "Too many active masters!"
      elif active < 1:
        job.last_status = Status.ERROR
        job.last_message = "No active masters!"
      elif job.running_tasks_count < 2:
        # TODO: Now it always reports warning as backup master doesn't run a http
        # server before it acquires zk lock. Comment this out and would change
        # master's startup workflow.
        #job.last_status = Status.WARN
        #job.last_message = "Less than 2 running masters, no HA guarantee"
        pass

    job = cluster.jobs["regionserver"]
    if job.running_tasks_count < 3:
      job.last_status = Status.ERROR
      job.last_message = "Too few running regionservers!"
    cluster.last_status = max([job.last_status for job in cluster.jobs.itervalues()])

  def update_yarn_cluster_status(self, cluster):
    job = cluster.jobs["resourcemanager"]
    for task in job.running_tasks.itervalues():
      # update cluster entry
      cluster.entry = '%s:%d' % (task.host, task.port)
    if job.running_tasks_count < 1:
      job.last_status = Status.ERROR
      job.last_message = "No running resourcemanager!"

    job = cluster.jobs["proxyserver"]
    if job.running_tasks_count < 1:
      job.last_status = Status.ERROR
      job.last_message = "No running proxyserver!"

    job = cluster.jobs["nodemanager"]
    if job.running_tasks_count < 3:
      job.last_status = Status.ERROR
      job.last_message = "Too few running nodemanager!"
    cluster.last_status = max([job.last_status for job in cluster.jobs.itervalues()])

    job = cluster.jobs["historyserver"]
    if job.running_tasks_count < 1:
      job.last_status = Status.ERROR
      job.last_message = "Too few running historyserver!"
    cluster.last_status = max([job.last_status for job in cluster.jobs.itervalues()])

  def update_impala_cluster_status(self, cluster):
    job = cluster.jobs["statestored"]
    for task in job.running_tasks.itervalues():
      # update cluster entry
      cluster.entry = '%s:%d' % (task.host, task.port)
    if job.running_tasks_count < 1:
      job.last_status = Status.ERROR
      job.last_message = "No running statestored!"

    job = cluster.jobs["impalad"]
    if job.running_tasks_count < 3:
      job.last_status = Status.ERROR
      job.last_message = "Too few running impalad!"
    cluster.last_status = max([job.last_status for job in cluster.jobs.itervalues()])

  def update_cluster_status(self, cluster):
    cluster.jobs = {}
    cluster.last_attempt_time = datetime.datetime.utcfromtimestamp(
        self.start_time).replace(tzinfo=timezone.utc)
    cluster.last_status = Status.OK
    cluster.last_message = ""

    for job in cluster.job_set.all():
      job.running_tasks = {}
      job.tasks = {}
      job.last_attempt_time = cluster.last_attempt_time
      job.last_status = Status.OK
      job.last_message = ""
      job.running_tasks_count = 0
      job.total_tasks_count = 0
      for task in job.task_set.filter(active=True):
        if task.health:
          job.running_tasks[task.id] = task
          job.running_tasks_count += 1
        job.total_tasks_count += 1
      cluster.jobs[job.name] = job

    service_handler = {
        "hdfs": self.update_hdfs_cluster_status,
        "hbase": self.update_hbase_cluster_status,
        "yarn": self.update_yarn_cluster_status,
        "impala": self.update_impala_cluster_status,
    }
    service_handler[cluster.service.name](cluster)

    for job in cluster.jobs.itervalues():
      if job.last_status < Status.ERROR:
        # OK or WARN
        job.last_success_time = job.last_attempt_time
      job.save()

    if cluster.last_status < Status.ERROR:
      # OK or WARN
      cluster.last_success_time = job.last_attempt_time
    cluster.save()

  def update_status(self):
    reactor.callInThread(self.update_status_in_thread)

  def update_status_in_thread(self):
    # TODO: make sure performance is not a problem as current approach queries
    # database many times.
    """
    tasks = get_tasks_by_service(service_id)
    clusters = []
    for task in tasks:
      if task.job.cluster not in clusters:
        clusters.append(task.job.cluster)
    """
    logger.info("updating clusters status, "
        "%d task in queue, %d workers, %d total threads",
        reactor.getThreadPool().q.qsize(),
        len(reactor.getThreadPool().working),
        len(reactor.getThreadPool().threads))

    self.start_time = time.time()
    for cluster in Cluster.objects.filter(active=True).all():
      self.update_cluster_status(cluster)
    logger.info("spent %f seconds for updating clusters status",
        time.time() - self.start_time)
    logger.info("gc: %r", gc.get_count())
    logger.info("usage: %r", resource.getrusage(resource.RUSAGE_SELF))

    # reactor.callLater is NOT thread-safe but reactor.callFromThread is, so we
    # put the callLater to the main loop.
    reactor.callFromThread(
        reactor.callLater, self.collector_config.period, self.update_status)


class Command(BaseCommand):
  args = ''
  help = "Run the background collector to fetch metrics from /jmx on each server."

  option_list = BaseCommand.option_list + (
      make_option(
        "--use_threadpool",
        action="store_true",
        default=False,
        help="Use thread pool to store metrics to database if the flag is on."),
  )

  def handle(self, *args, **options):
    gc.set_debug(gc.DEBUG_STATS)

    self.args = args
    self.options = options

    self.stdout.write("args: %r\n" % (args, ))
    self.stdout.write("options: %r\n" % options)

    self.collector_config = CollectorConfig(self.args, self.options)
    self.update_active_tasks()
    self.region_operation_aggregator = RegionOperationMetricAggregator(self.collector_config)
    # we start to aggregate region operation metric after one period
    reactor.callLater(self.collector_config.period + 1,
        self.region_operation_aggregator.aggregate_region_operation_metric)
    self.fetch_metrics()

  def update_active_tasks(self):
    # Mark all current tasks as deactive.
    Service.objects.all().update(active=False)
    Cluster.objects.all().update(active=False)
    Job.objects.all().update(active=False)
    Task.objects.all().update(active=False)

    # Add all active tasks
    self.metric_sources = []
    for service_name, service in self.collector_config.services.iteritems():
      # Save to database.
      # The active field has the default value True.
      service_record, created = Service.objects.get_or_create(
          name=service_name,
          defaults={"metric_url":service.metric_url})
      if not created:
        # Mark it as active if it exists.
        service_record.active = True
        service_record.save()

      for cluster_name, cluster in service.clusters.iteritems():
        cluster_record, created = Cluster.objects.get_or_create(
            service=service_record, name=cluster_name)
        if not created:
          cluster_record.active = True
          cluster_record.save()

        for job_name in service.jobs:
          job_record, created = Job.objects.get_or_create(
              cluster=cluster_record, name=job_name)
          if not created:
            job_record.active = True
            job_record.save()

          job = cluster.jobs[job_name]
          # We assume http port is always base_port + 1
          port = job.base_port + 1
          for task_id, host in job.hostnames.iteritems():
            task_record, created = Task.objects.get_or_create(
                job=job_record, task_id=task_id,
                defaults={"host":host, "port":port})
            if not created or task_record.host != host or task_record.port != port:
              task_record.active = True
              task_record.host = host
              task_record.port = port
              task_record.save()
            self.metric_sources.append(
                MetricSource(self.collector_config, task_record))

  def fetch_metrics(self):
    for metric_source in self.metric_sources:
      # Randomize the start time of each metric source.
      # Because StatusUpdater will always update cluster status every 'self.collector_config.period',
      # here, we use 'self.collector_config.period - 2' to give each task at least 2 seconds to
      # download page and update its status into database before StatusUpdater starting to update cluster
      # status based on each task's status
      wait_time = random.uniform(0, self.collector_config.period - 2)
      logger.info(
          "%r waiting %f seconds for %s..." ,
          metric_source.task, wait_time, metric_source.url)
      reactor.callLater(wait_time, metric_source.fetch_metrics)

    status_updater = StatusUpdater(self.collector_config)
    reactor.callLater(self.collector_config.period + 1,
        status_updater.update_status)

    reactor.run()
