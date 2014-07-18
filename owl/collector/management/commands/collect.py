import ConfigParser
import Queue
import argparse
import datetime
import json
import logging
import multiprocessing
import os
import random
import sys
import threading
import time
import urllib2

import deploy_utils

from optparse import make_option
from os import path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db import transaction
from django.utils import timezone

from monitor import dbutil
from monitor.models import Service, Cluster, Job, Task
from monitor.models import Status

from twisted.internet import reactor
from twisted.web import client

# For debugging
import gc

from metrics_updater import update_metrics_in_process
from status_updater import update_status_in_process
from metrics_aggregator import aggregate_region_operation_metric_in_process
from collect_utils import QueueTask
from collect_utils import METRIC_TASK_TYPE, STATUS_TASK_TYPE, AGGREGATE_TASK_TYPE

# the number of multiprocesses
PROCESS_NUM = 6

QUEUE_TASK_CALLBACK = {
  METRIC_TASK_TYPE: update_metrics_in_process,
  STATUS_TASK_TYPE: update_status_in_process,
  AGGREGATE_TASK_TYPE: aggregate_region_operation_metric_in_process, 
}

logger = logging.getLogger(__name__)

def process_queue_task(input_queue, output_queue):
  connection.close()
  while True:
    try:
      queue_task = input_queue.get(timeout=0.5)
      QUEUE_TASK_CALLBACK[queue_task.task_type](output_queue,
        queue_task.task_data)
    except Queue.Empty:
      logger.warning("Input Queue is empty in process %d." % os.getpid())
      continue

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
        self.clusters[cluster_name] = deploy_utils.get_service_config(args)
      self.metric_url = config.get(name, "metric_url")
      self.need_analyze = True # analyze for default
      if config.has_option(name, "need_analyze"):
        self.need_analyze = config.getboolean(name, "need_analyze")

  def __init__(self, args, options):
    # Parse collector config.
    self.options = options
    config_path = os.path.join(deploy_utils.get_config_dir(), 'owl',
      self.options['collector_cfg'])
    self.args = args
    self.config = self.parse_config_file(config_path)
    self.services = {}
    for service_name in self.config.get("collector", "services").split():
      self.services[service_name] = CollectorConfig.Service(options,
        self.config, service_name)
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

class MetricSource:
  def __init__(self, collector_config, task):
    self.collector_config = collector_config
    self.task = task
    self.url = "http://%s:%d%s" % (
      task.host, task.port,
      self.collector_config.config.get(task.job.cluster.service.name, "metric_url"))
    self.need_analyze = collector_config.services[task.job.cluster.service.name].need_analyze

  def schedule_next_fetch(self, input_queue):
    next_time = self.start_time + self.collector_config.period
    end_time = time.time()
    if end_time < next_time:
      wait_time = next_time - end_time
      logger.info("%r waiting %f seconds for %s..." ,
        self.task, wait_time, self.url)
      # reactor.callLater is NOT thread-safe but reactor.callFromThread is, so
      # we put the callLater to the main loop.
      reactor.callFromThread(reactor.callLater, wait_time,
        self.fetch_metrics, input_queue)
    else:
      # We are behind the schedule, fetch the metrics right away.
      reactor.callFromThread(self.fetch_metrics, input_queue)

  def fetch_metrics(self, input_queue):
    logger.info("%r fetching %s...", self.task, self.url)
    self.start_time = time.time()
    # Always use utc time with timezone info, see:
    # https://docs.djangoproject.com/en/1.4/topics/i18n/timezones/#naive-and-aware-datetime-objects
    self.task.last_attempt_time = datetime.datetime.utcfromtimestamp(
      self.start_time).replace(tzinfo=timezone.utc)
    client.getPage(str(self.url), timeout=self.collector_config.period - 1,
      followRedirect=False).addCallbacks(
      callback=self.success_callback, errback=self.error_callback,
      callbackArgs=[input_queue], errbackArgs=[input_queue])

  def success_callback(self, data, input_queue):
    logger.info("%r fetched %d bytes", self.task, len(data))
    try:
      # Save the raw data before passing it, in case the data is invalid and
      # throws an exception.
      self.task.last_metrics_raw = data
      self.task.last_status = Status.OK
      self.task.last_message = "Success"
      self.task.last_success_time = self.task.last_attempt_time
      input_queue.put(QueueTask(METRIC_TASK_TYPE, self.task))
    except Exception as e:
      logger.warning("%r failed to process result: %r", self.task, e)
      self.schedule_next_fetch(input_queue)

  def error_callback(self, error, input_queue):
    logger.warning("%r failed to fetch: %r", self.task, error)
    try:
      self.task.last_metrics_raw = None
      self.task.last_status = Status.ERROR
      self.task.last_message = "Error: %r" % error
      input_queue.put(QueueTask(METRIC_TASK_TYPE, self.task))
    except Exception as e:
      logger.warning("%r failed to process error: %r", self.task, e)
      self.schedule_next_fetch(input_queue)

# Region operation include : get, multiput, multidelete, checkAndPut, BulkDelete etc.
# one region operation include operation_NumOps, operation_AvgTime, operation_MaxTime and
# operation.MinTime. We aggregate operation metrics of regions to compute operation metrics
# for table and cluster
class RegionOperationMetricAggregator:
  def __init__(self, collector_config):
    self.collector_config = collector_config

  def produce_aggregate_task(self, input_queue):
    reactor.callInThread(self.produce_aggregate_task_in_thread, input_queue)

  def produce_aggregate_task_in_thread(self, input_queue):
    try:
      input_queue.put(QueueTask(AGGREGATE_TASK_TYPE, None))
    except Exception as e:
      logger.warning("Failed to produce aggregate task %r", e)
    finally:
      self.schedule_next_aggregation(input_queue)

  def schedule_next_aggregation(self, input_queue):
    wait_time = self.collector_config.period
    reactor.callFromThread(reactor.callLater, wait_time,
      self.produce_aggregate_task, input_queue)

class StatusUpdater:
  """
  Update status of all active clusters and jobs, which are inferred from
  tasks' status.
  """
  def __init__(self, collector_config):
    self.collector_config = collector_config

  def produce_status_update_task(self, input_queue):
    reactor.callInThread(self.produce_status_update_task_in_thread, input_queue)

  def produce_status_update_task_in_thread(self, input_queue):
    try:
      input_queue.put(QueueTask(STATUS_TASK_TYPE, None))
    except Exception as e:
      logger.warning("Failed to produce status updater task %r", e)
    finally:
      self.schedule_next_status_update(input_queue)

  def schedule_next_status_update(self, input_queue):
    wait_time = self.collector_config.period
    reactor.callFromThread(reactor.callLater, wait_time,
      self.produce_status_update_task, input_queue)

class Command(BaseCommand):
  args = ''
  help = "Run the background collector to fetch metrics from /jmx on each server."

  option_list = BaseCommand.option_list + (
      make_option(
        "--use_threadpool",
        action="store_true",
        default=False,
        help="Use thread pool to store metrics to database if the flag is on."),
      make_option(
        "--collector_cfg",
        default="collector.cfg",
        help="Specify collector configuration file"
      ),
      make_option(
        "--clear_old_tasks",
        action="store_true",
        default=False,
        help="Set true for clear old tasks"
      ),
  )

  def handle(self, *args, **options):
    gc.set_debug(gc.DEBUG_STATS)

    self.args = args
    self.options = options

    self.stdout.write("args: %r\n" % (args, ))
    self.stdout.write("options: %r\n" % options)

    self.collector_config = CollectorConfig(self.args, self.options)
    if self.options['clear_old_tasks']:
      self.clear_old_tasks()

    self.update_active_tasks()

    self.input_queue = multiprocessing.Queue()
    self.output_queue = multiprocessing.Queue()

    for idx in range(PROCESS_NUM):
      multiprocessing.Process(target=process_queue_task,
        args=(self.input_queue, self.output_queue)).start()

    self.fetch_metrics()

  def clear_old_tasks():
    # Mark all current tasks as deactive.
    Service.objects.all().update(active=False)
    Cluster.objects.all().update(active=False)
    Job.objects.all().update(active=False)
    Task.objects.all().update(active=False)

  def update_active_tasks(self):
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
          # support multiple instances
          hosts = job.hosts
          for host_id, host in hosts.iteritems():
            host_name = job.hostnames[host_id]
            for instance_id in range(host.instance_num):
              task_id = deploy_utils.get_task_id(hosts, host_id, instance_id)
              instance_port = deploy_utils.get_base_port(port,instance_id)
              task_record, created = Task.objects.get_or_create(
                job=job_record, task_id=task_id,
                defaults={"host":host_name, "port":instance_port})
              if not created or task_record.host != host_name or (
                task_record.port != instance_port):
                task_record.active = True
                task_record.host = host_name
                task_record.port = instance_port
                task_record.save()
              self.metric_sources.append(
                MetricSource(self.collector_config, task_record))

  def consume_processed_result(self):
    while True: # get all the task in output queue
      try:
        queue_task = self.output_queue.get(timeout=0.5)
        if queue_task.task_type == METRIC_TASK_TYPE:
          metric_source_id = queue_task.task_data
          self.metric_sources[metric_source_id].schedule_next_fetch(self.input_queue)
      except Queue.Empty:
        logger.warning('Output Queue is empty.')
        continue

  def schedule_next_rolling(self):
    reactor.callInThread(self.consume_processed_result)

  def fetch_metrics(self):
    for index, metric_source in enumerate(self.metric_sources):
      # Randomize the start time of each metric source.
      # Because StatusUpdater will always update cluster status every 'self.collector_config.period',
      # here, we use 'self.collector_config.period - 2' to give each task at least 2 seconds to
      # download page and update its status into database before StatusUpdater starting to update cluster
      # status based on each task's status
      wait_time = random.uniform(0, self.collector_config.period - 2)
      # store the metric_source id for one task and just return the metric_source id to
      # the output queue when the subprocesses complete the task.
      metric_source.task.metric_source_id = index
      # store a redundant attribute 'need_analyze' for task
      metric_source.task.need_analyze = metric_source.need_analyze
      logger.info("%r waiting %f seconds for %s..." ,
        metric_source.task, wait_time, metric_source.url)
      reactor.callLater(wait_time, metric_source.fetch_metrics, self.input_queue)
  
    # schedule next fetch for metrics updating
    reactor.callLater(self.collector_config.period - 2, self.schedule_next_rolling)

    # call status updater task after fetching metrics
    status_updater = StatusUpdater(self.collector_config)
    reactor.callLater(self.collector_config.period + 1,
      status_updater.produce_status_update_task, self.input_queue)

    region_operation_aggregator = RegionOperationMetricAggregator(
      self.collector_config)
    # we start to aggregate region operation metric after one period
    reactor.callLater(self.collector_config.period + 1,
      region_operation_aggregator.produce_aggregate_task, self.input_queue)

    reactor.run()

