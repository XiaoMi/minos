import ConfigParser
import argparse
import datetime
import json
import logging
import os
import smtplib
import sys
import time
import utils.mail

import deploy_utils

from optparse import make_option
from os import path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from monitor.models import Status, Service, Cluster, Job, Task

BOOL_METRIC_MAP = {
    "tag.IsOutOfSync": "true",
    "tag.HAState": "active",
}

STATUS_FILE_PATH = 'cluster.status'
# alert when cluster is not OK for ERROR_TIMES_FOR_ALERT
ERROR_TIMES_FOR_ALERT = 3

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
        self.clusters[cluster_name] = deploy_utils.get_service_config(args)
      self.metric_url = config.get(name, "metric_url")

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

class StatusChecker:
  """Check status of all active clusters and jobs, which are inferred from
  tasks' status."""

  def __init__(self, collector_config, last_status, options, mailer):
    self.collector_config = collector_config
    self.last_status = last_status
    self.options = options
    self.alert_msg = ''
    self.mailer = mailer

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

  def check_hdfs_cluster_status(self, cluster):
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

  def check_hbase_cluster_status(self, cluster):
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

  def check_yarn_cluster_status(self, cluster):
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

  def check_impala_cluster_status(self, cluster):
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

  def check_cluster_status(self, cluster):
    cluster.jobs = {}
    cluster.last_status = Status.OK
    cluster.last_message = ""

    for job in cluster.job_set.all():
      job.running_tasks = {}
      job.tasks = {}
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
        "hdfs": self.check_hdfs_cluster_status,
        "hbase": self.check_hbase_cluster_status,
        "yarn": self.check_yarn_cluster_status,
        "impala": self.check_impala_cluster_status,
    }
    service_handler[cluster.service.name](cluster)
    self.handle_status_result(cluster)

  def handle_status_result(self, cluster):
    # last_status store cluster_name->(status, status_times)
    (cluster_status, status_times) = self.last_status.setdefault(str(cluster), (Status.OK, 0))
    need_send_alert = False

    if cluster.last_status != cluster_status:
      self.last_status[str(cluster)] = (cluster.last_status, 1)
      if cluster.last_status == Status.OK and status_times >= ERROR_TIMES_FOR_ALERT:
        # send alert when cluster changed to from PROBLEM(alerted) to OK
        need_send_alert = True
    else:
      self.last_status[str(cluster)] = (cluster.last_status, status_times+1)
      # send alert when cluster in PROBLEM stutus reached ERROR_TIMES_FOR_ALERT times
      if cluster.last_status != Status.OK and status_times + 1 == ERROR_TIMES_FOR_ALERT:
        need_send_alert = True

    if need_send_alert:
      self.alert_msg += '[%s]Cluster[%s]\n' \
          % ('OK' if cluster.last_status == Status.OK else 'PROBLEM',
             cluster)
      for job in cluster.jobs.itervalues():
        if job.last_status != Status.OK:
          self.alert_msg += 'Job[%s] not healthy: %s\n' % (job.name, job.last_message)
      self.alert_msg += '******\n'


  def check_status(self):
    self.alert_msg = ''
    logger.info("checking clusters status")

    self.start_time = time.time()
    for cluster in Cluster.objects.filter(active=True).all():
      self.check_cluster_status(cluster)
    logger.info("spent %f seconds for updating clusters status",
        time.time() - self.start_time)
    if self.alert_msg:
      logger.warn('alert msg: %r' % self.alert_msg)
      self.mailer.send_email(subject = 'OWL cluster alert',
                             content = self.alert_msg,
                             to_email = self.options['to_email'])
    json.dump(self.last_status, open(STATUS_FILE_PATH, 'w'))

class Command(BaseCommand):
  args = ''
  help = "Run the background collector to fetch metrics from /jmx on each server."

  option_list = BaseCommand.option_list + (
      make_option(
        "--to_email",
        help="Email address to"),
      make_option(
        "--period",
        default=60,
        help="Check period"),
  )

  def handle(self, *args, **options):
    self.args = args
    self.options = options
    self.mailer = utils.mail.Mailer(options)

    self.stdout.write("args: %r\n" % (args, ))
    self.stdout.write("options: %r\n" % options)

    self.collector_config = CollectorConfig(self.args, self.options)

    self.last_status = {}
    try:
      self.last_status = json.load(open(STATUS_FILE_PATH, 'r'))
    except Exception as e:
      logger.warning('Failed to load status file: %r', e)

    status_checker = StatusChecker(self.collector_config,
                                   self.last_status,
                                   self.options,
                                   self.mailer)

    while True:
      try:
        status_checker.check_status()
      except Exception as e:
        logger.warning('OWL cluster checker error: %r', e)
        # send alert email when program got error
        admin_email = ''
        try:
          admin_email = settings.ADMINS[0][1]
        except:
          pass
        self.mailer.send_email(subject = 'OWL cluster check error',
                               content = repr(e),
                               to_email = admin_email,
                              )
      time.sleep(int(self.options['period']))
