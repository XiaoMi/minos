import datetime
import json
import logging
import os
import time

from django.utils import timezone
from monitor.models import Cluster
from monitor.models import Status

import gc
import resource

logger = logging.getLogger(__name__)

def get_latest_metric(task, group_name, metric_name):
  try:
    metric = json.loads(task.last_metrics)
    return metric[group_name][metric_name]
  except Exception as e:
    logger.warning("%r failed to get metric: %r", task, e)
    return 0

def is_namenode_active(task):
  try:
    metric = get_latest_metric(task,
      "Hadoop:service=NameNode,name=FSNamesystem", "tag.HAState")
    return bool(metric)
  except Exception as e:
    logger.warning("%r failed to get metric: %r", task, e)
    return False

def is_master_active(task):
  try:
    metric = get_latest_metric(task,
      "hadoop:service=Master,name=Master", "IsActiveMaster")
    return bool(metric)
  except Exception as e:
    logger.warning("%r failed to get metric: %r", task, e)
    return False

def update_hdfs_cluster_status(cluster):
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
      if is_namenode_active(task):
        # update cluster entry
        cluster.entry = '%s:%d' % (task.host, task.port)
        cluster.version = get_latest_metric(task,
          'Hadoop:service=NameNode,name=NameNodeInfo', 'Version')
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

def update_hbase_cluster_status(cluster):
  job = cluster.jobs["master"]
  if job.running_tasks_count < 1:
    job.last_status = Status.ERROR
    job.last_message = "No running masters!"
  else:
    active = 0
    for task in job.running_tasks.itervalues():
      if is_master_active(task):
        # update cluster entry
        cluster.entry = '%s:%d' % (task.host, task.port)
        version = get_latest_metric(task,
          'hadoop:service=HBase,name=Info', 'version')
        revision = get_latest_metric(task,
          'hadoop:service=HBase,name=Info', 'revision')
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

def update_yarn_cluster_status(cluster):
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

def update_impala_cluster_status(cluster):
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

def update_cluster_status(cluster, start_time):
  cluster.jobs = {}
  cluster.last_attempt_time = datetime.datetime.utcfromtimestamp(
    start_time).replace(tzinfo=timezone.utc)
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
      "hdfs": update_hdfs_cluster_status,
      "hbase": update_hbase_cluster_status,
      "yarn": update_yarn_cluster_status,
      "impala": update_impala_cluster_status,
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

def update_status_in_process(output_queue, task_data):
  logger.info("Updating clusters status in process %d" % os.getpid())
  try:
    start_time = time.time()
    for cluster in Cluster.objects.filter(active=True).all():
      update_cluster_status(cluster, start_time)
    logger.info("spent %f seconds for updating clusters status",
        time.time() - start_time)
    logger.info("gc: %r", gc.get_count())
    logger.info("usage: %r", resource.getrusage(resource.RUSAGE_SELF))
  except Exception as e:
    logger.warning("Failed to update status: %r", e)

