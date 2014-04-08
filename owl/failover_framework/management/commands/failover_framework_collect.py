# -*- coding: utf-8 -*-

import json
import logging
import os
import owl_config
import smtplib
import time
import urllib2
import utils.mail

from django.conf import settings
from django.core.management.base import BaseCommand

from failover_framework.models import Action
from failover_framework.models import Task

logger = logging.getLogger(__name__)

# Collect metrics from failover framework and insert into database periodically
class Command(BaseCommand):

  def handle(self, *args, **options):

    if len(args) >= 1:
      logger.warning("No need to give args for this script")

    mailer = utils.mail.Mailer(options)
    host = settings.FAILOVER_FRAMEWORK_HOST
    port = settings.FAILOVER_FRAMEWORK_PORT
    host_port = host + ":" + str(port)
    period = settings.FAILOVER_FRAMEWORK_PERIOD

    while True:
      start_time = time.time()
      self.collect_failover_framework_metrics(host_port, mailer)
      sleep_time = period - (time.time() - start_time)
      if sleep_time >= 0:
        logger.info("Sleep " + str(sleep_time) + " seconds for next time to collect metrics")
        time.sleep(sleep_time)
      else:
        logger.warning("Period is too short to collect metrics")

  def collect_failover_framework_metrics(self, host_port, mailer):
    try:
      # download json       
      metricsString = urllib2.urlopen("http://" + host_port + "/jmx")
      metrics = json.load(metricsString)

      # process json
      actions_info = []
      for metric in metrics["beans"]:
        # Task Metrics
        if "ActionsInfo" in metric: # the Task metric
          task_start_timestamp = metric["StartTimestamp"]
          task_start_time = metric["StartTime"]
          task_action_number = metric["ActionNumber"]
          actions_info = metric["ActionsInfo"]
          # Status Metrics
        elif "ClusterHealthy" in metric:
          task_cluster_healthy = True if metric["ClusterHealthy"] else False # int to boolean
          task_data_consistent = True if metric["DataConsistent"] else Fasle
          task_success = task_cluster_healthy and task_data_consistent

      # insert into database
      task = Task(start_timestamp=task_start_timestamp, start_time=task_start_time, action_number=task_action_number, cluster_healthy=task_cluster_healthy, data_consistent=task_data_consistent, success=task_success)
      task.save()
      logger.info("Insert Task into database which start at " + task_start_time)

      for action_info in actions_info:
        action = Action(task_id=task_start_timestamp, start_time=task_start_time, name=action_info["name"], success=action_info["success"], consume_time=action_info["consumeTime"])
        action.save()
        logger.info("Insert Action into database which is " + action.name)

      # send email
      if task_success == False:
        email_to = owl_config.FAILOVER_TO_EMAIL
        content = "Cluster healthy is " + str(task_cluster_healthy) + " and data consistent is " + str(task_data_consistent) + ".\nGo to owl for more details."
        logger.warning("Failover test fails, send email to " + email_to)
        mailer.send_email(content, "Failover Test Fails", to_mail)

    except:
      logger.warning("Can't get metrics from " + host_port + ", maybe failover framework is not running")
