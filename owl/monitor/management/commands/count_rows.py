# -*- coding: utf-8 -*-

import logging
import subprocess
import re
import time
import datetime

from django.conf import settings
from django.core.management.base import BaseCommand

from monitor.models import Table

logger = logging.getLogger(__name__)

# Count rows of HBase tables
class Command(BaseCommand):

  def handle(self, *args, **options):

    count_period = settings.COUNT_PERIOD
    count_start_hour = settings.COUNT_START_HOUR
    count_end_hour = settings.COUNT_END_HOUR
    logger.info("Count rows from " + str(count_start_hour) + " to " + str(count_end_hour) + " every " + str(count_period) + " days")
    
    # Wait to next day to count
    self.wait_to_next_day(count_start_hour)

    while True:
      period_start_time = datetime.datetime.now()

      # Get tables from database
      tables = Table.objects.filter(is_count_rows=True)

      # Count rows of tables one by one
      for table in tables:
        table_name = table.name
        cluster_name = table.cluster.name

        count = self.count_rows(cluster_name, table_name)

        # Insert result into database
        if count == -1:
          logger.info("Count error, will not update the database")
        else:
          table.rows = count
          table.last_update_time = datetime.datetime.now()
          table.save()
          logger.info("Save the new rows " + table.rows + " in database")

        # Continue or pause
        if datetime.datetime.now().hour >= count_end_hour:
          logger.info("Pause and wait to next day to count other tables")
          self.wait_to_next_day(count_start_hour)

      # Sleep for next period
      next_start_time = period_start_time + datetime.timedelta(days=count_period)
      sleep_time = (next_start_time - datetime.datetime.now()).total_seconds()

      logger.info("This period is finished. Sleep " + str(sleep_time) + " seconds for next period")
      time.sleep(sleep_time)

  def wait_to_next_day(self, hour):
    logger.info("Will wait to next day's " + str(hour) + " o'clock" )

    now = datetime.datetime.now()
    next_day = datetime.datetime(now.year, now.month, now.day + 1, hour, 0)
    sleep_time = (next_day - now).total_seconds()
    
    logger.info("Sleep " + str(sleep_time) + " seconds")
    time.sleep(sleep_time)
    
  def count_rows(self, cluster_name, table_name):
    logger.info("Count the rows of " + table_name + " in " + cluster_name)

    try:
      # deploy shell hbase sdtst-miliao org.apache.hadoop.hbase.coprocessor.example.CoprocessorRowcounter _acl_ --speed=3000
      deploy_command = settings.DEPLOY_COMMAND
      command_list = [deploy_command, "shell", "hbase"] + [cluster_name] + ["org.apache.hadoop.hbase.coprocessor.example.CoprocessorRowcounter"] + [table_name] + ["--speed=3000"]

      rowcounter_process = subprocess.Popen(command_list, stdout=subprocess.PIPE)
      rowcounter_result = rowcounter_process.communicate()
      rowcounter_status = rowcounter_process.wait()

      # e.g. "_acl_ 2014-4-18 3"
      pattern = table_name + " \\d+\\-\\d+\\-\\d+ (\\d+)"; 
      compiled_pattern = re.compile(pattern)
      re_result = compiled_pattern.search(rowcounter_result[0])
      return re_result.group(1)
    except:
      logger.error("Error to count rows, make sure kinit to run CoprocessorRowcounter and set DEPLOY_COMMAND")
      return -1


  
