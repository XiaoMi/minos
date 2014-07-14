# -*- coding: utf-8 -*-
from django.db import models
from django.utils import timezone

import datetime
import json


DEFAULT_DATETIME = datetime.datetime(1970, 1, 1, tzinfo=timezone.utc)
# If a cluster/job/task's last success time has passed this many seconds, it's
# considered as failed.
FAIL_TIME = 30

# The item could be cluster, job, or task.
def is_healthy(item, fail_time=FAIL_TIME):
  delta = datetime.timedelta(seconds=fail_time)
  if item.last_success_time + delta < datetime.datetime.now(tz=timezone.utc):
    return False
  return True


class Status:
  OK = 0
  WARN = 1
  ERROR = 2


class Service(models.Model):
  # Name of the service, like "hdfs", "hbase", etc.
  name = models.CharField(max_length=128)
  # Url to get metrics which is formatted in json.
  metric_url = models.CharField(max_length=128)
  # If the service is being actively monitored. We don't want to delete metrics
  # data once a service/cluster/job is deactive, so just use a boolean field to
  # indicate it.
  active = models.BooleanField(default=True)
  # A text description.
  description = models.CharField(max_length=1024)

  def __unicode__(self):
    return self.name


class Cluster(models.Model):
  # Each cluster must belong to one service.
  service = models.ForeignKey(Service, db_index=True)
  # The cluster name like "ggsrv-miliao", "sdtst-test", etc.
  name = models.CharField(max_length=128)
  # The same as service.
  active = models.BooleanField(default=True)
  # A text description.
  description = models.CharField(max_length=1024)
  # The last attempt time to fetch metrics, whether successful or failed.
  # It's the time of client initiating the request, not the time of client
  # receiving the response.
  last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  # If the last attempt is successful.
  last_status = models.IntegerField(default=Status.ERROR)
  # The status message of last attempt.
  last_message = models.CharField(max_length=128)
  # The last update time of this task's metrics, must be successful.
  # The definition is the same as last_attempt.
  last_success_time = models.DateTimeField(default=DEFAULT_DATETIME)
  # cluster version in format: "version, revision"
  version = models.CharField(max_length=128)
  # Entry for service's native main page
  entry = models.CharField(max_length=128)

  @property
  def health(self):
    return is_healthy(self)

  def __unicode__(self):
    return u"%s/%s" % (unicode(self.service), self.name)


class Job(models.Model):
  # Each job must belong to one cluster.
  cluster = models.ForeignKey(Cluster, db_index=True)
  # The job name like "namenode", "regionserver", etc.
  name = models.CharField(max_length=128)
  # The same as service.
  active = models.BooleanField(default=True)
  # A text description.
  description = models.CharField(max_length=1024)
  # How many tasks are in running and healthy.
  running_tasks_count = models.IntegerField(default=0)
  # How many tasks in total.
  total_tasks_count = models.IntegerField(default=0)
  # The last attempt time to fetch metrics, whether successful or failed.
  # It's the time of client initiating the request, not the time of client
  # receiving the response.
  last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  # If the last attempt is successful.
  last_status = models.IntegerField(default=Status.ERROR)
  # The status message of last attempt.
  last_message = models.CharField(max_length=128)
  # The last update time of this task's metrics, must be successful.
  # The definition is the same as last_attempt.
  last_success_time = models.DateTimeField(default=DEFAULT_DATETIME)

  @property
  def health(self):
    return is_healthy(self)

  def __unicode__(self):
    return u"%s/%s" % (unicode(self.cluster), self.name)


class Task(models.Model):
  job = models.ForeignKey(Job, db_index=True)
  # The task id.
  task_id = models.IntegerField()
  # The ip or hostname that the task is running on.
  host = models.CharField(max_length=128)
  # The port number where we could get metrics data from.
  port = models.IntegerField()
  # The same as service.
  active = models.BooleanField(default=True)
  # The last attempt time to fetch metrics, whether successful or failed.
  # It's the time of client initiating the request, not the time of client
  # receiving the response.
  last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  # If the last attempt is successful.
  last_status = models.IntegerField(default=Status.ERROR)
  # The status message of last attempt.
  last_message = models.CharField(max_length=128)
  # The last update time of this task's metrics, must be successful.
  # The definition is the same as last_attempt.
  last_success_time = models.DateTimeField(default=DEFAULT_DATETIME)
  # The last metric values, encoded in json.
  last_metrics = models.TextField()
  # The last raw metric values fetched from http server, for debug purpose
  last_metrics_raw = models.TextField()

  class Meta:
    index_together = [["host", "port"],]

  @property
  def health(self):
    return is_healthy(self)

  def __unicode__(self):
    return u"%s/%d" % (unicode(self.job), self.task_id)

class HBaseCluster(models.Model):
  cluster = models.OneToOneField(Cluster, db_index=True)

  memStoreSizeMB = models.IntegerField(default = 0)
  storefileSizeMB = models.IntegerField(default = 0)
  # readRequestsCount and writeRequestsCount may exceed max integer
  readRequestsCount = models.FloatField(default = 0, max_length = 20)
  writeRequestsCount = models.FloatField(default = 0, max_length = 20)
  readRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)
  writeRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)
  operationMetrics = models.TextField() # save operation metrics as json format

class RegionServer(models.Model):
  cluster = models.ForeignKey(Cluster, db_index=True)
  task = models.OneToOneField(Task, db_index=True)
  name = models.CharField(max_length=128)
  last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  load = models.IntegerField(default = 0)
  numberOfRegions = models.IntegerField(default = 0)
  numberOfRequests = models.IntegerField(default = 0)

  memStoreSizeMB = models.IntegerField(default = 0)
  storefileSizeMB = models.IntegerField(default = 0)
  readRequestsCount = models.IntegerField(default = 0)
  writeRequestsCount = models.IntegerField(default = 0)
  readRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)
  writeRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)
  replication_last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  replicationMetrics = models.TextField() # save replication metrics as json format

  def __unicode__(self):
    return unicode(self.name.split(',')[0])

class Table(models.Model):
  cluster = models.ForeignKey(Cluster, db_index=True)
  name = models.CharField(max_length=128)
  last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)

  memStoreSizeMB = models.IntegerField(default = 0)
  storefileSizeMB = models.IntegerField(default = 0)
  readRequestsCount = models.IntegerField(default = 0)
  writeRequestsCount = models.IntegerField(default = 0)
  readRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)
  writeRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)

  availability = models.FloatField(default=-1.0)
  operationMetrics = models.TextField() # save operation metrics as json format

  rows = models.IntegerField(default = -1)
  is_count_rows = models.BooleanField(default=False)
  last_update_time = models.DateTimeField(default=DEFAULT_DATETIME)
  
  def __unicode__(self):
    return unicode(self.name)

ROOT_TABLE_NAME = '-ROOT-'
META_TABLE_NAME = '.META.'
ROOT_REGION_ENCODING_NAME = '70236052'
META_REGION_ENCODING_NAME = '1028785192'

class Region(models.Model):
  table = models.ForeignKey(Table, db_index=True)
  region_server = models.ForeignKey(RegionServer, db_index=True)
  last_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  name = models.CharField(max_length=256)
  encodeName = models.CharField(max_length = 128, db_index=True)

  memStoreSizeMB = models.IntegerField(default = 0)
  storefileSizeMB = models.IntegerField(default = 0)
  readRequestsCount = models.IntegerField(default = 0)
  writeRequestsCount = models.IntegerField(default = 0)
  readRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)
  writeRequestsCountPerSec = models.FloatField(default = 0, max_length = 20)

  currentCompactedKVs = models.IntegerField(default = 0)
  requestsCount = models.IntegerField(default = 0)
  rootIndexSizeKB = models.IntegerField(default = 0)
  storefileIndexSizeMB = models.IntegerField(default = 0)
  storefiles = models.IntegerField(default = 0)
  stores = models.IntegerField(default = 0)
  totalCompactingKVs = models.IntegerField(default = 0)
  totalStaticBloomSizeKB = models.IntegerField(default = 0)
  totalStaticIndexSizeKB = models.IntegerField(default = 0)
  version = models.IntegerField(default = 0)
  last_operation_attempt_time = models.DateTimeField(default=DEFAULT_DATETIME)
  operationMetrics = models.TextField() # save operation metrics as json format

  # root and meta region use old region format, where root regon name is: '-ROOT-,,0.70236052'
  # and first meta region name is: .META.,,1.1028785192. 70236052 and 1028785192 will serve as
  # encode name for root region and meta region respectively. Other data region use new format,
  # such as hbase_client_test_table,01,1369368306964.7be6b8bda3e59d5e6d4556482fc84601. in which
  # 7be6b8bda3e59d5e6d4556482fc84601 will serve as encode name
  @staticmethod
  def get_encode_name(name):
    if name[0:6] == ROOT_TABLE_NAME:
      return ROOT_REGION_ENCODING_NAME
    if name[0:6] == META_TABLE_NAME:
      return META_REGION_ENCODING_NAME
    return name.split('.')[1]

  # the region operation metric name(AvgTime) for 'multiput' seems like:
  # tbl.hbase_client_test_table.region.9fdec6d4dbb175e2b098e16fc5987dcb.multiput_AvgTime where
  # 9fdec6d4dbb175e2b098e16fc5987dcb is the encode name
  @staticmethod
  def is_region_operation_metric_name(name):
    if name.find('tbl') >= 0 and name.find('region') >= 0:
      return True
    return False

  def get_region_id(self):
    region_id = ""
    try:
      element_list = self.name.split(',')
      region_id = element_list[-1].split('.')[1]
    except Exception as e:
      print "%s failed to get region id." % (self.name)
    return region_id

  @staticmethod
  def get_encode_name_from_region_operation_metric_name(name):
    tokens = name.split('.')
    return tokens[len(tokens) - 2]

  def analyze_region_record(self, region_value, update_time):
    time_interval = (update_time - self.last_attempt_time).seconds
    self.readRequestsCountPerSec = \
        (float)(region_value['readRequestsCount'] - self.readRequestsCount)\
        / time_interval
    if self.readRequestsCountPerSec < 0:
      self.readRequestsCountPerSec = 0
    self.writeRequestsCountPerSec = \
        (float)(region_value['writeRequestsCount'] - self.writeRequestsCount)\
        / time_interval
    if self.writeRequestsCountPerSec < 0:
      self.writeRequestsCountPerSec = 0
    self.last_attempt_time = update_time
    self.memStoreSizeMB = region_value['memStoreSizeMB']
    self.storefileSizeMB = region_value['storefileSizeMB']
    self.readRequestsCount = region_value['readRequestsCount']
    self.writeRequestsCount = region_value['writeRequestsCount']
    self.requestsCount = region_value['requestsCount']

  # operation metric from jmx is formatted as: 'tbl.tableName.region.encodeName.operationName_Suffix : value'
  # where Suffix could be OpsNum, AvgTime, MaxTime, MinTime, histogram_75percentile, histogram_95percentile etc.
  # We save all operation metrics as the a map: {operationName : {{OpsNum : value}, {AvgTime, value}, ...}}.
  # Then, the map will be converted to a json format and into self.operationMetrics
  def analyze_from_region_server_operation_metrics(self, region_operation_metrics, update_time):
    self.last_operation_attempt_time = update_time
    metric_saved = {}
    for region_operation in region_operation_metrics.keys():
      tokens = region_operation.split('.')
      tokens = tokens[len(tokens) - 1].split('_')
      tokens_len = len(tokens)

      index = 0
      while index < tokens_len:
        if tokens[index] == 'histogram':
          break;
        index = index + 1

      operationName = ''
      suffix = ''
      if index < tokens_len:
        # for histogram metics
        operationName = '_'.join(tokens[0 : index])
        suffix = '_'.join(tokens[index : tokens_len])
      else:
        operationName = '_'.join(tokens[0 : tokens_len - 1])
        suffix = tokens[tokens_len - 1]

      operationMetric = metric_saved.setdefault(operationName, {})
      operationMetric[suffix] = region_operation_metrics[region_operation]
    self.operationMetrics = json.dumps(metric_saved)

  def __unicode__(self):
    return unicode(self.name)

  def __str__(self):
    return repr(','.join((self.name.split(',')[:2]))).replace("'", '')

class Counter(models.Model):
  # The from ip of the counter
  host = models.CharField(max_length=16)
  # The group name of the counter
  group = models.CharField(max_length=64)
  name = models.CharField(max_length=128)

  # The last update time of the counter
  last_update_time = models.DateTimeField(default=DEFAULT_DATETIME)

  value = models.FloatField(default=0)
  # The unit of the value, reqs/s, ms, ...
  unit = models.CharField(max_length=16)
  # The label of the counter, used to display in corresponding section of html page
  label = models.CharField(max_length=64)

  def identity(self):
    return u"%s-%s" % (self.group, self.name)

  class Meta:
    unique_together = ("group", "name")

  def __unicode__(self):
    return u"%s/%s/%s/%s" % (self.host, self.group, self.name, self.last_update_time)

class Quota(models.Model):
  cluster = models.ForeignKey(Cluster, db_index=True)
  name = models.CharField(max_length=256)
  quota = models.CharField(max_length=16)
  used_quota = models.CharField(max_length=16)
  remaining_quota = models.CharField(max_length=16)
  space_quota = models.CharField(max_length=16)
  used_space_quota = models.CharField(max_length=16)
  remaining_space_quota = models.CharField(max_length=16)

  last_update_time = models.DateTimeField(default=DEFAULT_DATETIME)

  class Meta:
    unique_together = ("cluster", "name")

  def __unicode__(self):
    return u"%s/%s:%s" % (unicode(self.cluster), self.name,self.last_update_time)
