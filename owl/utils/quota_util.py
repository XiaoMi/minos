import datetime
import logging
import quota_injector
import time
import utils.hadoop_util

from django.db import transaction
from django.utils import timezone
from monitor.models import Cluster, Quota, Service

logger = logging.getLogger('quota')
quota_injector = quota_injector.QuotaInjector()

class QuotaUpdater:
  """Update path quota in hdfs"""

  def update_all_cluster(self):
    logger.info("start updating clusters quota")
    self.start_time = time.time()
    hdfs_service = Service.objects.get(name='hdfs')
    for cluster in Cluster.objects.filter(active=True, service=hdfs_service).all():
      self.update_cluster(cluster)
    logger.info("spent %f seconds for updating clusters quota",
        time.time() - self.start_time)

  @transaction.commit_on_success
  def update_cluster(self, cluster):
    logger.info("start update cluster %s" % cluster.name),
    cluster_name = cluster.name
    now = time.time()
    quota_list = utils.hadoop_util.get_quota_summary(cluster_name)
    quota_injector.push_quota_to_tsdb(quota_list, cluster_name)
    for quota in quota_list:
      quota_record, ok = Quota.objects.get_or_create(cluster=cluster, name=quota['name'])
      quota_record.quota = quota['quota']
      quota_record.used_quota = quota['used_quota']
      quota_record.remaining_quota = quota['remaining_quota']
      quota_record.space_quota = quota['space_quota']
      quota_record.used_space_quota = quota['used_space_quota']
      quota_record.remaining_space_quota = quota['remaining_space_quota']
      quota_record.last_update_time = datetime.datetime.utcfromtimestamp(
          now).replace(tzinfo=timezone.utc)
      quota_record.save()
    logger.info("end update cluster %s" % cluster.name),

def is_space_quota_healthy(total, used):
  try:
    # remaining < 1G or used ratio > 80% means not healthy
    if (int(total) - int(used)) < 1024*1024*1024 \
      or float(used) / float(total) > 0.8:
      return False
  except Exception, e:
    pass
  return True

def is_name_quota_healthy(total, used):
  try:
    # remaining < 500 or used ratio > 80% means not healthy
    if (int(total) - int(used)) < 500\
      or float(used) / float(total) > 0.8:
      return False
  except Exception, e:
    pass
  return True
