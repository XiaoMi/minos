import collections
import datetime
import logging
import smtplib
import sys
import time

from optparse import make_option
from os import path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

import owl_config
import monitor.dbutil
import utils.mail
import utils.quota_util
import deploy_utils
from monitor.models import Cluster, Quota, Service

logger = logging.getLogger('quota')

# cluster to generate report
QUOTA_REPORT_CLUSTER = owl_config.QUOTA_REPORT_CLUSTER
# user that receive cluster report
QUOTA_REPORT_ADMINS = owl_config.QUOTA_REPORT_ADMINS
# user that receive cluster quota alert
QUOTA_ALERT_ADMINS = owl_config.QUOTA_ALERT_ADMINS

KERBEROS_IDS_PATH = owl_config.KERBEROS_IDS_PATH

admin_email = ''
try:
  admin_email = settings.ADMINS[0][1]
except:  
  pass

class QuotaReportor:
  """Update path quota in hdfs"""
  def __init__(self, options):
    self.options = options
    self.mailer = utils.mail.Mailer(options)
    self.user_report = {} # report group by user
    self.cluster_report = {} # report group by cluster
    self.today = datetime.date.today()
    self.kerb_user_map = self.init_kerb_user_map()

  def report(self):
    logger.info('start make quota report')
    self.start_time = time.time()
    try:
      for cluster_name in QUOTA_REPORT_CLUSTER:
        self.update_cluster(cluster_name)
    except Exception as e:
      logger.info('gather quota info failed: %r', e)
      self.mailer.send_email(subject = 'Make quota report failed',
                             content = repr(e),
                             to_email = admin_email, 
                            )
    else:
      self.send_report_mail()

    logger.info('spent %f seconds for make quota report',
        time.time() - self.start_time)

  def update_cluster(self, cluster_name):
    hdfs_service = Service.objects.get(name='hdfs')
    cluster = Cluster.objects.get(service=hdfs_service, name = cluster_name)
    quota_list = monitor.dbutil.get_quota_summary(cluster)
    for quota_record in quota_list:
      user_report = self.user_report.setdefault(quota_record.name, {})
      user_report[cluster_name] = quota_record
      cluster_report = self.cluster_report.setdefault(cluster_name, {})
      cluster_report[quota_record.name] = quota_record

  def send_report_mail(self):
    self.send_user_report_mail()
    self.send_cluster_report_mail()
    self.alert_to_not_healthy_users()

  def send_user_report_mail(self):
    for user, cluster_quota in self.user_report.iteritems():
      subject = 'Hadoop hdfs quota report for user %s' % user
      content = 'Report date: %s<br>' % self.today
      content += self.format_quota_report_content('cluster', cluster_quota)
      email_user = self.map_kerb_user_to_email_user(user)
      if email_user:
        email_addr = ','.join([addr for addr in email_user.split()])

        self.mailer.send_email(to_email = email_addr,
                               subject = subject,
                               content = content,
                               type = 'html')
      else:
        logger.error('User %s has no email user' % user)

  def send_cluster_report_mail(self):
    subject = 'Hadoop hdfs quota report for admin'
    content = 'Report date: %s<br>' % self.today
    for cluster, user_quota in self.cluster_report.iteritems():
      content += 'Quota summary on cluster[%s]<br>' % cluster
      content += self.format_quota_report_content('user', user_quota)
      content += '********<br>'
    self.mailer.send_email(to_email = QUOTA_REPORT_ADMINS,
                           subject = subject,
                           content = content,
                           type = 'html')

  def alert_to_not_healthy_users(self):
    subject = 'Hadoop hdfs quota alert'
    for user, cluster_quota in self.user_report.iteritems():
      for cluster, quota in cluster_quota.iteritems():
        need_alert = False
        content = 'Cluster: %s\n' % cluster
        content += 'User: %s\n' % user

        if not utils.quota_util.is_space_quota_healthy(
          quota.space_quota, quota.used_space_quota):
          content += 'Alert: space quota exceeded the threshold. \
              Please cleanup trash or apply for more space quota.\n'
          need_alert = True

        if not utils.quota_util.is_name_quota_healthy(
          quota.quota, quota.used_quota):
          content += 'Alert: name quota exceeded the threshold. \
              Please cleanup trash or apply for more name quota.\n'
          need_alert = True

        if need_alert:
          email_addrs = QUOTA_ALERT_ADMINS
          email_user = self.map_kerb_user_to_email_user(user)
          if email_user:
            email_addrs += ','.join([addr for addr in email_user.split()])
          self.mailer.send_email(to_email = email_addrs,
                                 subject = subject,
                                 content = content)

  @staticmethod
  def format_quota_report_content(key_name, quota_map):
    content = '<table>'
    HEADER_FORMAT_STR = '<tr><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr>'
    content += HEADER_FORMAT_STR.format(key_name, 'SpaceQuota', 'UsedSpace', 'RemainingSpace', 'NameQuota', 'UsedName', 'RemainingName')

    ROW_FORMAT_STR = '<tr><td>{}</td><td>{}</td><td>{}</td><td %s>{}</td><td>{}</td><td>{}</td><td %s>{}</td></tr>'

    ordered_dict = collections.OrderedDict(sorted(quota_map.items()))
    for key, quota in ordered_dict.iteritems():
      space_quota_color = '' if utils.quota_util.is_space_quota_healthy(
        quota.space_quota, quota.used_space_quota) \
        else 'style="color:rgb(255,0,0)"'
      name_quota_color = '' if utils.quota_util.is_name_quota_healthy(
        quota.quota, quota.used_quota) \
        else 'style="color:rgb(255,0,0)"'
      format_str = ROW_FORMAT_STR % (space_quota_color, name_quota_color)
      content += format_str.format(key,
                                   format_bigint(quota.space_quota),
                                   format_bigint(quota.used_space_quota),
                                   format_bigint(quota.remaining_space_quota),
                                   quota.quota, quota.used_quota, quota.remaining_quota)
    content += '</table>'
    return content



  def init_kerb_user_map(self):
    res = {}
    config_path = deploy_utils.get_config_dir()
    with open(path.join(config_path, KERBEROS_IDS_PATH)) as f:
      for line in f:
        if line.startswith('#'):
          continue
        try:
          # file format: kerb_user user1[ user2 user3]
          kerb_user, email_users = line.strip().split(' ', 1)
          if kerb_user in res:
            logger.warn('Duplicated kerb user config for user: %s' % kerb_user)
          res[kerb_user] = email_users
        except Exception as e:
          logger.warn('Failed to parse user config [%r]: %s' % (e, line))
    return res


  def map_kerb_user_to_email_user(self, kerb_user):
    if kerb_user in self.kerb_user_map:
      return self.kerb_user_map[kerb_user]
    else:
      return None

class Command(BaseCommand):
  default_root = path.abspath(
      path.dirname(path.realpath(__file__)) + "/../../../..")

  args = ''
  help = "Run the background updater to collector quota on hdfs clusters."

  option_list = BaseCommand.option_list + (
      make_option(
        "--root",
        default=default_root,
        help="Root path of minos, used to look for deployment package/config."),
  )

  def handle(self, *args, **options):
    self.args = args
    self.options = options
    self.mailer = utils.mail.Mailer(options)

    self.stdout.write("args: %r\n" % (args, ))
    self.stdout.write("options: %r\n" % options)

    quota_reportor = QuotaReportor(options)

    try:
      quota_reportor.report()
    except Exception as e:
      logger.warning('Quota repotor aborted: %r', e)
      self.mailer.send_email(subject = 'Make quota report failed',
                             content = repr(e),
                             to_email = admin_email, 
                            )

def format_bigint(value):
  try:
    value = int(value)
  except (TypeError, ValueError):
    return value

  if value < 1024*1024:
    return value

  K = 1024
  formaters = (
    (2, '%.2fM'),
    (3, '%.2fG'),
    (4, '%.2fT'),
    (5, '%.2fP'),
  )

  for exponent, formater in formaters:
    larger_num = K ** exponent
    if value < larger_num * K:
      return formater % (value/float(larger_num))
