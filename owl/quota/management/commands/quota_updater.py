import logging
import time
import utils.mail

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from os import path
from utils.quota_util import QuotaUpdater

logger = logging.getLogger('quota')

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
      make_option(
        "--period",
        default=3600, # check per hour
        help="Check period"),
  )

  def handle(self, *args, **options):
    self.args = args
    self.options = options
    self.mailer = utils.mail.Mailer(options)

    self.stdout.write("args: %r\n" % (args, ))
    self.stdout.write("options: %r\n" % options)

    quota_updater = QuotaUpdater()

    while True:
      try:
        quota_updater.update_all_cluster()
        time.sleep(int(self.options['period']))
      except Exception as e:
        # send alert email when program error
        logger.warning('Quota updater error: %r', e)
        admin_email = ''
        try:
          admin_email = settings.ADMINS[0][1]
        except:  
          pass
        self.mailer.send_email(subject = 'Quota updater error',
                               content = repr(e),
                               to_email = admin_email,
                              )
