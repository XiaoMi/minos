import logging
import time
import utils.mail

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from os import path
from utils.quota_util import QuotaUpdater

from quota_reportor import QUOTA_REPORT_ADMINS

logger = logging.getLogger('quota')

class Command(BaseCommand):
  args = ''
  help = "Run the background updater to collector quota on hdfs clusters."

  option_list = BaseCommand.option_list + (
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
      except Exception as e:
        # send alert email when program error
        logger.warning('Quota updater error: %r', e)
        admin_email = ''
        try:
          admin_email = QUOTA_REPORT_ADMINS
        except:
          pass
        self.mailer.send_email(subject = 'Quota updater error',
                               content = repr(e),
                               to_email = admin_email,
                              )
      time.sleep(int(self.options['period']))
