import csv
import logging
import sys

from django.conf import settings
from django.core.management.base import BaseCommand

from machine.models import Machine


logger = logging.getLogger(__name__)

MACHINE_FIELDS = (
  'hostname', 'ip', 'idc', 'rack', 'cores', 'ram',
  'disks', 'disk_capacity', 'ssds', 'ssd_capacity'
)


class Command(BaseCommand):
  def handle(self, *args, **options):
    writer = csv.DictWriter(sys.stdout, MACHINE_FIELDS)
    writer.writeheader()
    writer.writerows(
        Machine.objects.order_by('hostname').values(*MACHINE_FIELDS))
