import csv
import logging
import sys

from django.conf import settings
from django.core.management.base import BaseCommand

from machine.models import Machine


logger = logging.getLogger(__name__)


class Command(BaseCommand):
  def handle(self, *args, **options):
    rows = [row for row in csv.DictReader(sys.stdin)]
    Machine.objects.all().delete()
    for row in rows:
      Machine.objects.create(**row)
