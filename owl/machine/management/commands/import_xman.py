import csv
import logging
import json
import sys
import urllib2

from django.conf import settings
from django.core.management.base import BaseCommand


from machine.models import Machine


logger = logging.getLogger(__name__)

XMAN_URL = "http://10.180.2.243/api/hostinfo.php?sql=hostname+=+'%s'"
IDC_ABBR = {
  'shangdi': 'sd',
  'lugu': 'lg',
  'lugu6': 'lg',
  'haihang': 'hh',
  'wucaicheng': 'dp',
}


class Command(BaseCommand):
  def handle(self, *args, **options):
    changes = []
    for machine in Machine.objects.order_by('hostname'):
      hostname = machine.hostname
      url = XMAN_URL % hostname
      data = json.load(urllib2.urlopen(url))
      xman = {}
      if data and type(data) is dict:
        k, v = data.popitem()
        if v and type(v) is dict:
          try:
            xman = {
              'ip': v['ipaddr'],
              'idc': IDC_ABBR[v['site'].lower()],
              'rack': v['location'].lower(),
            }
          except Exception as e:
            print 'Error on host: %s' % hostname
            raise
      if not xman:
        # the machine doesn't exist in xman, delete it later.
        changes.append((machine, xman, ))
      else:
        # check if any field changed.
        # can't use iteritems as the dict might change.
        for k, v in xman.items():
          if getattr(machine, k) == v:
            del xman[k]
        if xman:
          # some fields changed.
          changes.append((machine, xman, ))

    if not changes:
      print 'Nothing updated from xman, exiting.'
    else:
      print 'All changes from xman:'
      for machine, xman in changes:
        self.print_change(machine, xman)

      print
      print 'Confirm following changes...'
      answer = None
      for machine, xman in changes:
        self.print_change(machine, xman)
        while answer != 'a':
          answer = raw_input('Apply this or all following change[s]? '
              '<y[es]/n[o]/a[ll]>: ')
          if answer in ['y', 'n', 'a']: break
        if answer == 'n': continue
        # apply change
        self.apply_change(machine, xman)

  def print_change(self, machine, xman):
    if not xman:
      action = 'host deleted'
    else:
      action = ', '.join(['%s: %s ==> %s' % (k, getattr(machine, k), v)
        for k, v in xman.iteritems()])
    print '%s: %s' % (machine.hostname, action)

  def apply_change(self, machine, xman):
    if not xman:
      machine.delete()
    else:
      for k, v in xman.iteritems():
        setattr(machine, k, v)
      machine.save()
