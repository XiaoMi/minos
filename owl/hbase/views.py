# -*- coding: utf-8 -*-
from models import Longhaul
from monitor.views import respond
from monitor.dbutil import get_counters_by_group

def index(request):
  # show all cluster
  longhauls = Longhaul.objects.all()
  params = {
    'longhauls': longhauls,
  }
  return respond(request, 'hbase/index.html', params)

#url: /longhaul/$id/
def show_longhaul(request, id):
  longhaul = Longhaul.objects.get(id=id)
  group = longhaul.getCounterGroup()
  counters = get_counters_by_group(group)
  endpoint = 'unknown'
  counter_names = []
  for counter in counters:
    endpoint = counter.host
    counter_names.append(group + '-' + counter.name)
  
  params = {
    'endpoint': endpoint,
    'counter_names': counter_names,
    'longhaul': longhaul,
  }
  return respond(request, 'hbase/longhaul.html', params)
