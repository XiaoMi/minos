# -*- coding: utf-8 -*-

from models import Business
from monitor.views import respond
from monitor.dbutil import get_counters_by_group_and_label
from business_view_config import BUSINESS_METRICS_VIEW_CONFIG
from business_view_config import ONLINE_METRICS_MENU_CONFIG
from business_view_config import ONLINE_METRICS_COUNTER_CONFIG
from business_view_config import ONLINE_METRICS_TITLE_CONFIG
from business_view_config import ONLINE_METRICS_ENDPOINT_CONFIG

#define access type
ACCESS_TYPE_WRITE = 'Write'
ACCESS_TYPE_READ = 'Read'
#define hbase operation : operation/qps/success_rate
HBASE_OPERATION_LABEL = 'HBase'
QPS_LABEL = 'Qps'
SUCCESS_RATE_LABEL = 'SuccessRate'

def index(request):
  # show all business
  businesses = Business.objects.all()
  params = {
    'businesses': businesses,
  }
  return respond(request, 'business/index.html', params)

def get_latency_counter_name(success_rate_counter_name):
  return success_rate_counter_name.replace('_Qps', '_Latency')

def get_success_rate_counter_name(success_rate_counter_name):
  return success_rate_counter_name.replace('_Qps', '_SuccessRate')

def get_counter_name(group, access_type, label):
  label = access_type + "_" + label
  counters = get_counters_by_group_and_label(group, label)
  names = []
  print label
  print counters
  for counter in counters:
    names.append(group + "-" + counter.name)
  return names

def get_counter_name_of_hbase_operation(group, access_type):
  label = access_type + "_" + QPS_LABEL
  qps_counters = get_counters_by_group_and_label(group, label)
  #order by qps desc
  qps_counters = sorted(qps_counters,cmp=lambda x,y:cmp(y.value,x.value))

  #return countrs as : latency, qps and success_rate order by success_rate desc
  counter_names = []
  for qps_counter in qps_counters:
    latency_counter_name = get_latency_counter_name(qps_counter.name)
    success_rate_counter_name = get_success_rate_counter_name(qps_counter.name)
    counter_names.append(group + '-' + qps_counter.name)
    counter_names.append(group + '-' + latency_counter_name)
    counter_names.append(group + '-' + success_rate_counter_name)
  return counter_names

def get_endpoint(group, access_type):
  endpoint = 'unknown'
  label = access_type + "_" + HBASE_OPERATION_LABEL
  counters = get_counters_by_group_and_label(group, label)
  for counter in counters:
    endpoint = counter.host
    endpoint = endpoint.replace(':', '-')
    break
  return endpoint

class Menu:
  def __init__(self, name, path):
    self.name = name
    self.path = path
  def __unicode__(self):
    return u"%s/%s" % (self.name, self.path)

#url: /business/$id/$access_type/$label
def show_business(request, id, access_type, label):
  business = Business.objects.get(id=id)
  group = business.getCounterGroup()
  endpoint = get_endpoint(group, access_type)
  metric_names = []
  if label == HBASE_OPERATION_LABEL:
    metric_names = get_counter_name_of_hbase_operation(group, access_type)
  else:
    metric_names = get_counter_name(group, access_type, label)
  
  params = {
    'business_id' : id,
    'endpoint': endpoint,
    'write_menus' : BUSINESS_METRICS_VIEW_CONFIG['Write HBase'],
    'read_menus' : BUSINESS_METRICS_VIEW_CONFIG['Read HBase'],
    'metric_names' : metric_names,
    'business': business,
  }
  return respond(request, 'business/business.html', params)

def get_online_counters(access_type, label):
  metric_names = ONLINE_METRICS_COUNTER_CONFIG['Online ' + access_type][label]
  titles = ONLINE_METRICS_TITLE_CONFIG['Online ' + access_type][label]
  endpoints = ONLINE_METRICS_ENDPOINT_CONFIG['Online ' + access_type][label]
  metrics = []
  index = 0
  for name in metric_names:
    metric = []
    metric.append(titles[index])
    metric.append(name)
    metric.append(endpoints[index])
    metrics.append(metric)
    index = index + 1
  return metrics

#url: /business/$id/$access_type/$label
def show_online(request, id, access_type, label):
  business = Business.objects.get(id=id)
  metrics = get_online_counters(access_type, label)
  
  params = {
    'business_id' : id,
    'write_menus' : ONLINE_METRICS_MENU_CONFIG['Online Write'],
    'read_menus' : ONLINE_METRICS_MENU_CONFIG['Online Read'],
    'metrics' : metrics,
    'business': business,
  }
  return respond(request, 'business/online.html', params)
