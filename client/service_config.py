#!/usr/bin/env python

import argparse
import deploy_config
import getpass
import os
import re
import socket
import subprocess

from configobj import ConfigObj
from log import Log
from supervisor_client import SupervisorClient

def get_job_host(args, cluster, jobs, val):
  reg_expr = JOB_HOST_ID_REGEX.match(val)
  job = reg_expr.group('job')
  host_id = int(reg_expr.group('id'))
  return jobs[job].hosts[host_id]

def get_port_addition_result(args, cluster, jobs, val):
  reg_expr = JOB_PORT_EXPR_REGEX.match(val)
  job = reg_expr.group('job')
  add_num = int(reg_expr.group('num'))
  return jobs[job].base_port + add_num

def get_service_cluster_name(service, cluster):
  if service == "zookeeper":
    return cluster.zk_cluster
  elif service == "hdfs":
    hdfs_cluster = cluster.hdfs_cluster
    if hdfs_cluster != cluster.name:
      return hdfs_cluster
    else:
      return cluster.name

def get_service_config(args, service, cluster):
  get_short_user_name(args)

  if not getattr(args, service + "_config", None):
    service_args = argparse.Namespace()
    service_args.service = service
    service_args.cluster = get_service_cluster_name(service, cluster)
    setattr(args, service + "_config", ServiceConfig(service_args))
  return getattr(args, service + "_config")

def get_zk_job(args, cluster):
  zk_config = get_service_config(args, "zookeeper", cluster)
  return zk_config.jobs["zookeeper"]

def get_zk_hosts(args, cluster, jobs):
  zk_job = get_zk_job(args, cluster)
  return ",".join(zk_job.hosts.itervalues())

def get_zk_port(args, cluster, jobs):
  zk_job = get_zk_job(args, cluster)
  return zk_job.base_port

def get_zk_hosts_with_port(args, cluster, jobs):
  zk_job = get_zk_job(args, cluster)
  return ",".join(
    ["%s:%d" % (host, zk_job.base_port)
      for host in zk_job.hosts.itervalues()])

def get_service_job_host(args, cluster, jobs, val):
  reg_expr = SERVICE_JOB_HOST_ID_REGEX.match(val)
  service = reg_expr.group('service')
  job = reg_expr.group('job')
  host_id = int(reg_expr.group('id'))
  service_config = get_service_config(args, service, cluster)
  return service_config.jobs[job].hosts[host_id]

def get_zk_server_list(args, cluster, jobs):
  server_list = str()
  job = jobs[jobs.keys()[0]]
  for id, host in job.hosts.iteritems():
    server_list += ("server.%d=%s:%d:%d\n" %
      (id, host, job.base_port + 2, job.base_port + 3))
  return server_list

def get_supervisor_client(host, service, cluster_name, job):
  supervisor_config = deploy_config.get_deploy_config().get_supervisor_config()
  return SupervisorClient(host, supervisor_config.get('server_port'),
    supervisor_config.get('user'), supervisor_config.get('password'),
    service, cluster_name, job)

def get_config_dir(args=None, cluster=None, jobs=None):
  return deploy_config.get_deploy_config().get_config_dir()

def get_short_user_name(args, cluster=None, jobs=None):
  if not getattr(args, "short_user_name", None):
    args.short_user_name = get_short_user_name_full()[1]
  return args.short_user_name

def get_config_path(args):
  return "%s/conf/%s/%s-%s.cfg" % (get_config_dir(),
    args.service, args.service, args.cluster)

def get_short_user_name_full():
  try:
    cmd = ['klist']
    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT,)

    centos_line_prefix = 'Default principal:'
    macos_line_prefix = 'Principal:'
    for line in output.split('\n'):
      if (line.strip().startswith(centos_line_prefix) or
          line.strip().startswith(macos_line_prefix)):
        return True, line.split(':')[1].split('@')[0].strip()
  except subprocess.CalledProcessError, e:
    return False, getpass.getuser()


def get_specific_dir(host, service, cluster_name, job_name, attribute):
  supervisor_client = get_supervisor_client(host, service, cluster_name, job_name)

  if attribute == "data_dir":
    return supervisor_client.get_available_data_dirs()[0]
  elif attribute == "data_dirs":
    return ",".join(supervisor_client.get_available_data_dirs())
  elif attribute == "run_dir":
    return supervisor_client.get_run_dir()
  elif attribute == "log_dir":
    return supervisor_client.get_log_dir()
  elif attribute == "current_package_dir":
    return supervisor_client.get_current_package_dir()

def get_service_job_attribute(args, cluster, jobs, val):
  reg_expr = SERVICE_JOB_ATTRIBUTE_REGEX.match(val)
  service = reg_expr.group('service')
  job_name = reg_expr.group('job')
  attribute = reg_expr.group('attribute')

  service_config = get_service_config(args, service, cluster)
  if attribute == "base_port":
    return service_config.jobs[job_name].base_port
  else:
    host = service_config.jobs[job_name].hosts[0]
    return get_specific_dir(host, service, cluster.name, job_name, attribute)

def get_section_attribute(args, cluster, jobs, val):
  reg_expr = SECTION_ATTRIBUTE_REGEX.match(val)
  section = reg_expr.group('section')
  attribute = reg_expr.group('attribute')

  if section == "cluster":
    return getattr(cluster, attribute)
  else:
    if attribute == "base_port":
      return jobs[section].base_port
    else:
      host = jobs[section].hosts[0]
      return get_specific_dir(host, args.service, cluster.name, section, attribute)


CLUSTER_NAME_REGEX = re.compile(r'((?P<zk>[a-z0-9]+)-)?([a-z0-9]+)')
HOST_RULE_REGEX = re.compile(r'host\.(?P<id>\d+)')
HOST_REGEX = re.compile(r'(?P<host>(.+))$')
VARIABLE_REGEX = re.compile('\$\{(.+?)\}')

SECTION_ATTRIBUTE_REGEX = re.compile('(?P<section>(?!zk\.)\w+)\.(?P<attribute>\w+)$')
JOB_PORT_EXPR_REGEX = re.compile('(?P<job>\w+)\.base_port[+-](?P<num>\d+)')
JOB_HOST_ID_REGEX = re.compile('(?P<job>\w+)\.host\.(?P<id>\d+)')
SERVICE_JOB_ATTRIBUTE_REGEX = re.compile('(?P<service>\w+)\.(?P<job>\w+)\.(?P<attribute>[^0-9]\w+)$')
SERVICE_JOB_HOST_ID_REGEX = re.compile('(?P<service>\w+)\.(?P<job>\w+)\.host\.(?P<id>\d+)')


SCHEMA_MAP = {
  JOB_HOST_ID_REGEX : get_job_host,
  JOB_PORT_EXPR_REGEX : get_port_addition_result,
  SECTION_ATTRIBUTE_REGEX : get_section_attribute,
  SERVICE_JOB_HOST_ID_REGEX : get_service_job_host,
  SERVICE_JOB_ATTRIBUTE_REGEX : get_service_job_attribute,
  "zk.hosts" : get_zk_hosts,
  "zk.base_port" : get_zk_port,
  "zk.hosts_with_port" : get_zk_hosts_with_port,
  "server_list" : get_zk_server_list,
  "config_dir" : get_config_dir,
  "short_user_name" : get_short_user_name,
  # "slaves" : "\n".join(jobs["datanode"].hosts.values()),
}

COMMON_JOB_SCHEMA = {
    # "param_name": (type, default_value)
    # type must be in {bool, int, float, str}
    # if default_value is None, it means it's NOT an optional parameter.
    "base_port": (int, None),
    "xmx": (int, None),
    "xms": (int, None),
    "xmn": (int, None),
    "max_direct_memory": (int, None),
    "max_perm_size": (int, None),
}

CLUSTER_SCHEMA = {
  "name": (str, None),
  "version": (str, None),
  "jobs": (str, None),
  "kerberos_realm": (str, "XIAOMI.NET"),
  "kerberos_username": (str, ""),
  "ganglia_address" : (str, ""),
  "package_name": (str, ""),
  "revision": (str, ""),
  "timestamp": (str, ""),
  "hdfs_cluster": (str, ""),
}

class ServiceConfig:
  '''
  The class represents the configuration of a service.
  '''
  def __init__(self, args):
    self.config_dict_full = self.get_config_dict_full(
      get_config_path(args))

    self.cluster_dict = self.config_dict_full["cluster"]
    self.configuration_dict = self.config_dict_full["configuration"]

    self.cluster = ServiceConfig.Cluster(self.cluster_dict, args.cluster)
    self.jobs = {}
    for job_name in self.cluster.jobs:
      self.jobs[job_name] = ServiceConfig.Jobs(
        self.config_dict_full[job_name], job_name)
    self.configuration = ServiceConfig.Configuration(
      self.configuration_dict, args, self.cluster, self.jobs)

  class Cluster:
    '''
    The class represents a service cluster
    '''
    def __init__(self, cluster_dict, cluster_name):
      ServiceConfig.parse_params(self, "cluster", cluster_dict, CLUSTER_SCHEMA)

      self.jobs = self.jobs.split()
      if self.name != cluster_name:
        Log.print_critical(
          "Cluster name in config doesn't match the config file name: "
          "%s vs. %s" % (self.name, cluster_name))
      reg_expr = CLUSTER_NAME_REGEX.match(self.name)
      if not reg_expr:
        Log.print_critical("Illegal cluster name: %s" % self.name)
      self.zk_cluster = reg_expr.group("zk")

  class Jobs:
    '''
    The class represents all the jobs of a service
    '''
    def __init__(self, job_dict, job_name):
      self.name = job_name
      ServiceConfig.parse_params(self, job_name, job_dict, COMMON_JOB_SCHEMA)
      if self.base_port % 100 != 0:
        Log.print_critical("base_port %d is NOT a multiple of 100!" %
                            self.base_port)
      self.hosts = {}
      self.hostnames = {}
      for name, value in job_dict.iteritems():
        reg_expr = HOST_RULE_REGEX.match(name)
        if not reg_expr:
          continue
        host_id = int(reg_expr.group("id"))
        reg_expr = HOST_REGEX.match(value)
        if not reg_expr:
          Log.print_critical("Host/IP address expected on rule: %s = %s" %
                            (name, value))
        ip = reg_expr.group("host")
        self.hosts[host_id] = ip
        try:
          self.hostnames[host_id] = socket.gethostbyaddr(ip)[0]
        except:
          self.hostnames[host_id] = ip

  class Configuration:
    '''
    The class represents all the config files to be generated of a service
    '''
    def __init__(self, configuration_dict, args, cluster, jobs):
      self.config_section_dict = configuration_dict
      self.raw_files, self.generated_files = ServiceConfig.parse(
        self.config_section_dict, args, cluster, jobs)


  def get_config_dict_full(self, config_path):
    '''
    Get the whole configuration dict: reading the base common-config and
    using the child_config_dict to update the base_config_dict

    @param   config_path      The path for configuration file
    @return  dict             The whole configuration dict
    '''
    base_config_dict = {}
    child_config_dict = ConfigObj(config_path, file_error=True)

    if child_config_dict['configuration'].has_key('base'):
      config_path = child_config_dict['configuration']['base']

      if config_path.find('$') != -1:
        config_path = self.parse_item(None, None, None, config_path)

      base_config_dict = self.get_config_dict_full(config_path)
      child_configuration_dict = child_config_dict['configuration']
      base_configuration_dict = base_config_dict['configuration']

      for file_name, file_dict in base_configuration_dict.iteritems():
        if file_name in child_configuration_dict:
          file_dict.update(child_configuration_dict[file_name])

      base_config_dict['configuration'].update(base_configuration_dict)
      child_config_dict.update(base_config_dict)
    return child_config_dict


  @staticmethod
  def parse_params(namespace, section_name, section_dict, schema):
    '''
    Parse the parameters specified by the schema dict from the specific section dict
    '''
    for param_name, param_def in schema.iteritems():
      if param_name in section_dict:
        if param_def[0] is bool:
          param_value = section_dict.as_bool(param_name)
        elif param_def[0] is int:
          param_value = section_dict.as_int(param_name)
        elif param_def[0] is float:
          param_value = section_dict.as_float(param_name)
        else:
          param_value = section_dict[param_name]
      else:
        # option not found, use the default value if there is.
        if param_def[1] is None:
          Log.print_critical("required option %s missed in section %s!" %
            (param_name, section_name))
        else:
          param_value = param_def[1]
      setattr(namespace, param_name, param_value)


  @staticmethod
  def parse_item(args, cluster, jobs, item):
    '''
    Parse item which is enclosed by '${}' in key/value
    '''
    reg_expr = VARIABLE_REGEX.findall(item)
    new_item = []
    for iter in range(len(reg_expr)):
      for key, callback in SCHEMA_MAP.iteritems():
        if reg_expr[iter] == key:
          new_item.append(callback(args, cluster, jobs))
        elif type(key) == type(VARIABLE_REGEX) and key.match(reg_expr[iter]):
          new_item.append(callback(args, cluster, jobs, reg_expr[iter]))

    for iter in range(len(new_item)):
      item = item.replace("${"+reg_expr[iter]+"}", str(new_item[iter]))
    return item


  @staticmethod
  def parse(config_section_dict, args, cluster, jobs):
    '''
    Parse and calculate key/value which contains '${}',
    generate configuration file dicts as {file_name : file_dict}
    and read local configuration files as {file_name : file_content_str}
    '''
    raw_files = {}
    for file_name, file_dict in config_section_dict.iteritems():
      if type(file_dict) == str:
        file_dict = ServiceConfig.parse_item(args, cluster, jobs, file_dict)
        if os.path.exists(file_dict):
          raw_files[file_name] = open(file_dict).read()
        else:
          raw_files[file_name] = str()
      else:
        for key, value in file_dict.iteritems():
          if key.find('$') != -1:
            file_dict.pop(key)
            key = ServiceConfig.parse_item(args, cluster, jobs, key)
            file_dict[key] = value
          if value.find('$') != -1:
            value = ServiceConfig.parse_item(args, cluster, jobs, value)
            file_dict[key] = value

    generated_files = {}
    for file_name, file_dict in config_section_dict.iteritems():
      if type(file_dict) != str:
        generated_files[file_name] = file_dict

    return raw_files, generated_files


