import argparse
import cStringIO
import deploy_config
import getpass
import hashlib
import os
import pprint
import re
import service_config
import socket
import string
import subprocess
import sys
import telnetlib
import time
import urllib2
import uuid

from log import Log
from service_config import ServiceConfig
from supervisor_client import SupervisorClient
from tank_client import TankClient

HOST_TASK_REGEX = re.compile('(?P<host>\d+)(\.(?P<task>\d+))?$')

SUPERVISOR_SUCCESS = "OK"

PARALLEL_DEPLOY_JOBS = ["datanode", "regionserver", "nodemanager",
  "historyserver", "impalad", "supervisor", "logviewer", "kafka",
  "kafkascribe"]

STOPPED_STATUS = ["STOPPED", "BACKOFF", "EXITED", "FATAL"]

HADOOP_PROPERTY_PREFIX = "hadoop.property."
HADOOP_CONF_PATH = "/etc/hadoop/conf"
LATEST_PACKAGE_INFO_URI = "get_latest_package_info"
DOWNLOAD_PACKAGE_URI = "packages"

FAKE_SVN_VERSION = "12345"

class Template(string.Template):
  # the orginal delimiter '$' is also commonly used by shell script, so
  # overwrite to '%' here.
  delimiter = '%'


def get_deploy_config():
  return deploy_config.get_deploy_config()

def get_real_instance_id(instance_id):
  return service_config.get_real_instance_id(instance_id)

def get_base_port(base_port, instance_id):
  return service_config.get_base_port(base_port, instance_id)

def get_http_service_uri(host, base_port, instance_id):
  return 'http://%s:%d' % (host,
    get_base_port(base_port, instance_id) + 1)

def get_host_id(hosts, host_ip):
  for id, host in hosts.iteritems():
    if host_ip == host.ip:
      return id
  Log.print_critical("Invalid host ip: %s, please check your config." % host_ip)

def get_task_id(hosts, host_id, instance_id):
  instance_id = 0 if (instance_id == -1) else instance_id
  task_id = 0
  for id, host in hosts.iteritems():
    if host_id == id:
      task_id += instance_id
      break
    else:
      task_id += host.instance_num
  return task_id

def get_service_log_level(args, service_config):
  if args.log_level:
    return args.log_level
  else:
    return service_config.cluster.log_level

def get_local_package_path_general(path, artifact, version):
  '''
  Get the local tarball path of the package of specified artifact and version

  @param  path      the base path of the tarball
  @param  artifact  the artifact of the package
  @param  version   the version of the package
  @return string    the full path of the tarball

  Note: This method is for internal use, users shouldn't call it directly.
    Users who want to obtain the local package path should call
    get_local_package_path().
  '''
  return ("%s/%s-%s.tar.gz" % (path, artifact, version))

def get_local_package_path(artifact, version):
  '''
  Get the local tarball path of the package of specified artifact and version

  @param  artifact  the artifact of the package
  @param  version   the version of the package
  @return string    the full path of the tarball
  '''
  if artifact == "zookeeper":
    package_path = get_local_package_path_general(
        get_deploy_config().get_zookeeper_package_dir(),
        artifact, version)
  elif artifact == "hadoop":
    package_path = get_local_package_path_general(
        get_deploy_config().get_hadoop_package_dir(),
        artifact, version)
  elif artifact == "hbase":
    package_path = get_local_package_path_general(
        get_deploy_config().get_hbase_package_dir(),
        artifact, version)
  elif artifact == "impala-shell" or artifact == "impala":
    package_path = get_local_package_path_general(
        get_deploy_config().get_imapala_package_dir(),
        artifact, version)
  elif artifact == "kafka":
    package_path = get_local_package_path_general(
        get_deploy_config().get_kafka_package_dir(),
        artifact, version)
  elif artifact == "apache-storm":
    package_path = get_local_package_path_general(
        get_deploy_config().get_storm_package_dir(),
        artifact, version)
  elif artifact == "galaxy":
    package_path = get_local_package_path_general(
        get_deploy_config().get_galaxy_package_dir(),
        artifact, version)
  elif artifact == 'chronos':
    package_path = get_local_package_path_general(
        get_deploy_config().get_chronos_package_dir(),
        artifact, version)
  else:
    Log.print_critical("Unknow artifact: %s" % artifact)
  return package_path

def get_revision_number(cmd, output_prefix, work_space_dir):
  env = os.environ
  # Enforce English locale.
  env["LC_ALL"] = "C"
  current_work_dir = os.getcwd()
  os.chdir(work_space_dir)
  content = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
  os.chdir(current_work_dir)
  for line in content.splitlines():
    if line.startswith(output_prefix):
      return line[len(output_prefix):]

def generate_package_revision(root):
  '''
  Get the revision of the package. Currently, svn revision and git commit are
  supported. If the package directory is neither a svn working directory nor
  a git working directory, a fake revision will be returned.

  @param  root   the local package root directory
  @return string the revision of the package
  '''
  if os.path.islink(root):
    real_path = os.readlink(root)
    if not real_path.startswith('/'):
      abs_path = "%s/%s" % (os.path.dirname(root), real_path)
    else:
      abs_path = real_path
  else:
    abs_path = root

  try:
    try:
      cmd = ["svn", "info"]
      revision_prefix = "Revision: "
      return "r%s" % get_revision_number(cmd, revision_prefix, abs_path)
    except:
      cmd = ["git", "show"]
      commit_prefix = "commit "
      return get_revision_number(cmd, commit_prefix, abs_path)
  except:
    # We cannot get the version No., just return a fake one
    return "r%s" % FAKE_SVN_VERSION

def generate_checksum(path):
  '''
  Generate the SHA-1 digest of specified file.

  @param  path   the path of the file
  @return string the SHA-1 digest
  '''
  fd = open(path, "r")
  sha1 = hashlib.sha1()
  while True:
    buffer = fd.read(4096)
    if not buffer: break
    sha1.update(buffer)
  fd.close()
  return sha1.hexdigest()

def upload_package(args, artifact, version):
  '''
  Upload the specified package to the package server(Tank). Note that
  if the file with the same checksum is already uploaded, this uploading
  will be skipped.

  @param  args     the command line arguments object parsed by artparse.py
  @param  artifact the artifact of the package
  @param  version  the version of the package
  @return dict     the package information return by the package server
  '''
  package_path = get_local_package_path(artifact, version)
  Log.print_info("Uploading pacakge: %s" % package_path)

  revision = generate_package_revision(get_root_dir(args.service))
  Log.print_success("Revision is: %s" % revision)

  Log.print_info("Generating checksum of package: %s" % package_path)
  checksum = generate_checksum(package_path)
  Log.print_success("Checksum is: %s" % checksum)

  tank_client = get_tank_client()
  package_info = tank_client.check_package(artifact, checksum)

  if not package_info:
    if 200 == tank_client.upload(package_path, artifact, revision):
      Log.print_success("Upload package %s success" % package_path)
      package_info = tank_client.check_package(artifact, checksum)
      return eval(package_info)
  else:
    Log.print_warning("Package %s has already uploaded, skip uploading" %
        package_path)
    return eval(package_info)
  return None

def generate_site_xml(args, template_dict):
  '''
  Generate the *-site.xml file according to the given properties dict.

  @param  args          the argument object parsed by argparse
  @param  template_dict the properties dict
  @return string        the generated file content
  '''
  template_path = "%s/site.xml.tmpl" % get_template_dir()

  template = Template(open(template_path).read())
  config_value = ""
  keys = template_dict.keys()
  keys.sort()
  for key in keys:
    config_value += """
  <property>
    <name>%s</name>
    <value>%s</value>
  </property>
""" % (key, template_dict[key])
  return template.substitute({"config_value": config_value})

def generate_properties_file(args, template_dict):
  '''
  Generate the *.properties file according to the given properties dict.

  @param  args          the argument object parsed by argparse
  @param  template_dict the properties dict
  @return string        the generated file content
  '''
  template_path = "%s/properties.tmpl" % get_template_dir()

  template = Template(open(template_path).read())
  return template.substitute(
      {"config_value":
          "\n".join(["%s=%s" % (k, v) for k, v in template_dict.iteritems()])})

def generate_yaml_file(yaml_dict):
  '''
  Generate the yaml format config file according to the given yaml dict.

  @param  yaml_dict     the yaml dict
  @return string        the generated file content
  '''
  NESTING_DICT_REGEX = re.compile('\{(?P<consumers>.+?)\}')
  yaml_format_string = ""
  for key, value in yaml_dict.iteritems():
    yaml_format_string += key
    reg_expr = NESTING_DICT_REGEX.match(value)
    # the format of consumers: 
    #   different consumers separated by ';'
    #   different key-value pairs separated by ',' within the same consumer
    #   the key and value separated by ':'
    # for example: '{class:consumer_1,parallelism.hint:xx;class:consumer_2,...}'
    if reg_expr:
      consumers = reg_expr.group('consumers')
      consumer_list = consumers.split(';')
      # process consumers one by one
      for consumer in consumer_list:
        key_value_list = consumer.split(',')
        class_name = key_value_list[0].split(':')[1]
        yaml_format_string += ":\n  - class: %s\n" % class_name
        # process parallelism and other arguments
        for key_value_pair in key_value_list[1:]:
          key, value = key_value_pair.split(':')
          yaml_format_string += "    %s: %s\n" % (key, value)
    elif value.find(',') != -1:
      yaml_format_string += ":\n"
      for item in value.split(','):
        yaml_format_string += "  - %s\n" % item
    else:
      yaml_format_string += ": %s\n" % value

  return yaml_format_string

def create_run_script(template_path, template_dict):
  '''
  Generate the run script of given script template and variables dict.

  @param  template_path  the script template path
  @param  template_dict  the variables dict
  @return string         the generated file content
  '''
  template = Template(open(template_path).read())
  content = template.safe_substitute(template_dict)
  return content

def get_template_dir():
  '''
  Get the config templates directory.
  '''
  return '%s/template' % get_deploy_config().get_config_dir()

def get_config_dir():
  '''
  Get the service config directory.
  '''
  return get_deploy_config().get_config_dir()

def get_root_dir(service):
  '''
  Get the local root directory of specified service.

  @param  service  the service name
  @return string   the local root directory of the service
  '''
  if service == "hdfs" or service == "yarn" or service == "mapreduce":
    return get_deploy_config().get_hadoop_root()
  if service == "hbase":
    return get_deploy_config().get_hbase_root()
  if service == "zookeeper":
    return get_deploy_config().get_zookeeper_root()
  if service == "impala":
    return get_deploy_config().get_impala_root()
  if service == "kafka":
    return get_deploy_config().get_kafka_root()
  if service == "storm":
    return get_deploy_config().get_storm_root()
  if service == "fds":
    return get_deploy_config().get_galaxy_root()
  if service == "chronos":
    return get_deploy_config().get_chronos_root()
  Log.print_critical("Unknow service: %s" % service)

def get_supervisor_client(host, service, cluster, job, instance_id=-1):
  '''
  A factory method to construct a supervisor client object.

  @param  host        the remote server's host
  @param  service     the service name
  @param  cluster     the cluster name
  @param  job         the job name
  @param  instance_id the instance id
  @return object      the supervisor client object
  '''
  return service_config.get_supervisor_client(host, service, cluster, job, instance_id)

def get_tank_client():
  '''
  A factory method to construct a tank(package server) client object.
  '''
  deploy_config = get_deploy_config()
  tank_config = deploy_config.get_tank_config()

  return TankClient(tank_config.get('server_host'),
      tank_config.get('server_port'))

def get_service_config(args):
  '''
  Get service config, without any dependencies.

  @param  args       the command line arguments object parsed by argparse
  '''
  service_config.get_short_user_name(args)
  if not getattr(args, args.service + "_config", None):
    setattr(args, args.service+"_config", ServiceConfig(args))
  return getattr(args, args.service+"_config")

def generate_service_token(service, cluster):
  '''
  Generate a token used to bootstrap and cleanup.

  @param  service the service name
  @param  cluster the cluster name
  @return string  the generated token
  '''
  return str(uuid.uuid3(uuid.NAMESPACE_DNS,'%s-%s' % (
          service, cluster)))

def check_input(input, yes='y'):
  '''
  Check if the input string is yes or not.
  '''
  return input.strip().lower() == yes.lower()


def check_admin_priviledge(args):
  '''
  Check if the current user is in the administrators list or not. Note that
  this will be checked only when security is enabled.
  '''
  status, short_user_name = service_config.get_short_user_name_full()
  args.short_user_name = short_user_name

  if is_security_enabled(args):
    if status:
      admin_list = get_deploy_config().get_admin_list()
      if short_user_name not in admin_list:
        Log.print_critical("User %s is not an authorized administrator, "
          "this operation can't be processed" % short_user_name)
    else:
      Log.print_critical('You must kinit your kerberos principal first')

def is_security_enabled(args):
  '''
  Determine if security is enabled or not.
  '''
  get_service_config(args)

  if args.service == "zookeeper":
    return len(args.zookeeper_config.configuration.generated_files["jaas-server.conf"]) != 0
  elif args.service == "hdfs":
    core_site_dict = args.hdfs_config.configuration.generated_files["core-site.xml"]
    return (core_site_dict["hadoop.security.authentication"] == "kerberos") and (
             core_site_dict["hadoop.security.authorization"] == "true")
  elif args.service == "yarn":
    core_site_dict = args.yarn_config.configuration.generated_files["core-site.xml"]
    return (core_site_dict["hadoop.security.authentication"] == "kerberos") and (
             core_site_dict["hadoop.security.authorization"] == "true")
  elif args.service == "hbase":
    hbase_site_dict = args.hbase_config.configuration.generated_files["hbase-site.xml"]
    return (hbase_site_dict["hbase.security.authentication"] == "kerberos") and (
             hbase_site_dict["hbase.security.authorization"] == "true")
  elif args.service == "impala":
    core_site_dict = args.impala_config.configuration.generated_files["core-site.xml"]
    return (core_site_dict["hadoop.security.authentication"] == "kerberos") and (
             core_site_dict["hadoop.security.authorization"] == "true")
  elif args.service == "fds":
    core_site_dict = args.fds_config.configuration.generated_files["core-site.xml"]
    return (core_site_dict["hadoop.security.authentication"] == "kerberos") and (
             core_site_dict["hadoop.security.authorization"] == "true")
  elif args.service == "chronos":
    chronos_dict = args.chronos_config.configuration.generated_files["chronos.cfg"]
    return (chronos_dict["zkSecure"] == "true")
  else:
    return False

def confirm_bootstrap(service, service_config):
  '''
  Let the users confirm bootstrap interactively. Users will be asked to
  set a password, or a random password will be given. The password is
  the verification token when users want to do cleanup.
  '''
  Log.print_warning("You should set a bootstrap password, " \
      "it will be requried when you do cleanup")
  password = str()
  input = raw_input("Set a password manually? (y/n) ")
  if check_input(input):
    input = getpass.getpass("Please input your password: ")
    if len(input.strip()) >= 6:
      password = input.strip()
    else:
      Log.print_critical("The length of the password is at least 6")
  else:
    Log.print_info("A random password will be generated")
    password = generate_service_token(service, service_config.cluster.name)

  Log.print_warning("Your password is: %s, you should store this " \
      "in a safe place, because this is the verification code used " \
      "to do cleanup" % password)
  return password

def confirm_action(args, action):
  '''
  Let the users confirm the specify action interactively.
  '''
  Log.print_warning("You will %s the cluster \033[31;1m%s\033[0;33m, "
      "do you really want to do this?" % (action, args.cluster))
  token = generate_random_confirm_token()
  input = raw_input("Please input \033[31;1m%s\033[0m to confirm: " % token)
  if check_input(input, token):
    Log.print_info("Begin to %s the cluster" % action)
  else:
    Log.print_critical("%s canceled" % action.capitalize())

def confirm_cleanup(args, service, service_config):
  '''
  Let the user confirm cleanup interactively. Users will be asked to input
  the password set when the service is bootstrapped.
  '''
  confirm_action(args, 'cleanup')

  input = getpass.getpass("Please input your installation password: ")
  if len(input.strip()) >= 6:
    return input.strip()
  else:
    Log.print_critical("The length of the password is at least 6")

def confirm_stop(args):
  '''
  Let the user confirm the stop action interactively.
  '''
  confirm_action(args, 'stop')

def confirm_start(args):
  '''
  Let the user confirm the start action interactively.
  '''
  confirm_action(args, 'start')

def confirm_restart(args):
  '''
  Let the user confirm the restart action interactively.
  '''
  confirm_action(args, 'restart')

def install_service(args, service, service_config, artifact):
  '''
  Install the specified service. Here installation means uploading the
  service package to the package server(Tank).

  @param args           the command line arguments object
  @param service        the service name
  @param service_config the service config object
  @param artifact       the artifact name
  '''
  Log.print_info("Installing %s to package server" % artifact)
  package_info = upload_package(args, artifact, service_config.cluster.version)
  if package_info:
    Log.print_success("Install %s to package server success" % artifact)
    pprint.pprint(package_info)
  else:
    Log.print_critical("Install %s to package server fail" % artifact)

def cleanup_job(service, service_config, host, job_name,
    instance_id, cleanup_token, cleanup_script=""):
  '''
  Clean up a task of the specified service and job. Note that cleanup
  requires that the task must be stopped, so users should stop the task
  before cleanup.

  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  @param instance_id     the instance id
  @param cleanup_token   the token used to verify cleanup
  @param cleanup_script  the user supplied cleanup script
  @param artifact        the artifact name
  '''
  real_instance_id = get_real_instance_id(instance_id)
  host_id = get_host_id(service_config.jobs[job_name].hosts, host)
  task_id = get_task_id(service_config.jobs[job_name].hosts, host_id, instance_id)
  Log.print_info("Cleaning up task %d of %s on %s(%d)" % (
    task_id, job_name, host, real_instance_id))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name, instance_id)
  message = supervisor_client.cleanup(cleanup_token, cleanup_script)
  if SUPERVISOR_SUCCESS == message:
    Log.print_success("Cleanup task %d of %s on %s(%d) success" % (
      task_id, job_name, host, real_instance_id))
  else:
    Log.print_error("Cleanup task %d of %s on %s(%d) fail: %s" % (
      task_id, job_name, host, real_instance_id, message))

def bootstrap_job(args, artifact, service, service_config, host, job_name, instance_id,
    cleanup_token, data_dir_indexes='0', bootstrap_script='', **config_files):
  '''
  Bootstrap a task of the specified service and job. Note that before
  bootstrapping users should ensure that the data and log directories at
  the server side are empty.

  @param args             the command line arguments object
  @param artifact         the artifact name
  @param service          the service name
  @param service_config   the service config object
  @param host             the host of the task
  @param job_name         the job name
  @param instance_id      the instance id
  @param cleanup_token    the token used to verify cleanup
  @param data_dir_indexes the data directory indexes
  @param bootstrap_script the user supplied bootstrap script
  @param config_files     the config files dict
  '''
  real_instance_id = get_real_instance_id(instance_id)
  host_id = get_host_id(service_config.jobs[job_name].hosts, host)
  task_id = get_task_id(service_config.jobs[job_name].hosts, host_id, instance_id)
  Log.print_info("Bootstrapping task %d of %s on %s(%d)" % (
    task_id, job_name, host, real_instance_id))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name, instance_id)

  try:
    if (service_config.cluster.package_name and service_config.cluster.revision
        and service_config.cluster.timestamp):
      message = supervisor_client.bootstrap(artifact,
          package_name=service_config.cluster.package_name,
          revision=service_config.cluster.revision,
          timestamp=service_config.cluster.timestamp,
          cleanup_token=cleanup_token,
          bootstrap_script=bootstrap_script,
          data_dir_indexes=data_dir_indexes,
          **config_files)
    elif args.update_package:
      message = supervisor_client.bootstrap(artifact, force_update=True,
          cleanup_token=cleanup_token, bootstrap_script=bootstrap_script,
          data_dir_indexes=data_dir_indexes, **config_files)
    else:
      message = supervisor_client.bootstrap(artifact,
          package_name=args.package_name, revision=args.revision,
          timestamp=args.timestamp, cleanup_token=cleanup_token,
          bootstrap_script=bootstrap_script, data_dir_indexes=data_dir_indexes,
          **config_files)
    if SUPERVISOR_SUCCESS == message:
      Log.print_success("Bootstrap task %d of %s on %s(%d) success" % (
        task_id, job_name, host, real_instance_id))
    else:
      Log.print_critical("Bootstrap task %d of %s on %s(%d) fail: %s" % (
        task_id, job_name, host, real_instance_id, message))

  except BaseException, e:
    message = str(e)
    Log.print_error("Bootstrap task %d of %s on %s(%d) fail: %s" % (
      task_id, job_name, host, real_instance_id, message))

def start_job(args, artifact, service, service_config, host, job_name,
    instance_id, start_script, http_url, **config_files):
  '''
  Start the task of specified service and job.

  @param args            the command line arguments object
  @param artifact        the artifact name
  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  @param instance_id     the instance id
  @param start_script    the user supplied start script
  @param http_url        the task's http entry url
  @param config_files    the config files dict
  '''
  real_instance_id = get_real_instance_id(instance_id)
  host_id = get_host_id(service_config.jobs[job_name].hosts, host)
  task_id = get_task_id(service_config.jobs[job_name].hosts, host_id, instance_id)
  Log.print_info("Starting task %d of %s on %s(%d)" % (
    task_id, job_name, host, real_instance_id))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name, instance_id)

  if not args.update_config:
    config_files = dict()
    start_script = ""

  if (service_config.cluster.package_name and service_config.cluster.revision
      and service_config.cluster.timestamp):
    message = supervisor_client.start(artifact,
        package_name=service_config.cluster.package_name,
        revision=service_config.cluster.revision,
        timestamp=service_config.cluster.timestamp,
        http_url=http_url, start_script=start_script,
        **config_files)
  elif args.update_package:
    message = supervisor_client.start(artifact, force_update=True,
        http_url=http_url, start_script=start_script, **config_files)
  else:
    message = supervisor_client.start(artifact, package_name=args.package_name,
        revision=args.revision, timestamp=args.timestamp, http_url=http_url,
        start_script=start_script, **config_files)
  if SUPERVISOR_SUCCESS == message:
    Log.print_success("Start task %d of %s on %s(%d) success" % (
      task_id, job_name, host, real_instance_id))
  else:
    Log.print_error("Start task %d of %s on %s(%d) fail: %s" % (
      task_id, job_name, host, real_instance_id, message))

def stop_job(service, service_config, host, job_name, instance_id):
  '''
  Stop the task of specified service and job.

  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  @param instance_id     the instance id
  '''
  real_instance_id = get_real_instance_id(instance_id)
  host_id = get_host_id(service_config.jobs[job_name].hosts, host)
  task_id = get_task_id(service_config.jobs[job_name].hosts, host_id, instance_id)
  Log.print_info("Stopping task %d of %s on %s(%d)" % (
    task_id, job_name, host, real_instance_id))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name, instance_id)
  message = supervisor_client.stop()
  if SUPERVISOR_SUCCESS == message:
    Log.print_success("Stop task %d of %s on %s(%d) success" % (
      task_id, job_name, host, real_instance_id))
  else:
    Log.print_error("Stop task %d of %s on %s(%d) fail: %s" % (
      task_id, job_name, host, real_instance_id, message))

def show_job(service, service_config, host, job_name, instance_id):
  '''
  Show the state the task of specified service and job.

  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  @param instance_id     the instance id
  '''
  real_instance_id = get_real_instance_id(instance_id)
  host_id = get_host_id(service_config.jobs[job_name].hosts, host)
  task_id = get_task_id(service_config.jobs[job_name].hosts, host_id, instance_id)
  Log.print_info("Showing task %d of %s on %s(%d)" % (
    task_id, job_name, host, real_instance_id))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name, instance_id)
  state = supervisor_client.show()
  if state == 'RUNNING':
    Log.print_success("Task %d of %s on %s(%d) is %s" % (
      task_id, job_name, host, real_instance_id, state))
  else:
    Log.print_error("Task %d of %s on %s(%d) is %s" % (
      task_id, job_name, host, real_instance_id, state))

def check_service(host, port):
  '''
  Check whether the given host:port is accessable or not.
  '''
  t = telnetlib.Telnet()
  try:
    t.open(host, port)
  except:
    return False
  t.close()
  return True

def check_job_stopped(service, cluster, job, host, instance_id):
  '''
  Check whether a specified task is already stopped or not.
  '''
  supervisor_client = get_supervisor_client(host,
      service, cluster, job, instance_id)
  status = supervisor_client.show()
  return status in STOPPED_STATUS

def wait_for_job_stopping(service, cluster, job, host, instance_id):
  '''
  Wait for a specified job to be stopped.
  '''
  while not check_job_stopped(service, cluster, job, host, instance_id):
    Log.print_warning("Wait for instance %d of %s on %s stopping" % (
      get_real_instance_id(instance_id), job, host))
    time.sleep(2)

def check_job_started(service, cluster, job, host, instance_id):
  '''
  Check whether a specified task is already started or not.
  '''
  supervisor_client = get_supervisor_client(host,
      service, cluster, job, instance_id)
  status = supervisor_client.show()
  return status == 'RUNNING'

def wait_for_job_starting(service, cluster, job, host, instance_id):
  '''
  Wait for a specified job to be started.
  '''
  # Wait 10 seconds to let supervisord start the task
  time.sleep(10)
  if not check_job_started(service, cluster, job, host, instance_id):
    Log.print_critical('Instance %d of %s on %s start failed' % (
      get_real_instance_id(instance_id), job, host))


def get_package_uri(artifact, package_name, revision, timestamp):
  tank_config = get_deploy_config().get_tank_config()

  return 'http://%s:%s/%s/%s/%s-%s/%s' % (tank_config['server_host'],
      tank_config['server_port'], DOWNLOAD_PACKAGE_URI, artifact,
      revision, timestamp, package_name)

def get_query_latest_package_info_uri(artifact, package_name):
  tank_config = get_deploy_config().get_tank_config()

  return 'http://%s:%s/%s/?artifact=%s&package_name=%s' % (
    tank_config['server_host'], tank_config['server_port'],
    LATEST_PACKAGE_INFO_URI, artifact, package_name)

def get_latest_package_info(artifact, package_name):
  uri = get_query_latest_package_info_uri(artifact, package_name)
  info_fp = urllib2.urlopen(uri, None, 30)
  info = info_fp.read()

  if info and info.startswith('{'):
    info_dict = eval(info)
    info_fp.close()
    return info_dict
  else:
    info_fp.close()
    return None

def check_cluster_version(cluster, specified_package_name):
  if specified_package_name.find(cluster.version) == -1:
    Log.print_critical("The version: %s is inconsistent with " \
      "the package_name: %s" % (cluster.version, specified_package_name))

def get_package_info(args, artifact, cluster):
  if (cluster.package_name and cluster.revision and cluster.timestamp):
    check_cluster_version(cluster, cluster.package_name)
    package_name = cluster.package_name
    revision = cluster.revision
    timestamp = cluster.timestamp
  elif (args.package_name and args.revision and args.timestamp):
    check_cluster_version(cluster, args.package_name)
    package_name = args.package_name
    revision = args.revision
    timestamp = args.timestamp
  else:
    package_info = get_latest_package_info(artifact,
      artifact + "-" + cluster.version + ".tar.gz")
    if package_info:
      package_name = package_info.get('package_name')
      revision = package_info.get('revision')
      timestamp = package_info.get('timestamp')
    else:
      Log.print_critical("No package found on package server of %s" %
        artifact + "-" + cluster.version + ".tar.gz")

  return {
    "package_name": package_name,
    "revision": revision,
    "timestamp": timestamp,
  }

def print_progress_bar(message):
  sys.stdout.write(message)
  sys.stdout.flush()

def download_package(download_uri, dest_file):
  try:
    data_file = urllib2.urlopen(download_uri, None, 30)
    data_size = int(dict(data_file.headers).get('content-length'))
  except urllib2.HTTPError, e:
    Log.print_critical("Not found package for uri: %s" % download_uri)

  if not os.path.exists(os.path.dirname(dest_file)):
    os.makedirs(os.path.dirname(dest_file))
  fp = open(dest_file, 'ab')

  read_unit_size = 1048576 # read at most 1M every time
  read_size = 0
  bar_length = 70  # print 70 '='
  speed_max_length = 11 # for example, 1023.99KB/s

  Log.print_info("Package downloading...\nLength: %s bytes\nSaving to %s" % (
    data_size, dest_file))
  start_time = time.time()
  while read_size < data_size:
    read_data = data_file.read(read_unit_size)
    fp.write(read_data)
    read_size += len(read_data)
    progress_bar = '=' * int(float(read_size) / data_size * bar_length)

    download_time = int(time.time() - start_time) + 1
    download_percent = int(float(read_size) / data_size * 100)
    blank_bar = " " * (bar_length - len(progress_bar))
    read_size_str = format(read_size, ',')

    download_speed = float(read_size)/download_time
    if download_speed >= 1024 * 1024:
      download_speed = format(download_speed / (1024 * 1024), '.2f') + 'M' # MB/s
    elif download_speed >= 1024:
      download_speed = format(download_speed / 1024, '.2f') + 'K'          # KB/s
    else:
      download_speed = format(download_speed, '.2f')                       # B/s

    speed_blanks = ' ' * (speed_max_length - len(download_speed) - len('B/s'))
    print_progress_bar(str(download_percent) + "% [" + progress_bar +
      ">" + blank_bar + "] " + read_size_str + "  " + speed_blanks +
      download_speed + "B/s\r")

  print_progress_bar("\n")
  Log.print_info("Download complete.")
  fp.close()
  data_file.close()

def make_package_download_dir(args, artifact, cluster):
  package_info = get_package_info(args, artifact, cluster)
  package_download_path = "%s/%s/%s-%s/%s" % (
    get_deploy_config().get_package_download_root(), artifact,
    package_info['revision'], package_info['timestamp'], package_info['package_name'])

  # check if the tarball is already downloaded, if not, download it
  if not os.path.exists(package_download_path):
    package_uri = get_package_uri(artifact, package_info['package_name'],
      package_info['revision'], package_info['timestamp'])
    download_package(package_uri, package_download_path)

  # unpack the tarball
  package_download_dir = package_download_path[
    0: len(package_download_path) - len('.tar.gz')]
  if not os.path.exists(package_download_dir):
    cmd = ['tar', '-zxf', package_download_path, '-C', os.path.dirname(package_download_dir)]
    subprocess.check_call(cmd)

  return package_download_dir

def get_artifact_package_root(args, cluster, artifact):
  '''
  Get the artifact package root directory
  '''
  if artifact == 'hbase':
    package_path = "hbase-%s/hbase-%s" % (cluster.version, cluster.version)
  else:
    package_path = "%s-%s" % (artifact, cluster.version)

  artifact_package_root = "%s/%s" % (
    eval("get_deploy_config().get_" + artifact + "_package_dir()"), package_path)

  if os.path.exists(artifact_package_root):
    return artifact_package_root
  else:
    return make_package_download_dir(args, artifact, cluster)

def parse_shell_command(args, command_dict):
  '''
  Parse the shell command and its options from the command line arguements.
  '''
  if len(args.command) == 0 or args.command[0] == 'help':
    print_shell_help_info(command_dict)
    return (None, None)

  command = args.command[0]
  command_info = command_dict.get(command)
  if not command_info:
    Log.print_warning(
        "Can't find main class of '%s', suppose it's a class name" % command)
    main_class = command
  else:
    main_class = command_info[0]
  return (main_class, args.command[1:])

def print_shell_help_info(command_dict):
  '''
  Print the help information for the specified shell commands.
  '''
  help_info="help      \tprint this help information"
  for key, value in command_dict.iteritems():
    help_info += "\n%-10s\t%s" % (key, value[1])
  print help_info

def write_file(file_name, content):
  '''
  Write the specified content to the specified file.
  '''
  file = open(file_name, "wb")
  file.write(content)
  file.close()

def make_package_dir(args, artifact, cluster):
  '''
  Make the local package directories.
  '''
  cmd = ["mkdir", "-p", "%s/%s/" % (args.package_root, args.cluster)]
  subprocess.check_call(cmd)

  package_path = get_local_package_path(artifact, cluster.version)
  if not os.path.exists(package_path):
    package_path = make_package_download_dir(args, artifact, cluster) + ".tar.gz"

  cmd = ["tar", "-zxf", package_path, "-C", "%s/%s/" % (
      args.package_root, args.cluster)]
  subprocess.check_call(cmd)

def pack_package(args, artifact, version):
  '''
  Pack the package with generated configuration files into a tarball.
  '''
  cmd = ["tar", "-C", "%s/%s" % (args.package_root, args.cluster),
    "-zchf", "%s/%s/%s-%s-%d.tar.gz" % (args.package_root,
        args.cluster, artifact, version, time.time()),
    "./%s-%s" % (artifact, version)]
  subprocess.check_call(cmd)

def append_to_file(file, content):
  '''
  Append specified content to the specified file.
  '''
  fp = open(file, "a")
  fp.write(content)
  fp.close()

def confirm_rolling_update(host_id, instance_id, wait_time):
  '''
  Let the user confirm the rolling update action interactively.
  '''
  while True:
    if wait_time > 0:
      Log.print_info("Waiting %d seconds before updating next task..."
          % wait_time)
      time.sleep(wait_time)

    while True:
      input = raw_input("Ready to update instance %d on host %d? (y/n) " % (
        get_real_instance_id(instance_id), host_id))
      if check_input(input):
        return True
  return False

def get_zk_address(cluster):
  '''
  Get the zookeeper name address according to the cluster name.
  '''
  return "bj%s-zk-%s.hadoop.srv" % (cluster[0:2], cluster[2:])

def generate_random_confirm_token():
  '''
  Generate a random 8 bytes token used to do confirm
  '''
  return str(uuid.uuid4())[0:8]

def add_task_to_map(task_map, host_id, instance_id):
  if host_id in task_map.keys():
    if instance_id not in task_map[host_id]:
      task_map[host_id].append(instance_id)
  else:
    task_map[host_id] = [instance_id]

def parse_task(args, hosts):
  task_map = {}
  for id in args.task:
    task_id = int(id)
    host_id, instance_id = service_config.parse_task_number(task_id, hosts)
    add_task_to_map(task_map, host_id, instance_id)
  return task_map

def get_task_by_hostname(hosts, hostnames):
  task_map = {}
  for hostname in hostnames:
    host_ip = socket.gethostbyname(hostname)
    found_task = False
    for host_id, host in hosts.iteritems():
      if host.ip == host_ip:
        for instance_id in range(host.instance_num):
          add_task_to_map(task_map, host_id, instance_id)
        found_task = True
        break
    # raise a ValueError if can't find valid task
    if found_task == False:
      raise ValueError(hostname + ' is not a valid host of cluster, please check your config')
  return task_map

def parse_args_host_and_task(args, hosts):
  # the format of task_map is:
  # { host_1 : [instance_1,instance_2...], host_2 : [instance_1,instance_2...] }
  task_map = {}
  if args.host is not None:
    task_map.update(get_task_by_hostname(hosts, args.host))
  elif args.task is not None:
    task_map.update(parse_task(args, hosts))
  return task_map

def is_multiple_instances(host_id, hosts):
  # return False if deploy only one instance on the host
  return hosts[host_id].instance_num > 1

def schedule_task_for_threads(args, hosts, job_name, command, cleanup_token='',
  is_wait=False):
  '''
  Schedule the tasks according to the number of threads and return the task list.
  The task list contains the parameter lists of function called by threads.

  @param  args          the args
  @param  hosts         the hosts of specific job
  @param  job_name      the job name
  @param  command       the deploy command: [bootstrap, start, stop, cleanup]
  @param  cleanup_token the cleanup token
  @param  is_wait       the flag whether to wait for stopping when starting a process
  @return list          the task list for threads
  '''
  args.task_map = parse_args_host_and_task(args, hosts)
  first = True

  thread_num = 1
  if job_name in PARALLEL_DEPLOY_JOBS and args.thread_num > 0:
    thread_num = args.thread_num

  task_list = range(thread_num)
  for index in range(thread_num):
    task_list[index] = []

  for host_id in args.task_map.keys() or hosts.keys():
    for instance_id in args.task_map.get(host_id) or range(
      hosts[host_id].instance_num):

      instance_id = -1 if not \
        is_multiple_instances(host_id, hosts) else instance_id

      if command == 'bootstrap' or command == 'cleanup':
        func_args = (args, hosts[host_id].ip, job_name, host_id,
          instance_id, cleanup_token, first)
      elif command == 'start':
        func_args = (args, hosts[host_id].ip, job_name, host_id,
          instance_id, is_wait)
      elif command == 'stop' or command == 'show':
        func_args = (args, hosts[host_id].ip, job_name, instance_id)

      task_list[host_id % thread_num].append(func_args)
      first = False

  return task_list

if __name__ == '__main__':
  test()
