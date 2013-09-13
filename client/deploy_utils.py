#!/usr/bin/env python

import ConfigParser
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
import tempfile
import time
import uuid

from datetime import datetime
from supervisor_client import SupervisorClient
from service_config import ServiceConfig
from tank_client import TankClient

SUPERVISOR_SUCCESS = "OK"

STOPPED_STATUS = ["STOPPED", "BACKOFF", "EXITED", "FATAL"]

HADOOP_PROPERTY_PREFIX = "hadoop.property."
HADOOP_CONF_PATH = "/etc/hadoop/conf"

FAKE_SVN_VERSION = "12345"

class Template(string.Template):
  # the orginal delimiter '$' is also commonly used by shell script, so
  # overwrite to '%' here.
  delimiter = '%'


class Log:
  # We have such a agreement on verbosity level:
  # 0: equals to print_info
  # 1: summary of a host level operation (a batch of command)
  # 2: summary of a command
  # 3: details or content of a command
  verbosity = 0

  @staticmethod
  def _print(message):
    print "%s %s" % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), message)

  @staticmethod
  def error_exit(print_stack):
    if not print_stack:
      sys.exit(2)
    else:
      raise RuntimeError("fatal error")

  @staticmethod
  def print_verbose(message, verbosity):
    if verbosity <= Log.verbosity:
      Log.print_info(message)

  @staticmethod
  def print_info(message):
    Log._print(message)

  @staticmethod
  def print_success(message):
    Log._print("\033[0;32m%s\033[0m" % message)

  @staticmethod
  def print_warning(message):
    Log._print("\033[0;33m%s\033[0m" % message)

  @staticmethod
  def print_error(message):
    Log._print("\033[0;31m%s\033[0m" % message)

  @staticmethod
  def print_critical(message):
    Log.print_error(message)
    Log.error_exit(False)

def get_deploy_config():
  return deploy_config.get_deploy_config()

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
  else:
    Log.print_critical("Unknow artifact: %s" % artifact)
  return package_path

def generate_package_revision(root):
  '''
  Get the revision of the package. Currently, only svn revision is
  supported. If the package directory is not a svn working directory,
  a fake revision will be returned.

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
    cmd = ["svn", "info", abs_path]
    env = os.environ
    # Enforce English locale.
    env["LC_ALL"] = "C"
    revision_prefix = "Revision: "
    content = subprocess.check_output(cmd, env=env)
    for line in content.splitlines():
     if line.startswith(revision_prefix):
       return "r%s" % line[len(revision_prefix):]
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
  if service == "hdfs" or service == "yarn":
    return get_deploy_config().get_hadoop_root()
  if service == "hbase":
    return get_deploy_config().get_hbase_root()
  if service == "zookeeper":
    return get_deploy_config().get_zookeeper_root()
  if service == "impala":
    return get_deploy_config().get_impala_root()
  Log.print_critical("Unknow service: %s" % service)

def get_supervisor_client(host, service, cluster, job):
  '''
  A factory method to construct a supervisor client object.

  @param  host    the remote server's host
  @param  service the service name
  @param  cluster the cluster name
  @param  job     the job name
  @return object  the supervisor client object
  '''
  return service_config.get_supervisor_client(host, service, cluster, job)

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
          "this operation can't be processed" % user)
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
  else:
    return false

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
    cleanup_token, cleanup_script=""):
  '''
  Clean up a task of the specified service and job. Note that cleanup
  requires that the task must be stopped, so users should stop the task
  before cleanup.

  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  @param cleanup_token   the token used to verify cleanup
  @param cleanup_script  the user supplied cleanup script
  @param artifact        the artifact name
  '''
  Log.print_info("Cleaning up %s on %s" % (job_name, host))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name)
  message = supervisor_client.cleanup(cleanup_token, cleanup_script)
  if SUPERVISOR_SUCCESS == message:
    Log.print_success("Cleanup %s on %s success" % (job_name, host))
  else:
    Log.print_error("Cleanup %s on %s fail: %s" % (job_name, host, message))

def bootstrap_job(args, artifact, service, service_config, host, job_name,
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
  @param cleanup_token    the token used to verify cleanup
  @param data_dir_indexes the data directory indexes
  @param bootstrap_script the user supplied bootstrap script
  @param config_files     the config files dict
  '''
  Log.print_info("Bootstrapping %s on %s" % (job_name, host))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name)

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
    Log.print_success("Bootstrap %s on %s success" % (job_name, host))
  else:
    Log.print_critical("Bootstrap %s on %s fail: %s" % (job_name,
          host, message))

def start_job(args, artifact, service, service_config, host, job_name,
    start_script, http_url, **config_files):
  '''
  Start the task of specified service and job.

  @param args            the command line arguments object
  @param artifact        the artifact name
  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  @param start_script    the user supplied start script
  @param http_url        the task's http entry url
  @param config_files    the config files dict
  '''
  Log.print_info("Starting %s on %s" % (job_name, host))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name)

  if not args.update_config:
    config_files = dict()

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
    Log.print_success("Start %s on %s success" % (job_name, host))
  else:
    Log.print_error("Start %s on %s fail: %s" % (job_name, host, message))

def stop_job(service, service_config, host, job_name):
  '''
  Stop the task of specified service and job.

  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  '''
  Log.print_info("Stopping %s on %s" % (job_name, host))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name)
  message = supervisor_client.stop()
  if SUPERVISOR_SUCCESS == message:
    Log.print_success("Stop %s on %s success" % (job_name, host))
  else:
    Log.print_error("Stop %s on %s fail: %s" % (job_name, host, message))

def show_job(service, service_config, host, job_name):
  '''
  Show the state the task of specified service and job.

  @param service         the service name
  @param service_config  the service config object
  @param host            the host of the task
  @param job_name        the job name
  '''
  Log.print_info("Showing %s on %s" % (job_name, host))
  supervisor_client = get_supervisor_client(host, service,
      service_config.cluster.name, job_name)
  state = supervisor_client.show()
  if state == 'RUNNING':
    Log.print_success("%s on %s is %s" % (job_name, host, state))
  else:
    Log.print_error("%s on %s is %s" % (job_name, host, state))

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

def check_job_stopped(service, cluster, job, host):
  '''
  Check whether a specified task is already stopped or not.
  '''
  supervisor_client = get_supervisor_client(host,
      service, cluster, job)
  status = supervisor_client.show()
  return status in STOPPED_STATUS

def wait_for_job_stopping(service, cluster, job, host):
  '''
  Wait for a specified job to be stopped.
  '''
  while not check_job_stopped(service, cluster, job, host):
    Log.print_warning("Wait for %s on %s stopping" % (job, host))
    time.sleep(2)

def check_job_started(service, cluster, job, host):
  '''
  Check whether a specified task is already started or not.
  '''
  supervisor_client = get_supervisor_client(host,
      service, cluster, job)
  status = supervisor_client.show()
  return status == 'RUNNING'

def wait_for_job_starting(service, cluster, job, host):
  '''
  Wait for a specified job to be started.
  '''
  # Wait 10 seconds to let supervisord start the task
  time.sleep(10)
  if not check_job_started(service, cluster, job, host):
      Log.print_critical('%s on %s start failed' % (job, host))

def get_hadoop_package_root(version):
  '''
  Get the hadoop package root directory
  '''
  return "%s/hadoop-%s" % (get_deploy_config().get_hadoop_package_dir(), version)

def get_hbase_package_root(version):
  '''
  Get the hbase package root directory
  '''
  return "%s/hbase-%s/hbase-%s" % (get_deploy_config().get_hbase_package_dir(),
      version, version)

def get_zookeeper_package_root(version):
  '''
  Get the zookeeper package root directory
  '''
  return "%s/zookeeper-%s" % (
      get_deploy_config().get_zookeeper_package_dir(), version)

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

def make_package_dir(args, artifact, version):
  '''
  Make the local package directories.
  '''
  cmd = ["mkdir", "-p", "%s/%s/" % (args.package_root, args.cluster)]
  subprocess.check_call(cmd)

  package_path = get_local_package_path(artifact, version)
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

def confirm_rolling_update(id, wait_time):
  '''
  Let the user confirm the rolling update action interactively.
  '''
  while True:
    if wait_time > 0:
      Log.print_info("Waiting %d seconds before updating next task..."
          % wait_time)
      time.sleep(wait_time)

    input = raw_input("Ready to update task %d? (y/n) " % id)
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

def get_task_by_hostname(hosts, hostnames):
  tasks = []
  for hostname in hostnames:
    host_ip = socket.gethostbyname(hostname)
    found_task = False
    for id in hosts.iterkeys():
      if hosts[id] == host_ip:
        tasks.append(id)
        found_task = True
        break
    # return an invalid task id if can't find valid task
    if found_task == False:
      raise ValueError(hostname + ' is not a valid host of cluster, please check your config')
  return tasks

if __name__ == '__main__':
  test()
