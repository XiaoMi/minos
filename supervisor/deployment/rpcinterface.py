#!/usr/bin/env python
#
# Copyright (c) 2012, Xiaomi.com.
# Author:  Wu Zesheng <wuzesheng@xiaomi.com>

import ConfigParser
import cStringIO
import subprocess
import os
import time
import urllib2

from exceptions import RuntimeError
from supervisor.datatypes import DEFAULT_EXPECTED_EXIT_CODE
from supervisor.http import NOT_DONE_YET
from supervisor.options import ClientOptions
from supervisor.rpcinterface import SupervisorNamespaceRPCInterface
from supervisor.states import STOPPED_STATES
from supervisor.supervisorctl import Controller
from supervisor.xmlrpc import Faults
from supervisor.xmlrpc import RPCError

DEFAULT_PACKAGE_ROOT = '/home/work/packages'
DEFAULT_APP_ROOT     = '/home/work/app'
DEFAULT_LOG_ROOT     = '/home/work/log'
DEFAULT_DATA_DIRS    = '/home/work/data'

CONFIG_PATH = 'conf'
JOB_RUN_CONFIG = 'run.cfg'

SUCCESS_STATUS = 'OK'

class DeploymentRPCInterface:
  def __init__(self, supervisord, **config):
    self.supervisord = supervisord
    self.global_config = config
    self.supervisor_rpcinterface = SupervisorNamespaceRPCInterface(supervisord)
    self.package_server = config.get('package_server')
    self.download_package_uri = config.get('download_package_uri')
    self.get_latest_package_info_uri = config.get('get_latest_package_info_uri')

  def get_run_dir(self, service, cluster, job, instance_id=-1):
    '''
    Get the run directory of the specified job

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return string     the job's run root directory
    '''
    app_root = self.global_config.get('app_root', DEFAULT_APP_ROOT)
    if instance_id == -1:
      return '%s/%s/%s/%s' % (app_root, service, cluster, job)
    else:
      return '%s/%s/%s/%s/%s' % (app_root, service, cluster, job, instance_id)

  def get_log_dir(self, service, cluster, job, instance_id=-1):
    '''
    Get the log directory of the specified job

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return string     the job's log root directory
    '''
    log_root = self.global_config.get('log_root', DEFAULT_LOG_ROOT)
    if instance_id == -1:
      return '%s/%s/%s/%s' % (log_root, service, cluster, job)
    else:
      return '%s/%s/%s/%s/%s' % (log_root, service, cluster, job, instance_id)

  def get_stdout_dir(self, service, cluster, job, instance_id=-1):
    '''
    Get the stdout directory of the specified job

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return string     the job's log root directory
    '''
    run_dir = self.get_run_dir(service, cluster, job, instance_id)
    return '%s/stdout' % run_dir

  def get_available_data_dirs(self, service, cluster, job, instance_id=-1):
    '''
    Get all the available data directories that the specified job may use

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return list       all the available data root directories
    '''
    data_dirs = self.global_config.get('data_dirs', DEFAULT_DATA_DIRS)
    if instance_id == -1:
      return ['%s/%s/%s/%s' % (data_dir, service, cluster, job)
        for data_dir in data_dirs.split(',')
      ]
    else:
      return ['%s/%s/%s/%s/%s' % (data_dir, service, cluster, job, instance_id)
        for data_dir in data_dirs.split(',')
      ]

  def get_data_dirs(self, service, cluster, job, instance_id=-1):
    '''
    Get all the data directories of the specified job

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return list       the job's data root directories
    '''
    file_name = '%s/%s' % (self.get_run_dir(service, cluster, job, instance_id),
        JOB_RUN_CONFIG)
    if not os.path.exists(file_name):
      return 'You should bootstrapped the job first'

    data_dirs = self.get_available_data_dirs(service, cluster, job, instance_id)
    run_config = ConfigParser.SafeConfigParser()
    run_config.read([file_name])
    data_dir_indexes = run_config.get('run_info', 'data_dir_indexes')
    job_data_dirs = []
    for i in data_dir_indexes.split(','):
      job_data_dirs.append(data_dirs[int(i)])
    return job_data_dirs

  def get_package_dir(self, service, cluster, job, instance_id=-1):
    '''
    Get the current package directory of the specified job

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return string     the job's package root directory(symbol link)
    '''
    return '%s/package' % self.get_run_dir(service, cluster, job, instance_id)

  def get_real_package_dir(self, service, cluster, job, instance_id=-1):
    '''
    Get the current package directory real path of the specified job

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return string     the job's package root directory(real path)
    '''
    return os.readlink(self.get_package_dir(service, cluster, job, instance_id))

  def get_current_package_dir(self, service, cluster):
    '''
    Get the currently used package directory of the specified service

    @param service   the service name
    @param cluster   the cluster name
    @return string   the currently used package directory
    '''
    package_root = self.global_config.get('package_root')
    return '%s/%s/%s/current' % (package_root, service, cluster)

  def get_cleanup_token(self, service, cluster, job, instance_id=-1):
    '''
    Get the token used to do cleanuping

    @param service     the server name
    @param cluster     the cluster name
    @param job         the job name
    @param instance_id the instance id
    @return string     the job's cleanup token
    '''
    file_name = '%s/%s' % (self.get_run_dir(service, cluster, job, instance_id),
        JOB_RUN_CONFIG)
    if not os.path.exists(file_name):
      return 'You should bootstrap the job first'

    run_config = ConfigParser.SafeConfigParser()
    run_config.read([file_name])
    return run_config.get('run_info', 'cleanup_token')

  def bootstrap(self, service, cluster, job, config_dict, instance_id=-1):
    '''
    Bootstrap the specified job

    @param service      the server name
    @param cluster      the cluster name
    @param job          the job name
    @param instance_id  the instance id
    @param config_dict  the config information dictionary
    @return string      'OK' on success, otherwise, the error message

    Note: config_dict must contain the following item:
      1. artifact
      2. bootstrap.sh
      3. if any config files are needed, just put it in 'config_files' item

    config_dict can also contain the following optional items:
      1. cleanup_token: if this token is specified, user should supply
         the token to do cleanup
      2. package_name: package_name, revision, timestamp should be specified
         simultaneously, otherwise will be ignored
      3. revision
      4. timestamp
      5. data_dir_indexes: if this is not specified, the first data_dir is
         used by default
      6. force_update
    This is an example:
      config_dict = {
        'artifact': 'hadoop',
        'bootstrap.sh': $bootstrap_file_content,
        'config_files': {
          'core-site.xml': $core_site_xml_content,
          ...
        },
      }
    '''
    return self._do_bootstrap(service, cluster, job, instance_id, **config_dict)

  def start(self, service, cluster, job, config_dict, instance_id=-1):
    '''
    Start the specified job

    @param service      the server name
    @param cluster      the cluster name
    @param job          the job name
    @param instance_id  the instance id
    @param config_dict  the config information dictionary
    @return string      'OK' on success, otherwise, the error message

    Note: config_dict must contain the following item:
      1. start.sh
      2. artifact
      3. if any config files are needed, just put it in 'config_files' item

    config_dict can also contain the following optional items:
      1. http_url: the server's http service url
      2. package_name: package_name, revision, timestamp should be specified
         simultaneously, otherwise will be ignored
      3. revision
      4. timestamp
      5. force_update
    This is an example:
      config_dict = {
        'start.sh': $start_file_content,
        'artifact': hadoop,
        'config_files': {
          'core-site.xml': $core_site_xml_content,
          ...
        },
        'http_url': 'http://10.235.3.67:11201',
      }
    '''
    return self._do_start(service, cluster, job, instance_id, **config_dict)

  def stop(self, service, cluster, job, config_dict, instance_id=-1):
    '''
    Stop the specified job

    @param service      the server name
    @param cluster      the cluster name
    @param job          the job name
    @param instance_id  the instance id
    @param config_dict  the config information dictionary
    @return string      'OK' on success, otherwise, the error message

    Note: config_dict is not used currently, reserved for extendibility
    '''
    return self._do_stop(service, cluster, job, instance_id, **config_dict)

  def cleanup(self, service, cluster, job, config_dict, instance_id=-1):
    '''
    Cleanup the specified job's data/log directories

    @param service      the server name
    @param cluster      the cluster name
    @param job          the job name
    @param instance_id  the instance id
    @param config_dict  the config information dictionary
    @return string      'OK' on success, otherwise, the error message

    Note: config_dict may contain the following item:
      1. cleanup_token: [optional] token used to do verification
      2. cleanup.sh: [optional] script used to do cleanuping
    This is an example:
      config_dict = {
        'cleanup_token': '550e8400-e29b-41d4-a716-446655440000',
        'cleanup.sh': $cleanup_script,
      }
    '''
    return self._do_cleanup(service, cluster, job, instance_id, **config_dict)

  def show(self, service, cluster, job, config_dict, instance_id=-1):
    '''
    Get the specified job's current status
    @param service      the server name
    @param cluster      the cluster name
    @param job          the job name
    @param instance_id  the instance id
    @param config_dict  the config information dictionary
    @return string      the process status
    Possible values of  process status:
      RUNNING STARTING  BACKOFF STOPPING EXITED FATAL UNKNOWN

    Note: config_dict is not used currently, reserved for extendibility
    '''
    return self._do_show(service, cluster, job, instance_id, **config_dict)

  def read_file(self, file_path):
    '''
    Read the file with the given file path on server
    @param file_path      the name of file to read
    '''
    with open(file_path) as fi:
      return fi.read()

  def write_text_to_file(self, file_path, content):
    '''
    Write content to the file with the given file path on server
    @param file_path      the name of file to write
    @param content        the content to write
    '''
    with open(file_path, 'w') as fi:
      fi.write(content)
    return 'OK'

  def append_text_to_file(self, file_path, content):
    '''
    Append content to the file with the given file path on server
    @param file_path      the name of file to append
    @param content        the content to append
    '''
    with open(file_path, 'a') as fi:
      fi.write(content)
    return 'OK'

  def _get_package_uri(self, artifact, revision, timestamp, package_name):
    return '%s/%s/%s/%s-%s/%s' % (self.package_server,
        self.download_package_uri, artifact,
        revision, timestamp, package_name)

  def _get_query_latest_package_info_uri(self, artifact):
    return '%s/%s/?artifact=%s' % (self.package_server,
        self.get_latest_package_info_uri, artifact)

  def _downlowd_package(self, uri, dest_file):
    data_file = urllib2.urlopen(uri, None, 30)
    if not os.path.exists(os.path.dirname(dest_file)):
      os.makedirs(os.path.dirname(dest_file))
    fp = open(dest_file, 'wb')
    fp.write(data_file.read())
    fp.close()
    data_file.close()

  def _write_file(self, file_path, file_content):
    fp = open(file_path, 'wb')
    fp.write(file_content)
    fp.close()

  def _write_config_files(self, run_dir, **config_dict):
    for file_name, content in config_dict.iteritems():
      file_path = '%s/%s' % (run_dir, file_name)
      if os.path.exists(file_path):
        os.remove(file_path)
      self._write_file(file_path, content)

  def _get_process_name(self, service, cluster, job, instance_id):
    if instance_id == -1:
      return '%s--%s--%s' % (service, cluster, job)
    else:
      return '%s--%s--%s%d' % (service, cluster, job, instance_id)

  def _cleanup_dir(self, path):
    cmd = 'rm -rf %s/*' % path
    subprocess.check_call(cmd, shell=True)

  def _check_dir_empty(self, path):
    if not os.path.exists(path):
      return True

    lists = os.listdir(path)
    return len(lists) == 0

  def _check_bootstrapped(self, service, cluster, job, instance_id):
    run_dir = self.get_run_dir(service, cluster, job, instance_id)
    return os.path.exists('%s/%s' % (run_dir, JOB_RUN_CONFIG))

  def _get_latest_package_info(self, artifact):
    uri = self._get_query_latest_package_info_uri(artifact)
    info_fp = urllib2.urlopen(uri, None, 30)
    info = info_fp.read()

    if info and info.startswith('{'):
      info_dict = eval(info)
      info_fp.close()
      return info_dict
    else:
      info_fp.close()
      return None

  def _make_package_dir(self, artifact, service, cluster, job, instance_id,
    revision, timestamp, package_name):
    # Check if the tarball is already downloaded, if not, download it
    package_path = '%s/%s/%s/%s-%s/%s' % (self.global_config.get('package_root'),
        service, cluster, revision, timestamp, package_name)
    if not os.path.exists(package_path):
      self._downlowd_package(
          self._get_package_uri(artifact, revision, timestamp, package_name),
          package_path)

    # Unpack the tarball
    package_dir = package_path[0: len(package_path) - len('.tar.gz')]
    if os.path.exists(package_dir):
      cmd = ['rm', '-rf', package_dir]
      subprocess.check_call(cmd)
    cmd = ['tar', '-zxf', package_path, '-C', os.path.dirname(package_dir)]
    subprocess.check_call(cmd)

    # Link the package dir to the 'current'
    current_dir = self.get_current_package_dir(service, cluster)
    if os.path.lexists(current_dir):
      os.unlink(current_dir)
    os.symlink(package_dir, current_dir)

    # Link the package dir to the run dir
    symbol_package_dir = self.get_package_dir(service, cluster, job, instance_id)
    if os.path.lexists(symbol_package_dir):
      os.unlink(symbol_package_dir)
    os.symlink(package_dir, symbol_package_dir)
    return package_dir

  def _update_run_cfg(self, file_path, section, key, value):
    run_config = ConfigParser.SafeConfigParser()
    run_config.read([file_path])
    run_config.set(section, key, value)
    fp = open(file_path, 'w')
    run_config.write(fp)
    fp.close()

  def _prepare_run_env(self, service, cluster, job, instance_id, **config_dict):
    artifact = config_dict.get('artifact')
    if not artifact:
      return 'Invalid config_dict: can\'t find artifact'

    # Create run dirs
    run_dir = self.get_run_dir(service, cluster, job, instance_id)
    if not os.path.exists(run_dir):
      os.makedirs(run_dir)

    # Create stdout dir
    stdout_dir = self.get_stdout_dir(service, cluster, job, instance_id)
    if not os.path.exists(stdout_dir):
      os.makedirs(stdout_dir)

    # Create and link log dir to the run dir
    log_dir = self.get_log_dir(service, cluster, job, instance_id)
    if os.path.exists(log_dir):
      if not self._check_dir_empty(log_dir):
        return 'The log dir %s is not empty, please do cleanup first' % log_dir
    else:
      os.makedirs(log_dir)
    symbol_log_dir = '%s/log' % run_dir
    if not os.path.exists(symbol_log_dir):
      os.symlink(log_dir, symbol_log_dir)

    # Create and link data dirs to the run dir
    data_dirs = self.global_config.get('data_dirs', DEFAULT_DATA_DIRS).split(',')
    data_dir_indexes  = (config_dict.get('data_dir_indexes') or '0')
    for i in data_dir_indexes.split(','):
      if instance_id == -1:
        data_dir = '%s/%s/%s/%s' % (data_dirs[int(i)], service, cluster, job)
      else:
        data_dir = '%s/%s/%s/%s/%s' % (data_dirs[int(i)], service, cluster, job, instance_id)
      if os.path.exists(data_dir):
        if not self._check_dir_empty(data_dir):
          return 'The data dir %s is not empty, please do cleanup first' % data_dir
      else:
        try:
          os.makedirs(data_dir)
        except OSError, e:
          return "Error: %s" % str(e)
      symbol_data_dir = '%s/%s' % (run_dir, os.path.basename(data_dirs[int(i)]))
      if not os.path.exists(symbol_data_dir):
        os.symlink(data_dir, symbol_data_dir)

    # Check the package information
    force_update = config_dict.get('force_update', False)
    if force_update:
      package_info = self._get_latest_package_info(artifact)
      if package_info:
        package_name = package_info.get('package_name')
        revision = package_info.get('revision')
        timestamp = package_info.get('timestamp')
    else:
      package_name = config_dict.get('package_name')
      revision = config_dict.get('revision')
      timestamp = config_dict.get('timestamp')
      if not (package_name and revision and timestamp):
        package_info = self._get_latest_package_info(artifact)
        if package_info:
          package_name = package_info.get('package_name')
          revision = package_info.get('revision')
          timestamp = package_info.get('timestamp')
    if not (package_name and revision and timestamp):
      return 'No package found on package server of %s' % artifact

    # Write the job's run.cfg
    try:
      package_dir = self._make_package_dir(artifact, service, cluster, job,
          instance_id, revision, timestamp, package_name)
    except urllib2.URLError, e:
      return "%s. There may be an error about your package information." % str(e)
    except subprocess.CalledProcessError, e:
      return "Error: %s" % str(e)
    cleanup_token = config_dict.get('cleanup_token', str())
    run_config = ConfigParser.SafeConfigParser()
    run_config.add_section('run_info')
    run_config.set('run_info', 'cleanup_token', cleanup_token)
    run_config.set('run_info', 'data_dir_indexes', data_dir_indexes)
    run_config.set('run_info', 'run_dir', run_dir)
    run_config.set('run_info', 'log_dir', log_dir)
    run_config.set('run_info', 'package_dir', package_dir)
    fp = open('%s/%s' % (run_dir, JOB_RUN_CONFIG), 'w')
    run_config.write(fp)
    fp.close()
    return SUCCESS_STATUS

  def _do_bootstrap(self, service, cluster, job, instance_id, **config_dict):
    # prepare run dir
    message = self._prepare_run_env(service, cluster, job, instance_id, **config_dict)
    if message != SUCCESS_STATUS:
      return message

    # Write other config files to local disk
    config_files = config_dict.get('config_files')
    service_root = self.get_run_dir(service, cluster, job, instance_id)
    if config_files:
      self._write_config_files(service_root, **config_files)

    # Do bootstraping
    bootstrap_sh = config_dict.get('bootstrap.sh')
    if bootstrap_sh:
      self._write_file('%s/bootstrap.sh' % service_root, bootstrap_sh)
      cmd = ['/bin/bash', '%s/bootstrap.sh' % service_root]
      subprocess.call(cmd)
    return SUCCESS_STATUS

  def _do_start(self, service, cluster, job, instance_id, **config_dict):
    artifact = config_dict.get('artifact')
    if not artifact:
      return 'Inval config_dict: can\'t find artifact'

    if not self._check_bootstrapped(service, cluster, job, instance_id):
      return "You should bootstrap the job first"

    # Check if need update the package
    force_update = config_dict.get('force_update', False)
    if force_update:
      package_info = self._get_latest_package_info(artifact)
      if package_info:
        package_name = package_info.get('package_name')
        revision = package_info.get('revision')
        timestamp = package_info.get('timestamp')
    else:
      package_name = config_dict.get('package_name')
      revision = config_dict.get('revision')
      timestamp = config_dict.get('timestamp')

    if (package_name and revision and timestamp):
      package_path = '%s/%s/%s-%s/%s' % (
          self.global_config.get('package_root'),
          artifact, revision, timestamp, package_name)
      try:
        if not os.path.exists(package_path):
          self._downlowd_package(
              self._get_package_uri(artifact, revision, timestamp, package_name),
              package_path)
        package_dir = self._make_package_dir(artifact, service, cluster, job,
            instance_id, revision, timestamp, package_name)
      except urllib2.URLError, e:
        return "%s. There may be an error about your package information." % str(e)
      except subprocess.CalledProcessError, e:
        return "Error: %s" % str(e)
      run_cfg = '%s/%s' % (self.get_run_dir(service, cluster, job, instance_id),
          JOB_RUN_CONFIG)
      self._update_run_cfg(run_cfg, 'run_info', 'package_dir', package_dir)

    # Write the start script to local disk
    start_sh = config_dict.get('start.sh')
    service_root = self.get_run_dir(service, cluster, job, instance_id)
    if not start_sh and not os.path.exists('%s/start.sh' % service_root):
      return 'No start script found'
    elif start_sh:
      self._write_file('%s/start.sh' % service_root, start_sh)

    # Write other config files to local disk
    config_files = config_dict.get('config_files')
    if config_files:
      self._write_config_files(service_root, **config_files)

    # Write supervisor config
    http_url = config_dict.get('http_url', '')
    process_name = self._get_process_name(service, cluster, job, instance_id)
    job_config = ConfigParser.SafeConfigParser()
    section = 'program:%s' % process_name
    job_config.add_section(section)
    job_config.set(section, 'command', '/bin/bash %s/start.sh' % service_root)
    job_config.set(section, 'process_name', process_name)
    job_config.set(section, 'directory', service_root)
    job_config.set(section, 'http_url', http_url)
    # Process will be unconditionally restarted when it exits, without regard
    # to its exit code
    job_config.set(section, 'autorestart', 'true')
    job_config.set(section, 'exitcodes', str(DEFAULT_EXPECTED_EXIT_CODE))
    # Process will NOT be automatically started when supervisor restart.
    job_config.set(section, 'autostart', 'false')
    fp = open('%s/%s/%s.cfg' % (os.getcwd(), CONFIG_PATH, process_name), 'wb')
    job_config.write(fp)
    fp.close()

    # Start the job
    self.supervisor_rpcinterface.reloadConfig()
    try:
      self.supervisor_rpcinterface.addProcessGroup(process_name)
    except RPCError, e:
      if e.code != Faults.ALREADY_ADDED:
        raise e
    self.supervisor_rpcinterface.startProcess(process_name)()
    return SUCCESS_STATUS

  def _do_stop(self, service, cluster, job, instance_id, **config_dict):
    process_name = self._get_process_name(service, cluster, job, instance_id)
    self.supervisor_rpcinterface.stopProcess(process_name)()
    return SUCCESS_STATUS

  def _do_cleanup(self, service, cluster, job, instance_id, **config_dict):
    # check cleanup token
    cleanup_token = config_dict.get('cleanup_token')
    if cleanup_token:
      local_token = self.get_cleanup_token(service, cluster, job, instance_id)
      if local_token != cleanup_token:
        return 'Cleanup token is invalid'

    try:
      state = self._do_show(service, cluster, job, instance_id, **config_dict)
      if state == 'RUNNING':
        return 'You should stop the job first'
    except RPCError, e:
      pass

    log_dir = self.get_log_dir(service, cluster, job, instance_id)
    cleanup_script = config_dict.get('cleanup.sh', str())
    if cleanup_script:
      service_root = self.get_run_dir(service, cluster, job, instance_id)
      self._write_file('%s/cleanup.sh' % service_root, cleanup_script)
      cmd = ['/bin/bash', '%s/cleanup.sh' % service_root]
      if subprocess.call(cmd) != 0:
        self._cleanup_dir(log_dir)
        return 'Execute cleanup.sh failed'

    self._cleanup_dir(log_dir)
    data_dirs = self.get_data_dirs(service, cluster, job, instance_id)
    for data_dir in data_dirs:
      self._cleanup_dir(data_dir)

    process_name = self._get_process_name(service, cluster, job, instance_id)
    job_config = '%s/%s/%s.cfg' % (os.getcwd(), CONFIG_PATH, process_name)
    if os.path.exists(job_config):
      os.remove(job_config)
      try:
        self.supervisor_rpcinterface.removeProcessGroup(process_name)
        self.supervisor_rpcinterface.reloadConfig()
      except RPCError, e:
        pass
    return SUCCESS_STATUS

  def _do_show(self, service, cluster, job, instance_id, **config_dict):
    info = self.supervisor_rpcinterface.getProcessInfo(
        self._get_process_name(service, cluster, job, instance_id))
    return info.get('statename')

def check_and_create(path):
  if not os.path.exists(path):
    os.makedirs(path)

def initialize_deployment_env(**config):
  app_root = config.get('app_root', DEFAULT_APP_ROOT)
  check_and_create(app_root)

  log_root = config.get('log_root', DEFAULT_LOG_ROOT)
  check_and_create(app_root)

  package_root = config.get('package_root', DEFAULT_PACKAGE_ROOT)
  check_and_create(package_root)

  data_dirs = config.get('data_dirs', DEFAULT_DATA_DIRS).split(',')
  for data_dir in data_dirs:
    if not os.path.exists(data_dir):
      raise RuntimeError(
          'Data dir %s must created before starting supervisord'
          % data_dir)

def deployment_rpcinterface(supervisord, **config):
  initialize_deployment_env(**config)
  return DeploymentRPCInterface(supervisord, **config)

def test():
  pass

if __name__ == '__main__':
  test()
