#!/usr/bin/env python

import xmlrpclib

# The supervisor client
class SupervisorClient:
  def __init__(self, host, port, user, passwd, service, cluster, job):
    self.proxy = xmlrpclib.ServerProxy('http://%s:%s@%s:%d' % (
      user, passwd, host, port))
    self.service = service
    self.cluster = cluster
    self.job = job

  def get_available_data_dirs(self):
    return self.proxy.deployment.get_available_data_dirs(self.service,
        self.cluster, self.job)

  def get_data_dirs(self):
    return self.proxy.deployment.get_data_dirs(self.service,
        self.cluster, self.job)

  def get_log_dir(self):
    return self.proxy.deployment.get_log_dir(self.service,
        self.cluster, self.job)

  def get_cleanup_token(self):
    return self.proxy.deployment.get_cleanup_token(self.service,
        self.cluster, self.job)

  def get_run_dir(self):
    return self.proxy.deployment.get_run_dir(self.service,
        self.cluster, self.job)

  def get_package_dir(self):
    return self.proxy.deployment.get_package_dir(self.service,
        self.cluster, self.job)

  # The reture value of get_package_dir() is the symbol link path of
  # the package dir, the return value of get_real_package_dir() is
  # the result of os.readlink(get_package_dir())
  def get_real_package_dir(self):
    return self.proxy.deployment.get_real_package_dir(
        self.service, self.cluster, self.job)

  def get_current_package_dir(self):
    return self.proxy.deployment.get_current_package_dir(self.service, self.cluster)

  def bootstrap(self, artifact, force_update=False, package_name='',
      revision='', timestamp='', cleanup_token='', bootstrap_script='',
      data_dir_indexes='0', **config_files):
    try:
      config_dict = {
        'artifact': artifact,
        'force_update': force_update,
        'package_name': package_name,
        'revision': revision,
        'timestamp': timestamp,
        'cleanup_token': cleanup_token,
        'bootstrap.sh': bootstrap_script,
        'data_dir_indexes': data_dir_indexes,
        'config_files': config_files,
      }
      message = self.proxy.deployment.bootstrap(self.service, self.cluster,
          self.job, config_dict)
    except xmlrpclib.Fault, f:
      message = str(f)
    return message

  def start(self, artifact, force_update=False, package_name='', revision='',
      timestamp='', http_url='', start_script='', **config_files):
    try:
      config_dict = {
        'start.sh': start_script,
        'artifact': artifact,
        'config_files': config_files,
        'http_url': http_url,
        'force_update': force_update,
        'package_name': package_name,
        'revision': revision,
        'timestamp': timestamp,
      }
      message = self.proxy.deployment.start(self.service, self.cluster,
          self.job, config_dict)
    except xmlrpclib.Fault, f:
      message = str(f)
    return message

  def stop(self):
    try:
      message = self.proxy.deployment.stop(self.service, self.cluster,
          self.job, dict())
    except xmlrpclib.Fault, f:
      message = str(f)
    return message

  def show(self):
    try:
      message = self.proxy.deployment.show(self.service, self.cluster,
          self.job, dict())
    except xmlrpclib.Fault, f:
      message = str(f)
    return message

  def restart(self, start_script, **config_files):
    if self.stop() == 'OK':
      return self.start(start_script, **config_files)
    else:
      return 'Stop %s-%s-%s failed' % (self.service, self.cluster, self.job)

  def cleanup(self, cleanup_token, cleanup_script):
    try:
      config_dict = {
        'cleanup_token': cleanup_token,
        'cleanup.sh': cleanup_script,
      }
      message = self.proxy.deployment.cleanup(self.service, self.cluster,
          self.job, config_dict)
    except xmlrpclib.Fault, f:
      message = str(f)
    return message

if __name__ == '__main__':
  test()
