import socket
import xmlrpclib

class SupervisorClient:
  '''
  The supervisor client.
  '''
  def __init__(self, host, port, user, passwd, service, cluster, job, instance_id):
    self.proxy = xmlrpclib.ServerProxy('http://%s:%s@%s:%d' % (
      user, passwd, host, port))
    self.service = service
    self.cluster = cluster
    self.job = job
    self.instance_id = instance_id

  def get_available_data_dirs(self):
    '''
    Get the available data directories of the remote server.
    '''
    # The `if` statement block is intended to be compatible with the old supervisor.
    # After all supervisors upgrading to the latest version,
    # the `if` statement block will be deleted.
    # The following similar block will not say more.
    if self.instance_id == -1:
      return self.proxy.deployment.get_available_data_dirs(self.service,
        self.cluster, self.job)
    else:
      return self.proxy.deployment.get_available_data_dirs(self.service,
        self.cluster, self.job, self.instance_id)

  def get_data_dirs(self):
    '''
    Get the currently used data directories of this job.
    '''
    if self.instance_id == -1:
      return self.proxy.deployment.get_data_dirs(self.service,
        self.cluster, self.job)
    else:
      return self.proxy.deployment.get_data_dirs(self.service,
        self.cluster, self.job, self.instance_id)

  def get_log_dir(self):
    '''
    Get the log directory of this job.
    '''
    if self.instance_id == -1:
      return self.proxy.deployment.get_log_dir(self.service,
        self.cluster, self.job)
    else:
      return self.proxy.deployment.get_log_dir(self.service,
        self.cluster, self.job, self.instance_id)

  def get_cleanup_token(self):
    '''
    Get the cleanup token of this job.
    '''
    if self.instance_id == -1:
      return self.proxy.deployment.get_cleanup_token(self.service,
        self.cluster, self.job)
    else:
      return self.proxy.deployment.get_cleanup_token(self.service,
        self.cluster, self.job, self.instance_id)

  def get_run_dir(self):
    '''
    Get the running directory of this job.
    '''
    if self.instance_id == -1:
      return self.proxy.deployment.get_run_dir(self.service,
        self.cluster, self.job)
    else:
      return self.proxy.deployment.get_run_dir(self.service,
        self.cluster, self.job, self.instance_id)

  def get_package_dir(self):
    '''
    Get the package directory of this job.
    '''
    if self.instance_id == -1:
      return self.proxy.deployment.get_package_dir(self.service,
        self.cluster, self.job)
    else:
      return self.proxy.deployment.get_package_dir(self.service,
        self.cluster, self.job, self.instance_id)

  # The reture value of get_package_dir() is the symbol link path of
  # the package dir, the return value of get_real_package_dir() is
  # the result of os.readlink(get_package_dir())
  def get_real_package_dir(self):
    if self.instance_id == -1:
      return self.proxy.deployment.get_real_package_dir(
        self.service, self.cluster, self.job)
    else:
      return self.proxy.deployment.get_real_package_dir(
        self.service, self.cluster, self.job, self.instance_id)

  def get_current_package_dir(self):
    return self.proxy.deployment.get_current_package_dir(self.service, self.cluster)

  def bootstrap(self, artifact, force_update=False, package_name='',
      revision='', timestamp='', cleanup_token='', bootstrap_script='',
      data_dir_indexes='0', **config_files):
    '''
    Bootstrap the job.
    '''
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
      if self.instance_id == -1:
        message = self.proxy.deployment.bootstrap(self.service, self.cluster,
          self.job, config_dict)
      else:
        message = self.proxy.deployment.bootstrap(self.service, self.cluster,
          self.job, config_dict, self.instance_id)
    except (xmlrpclib.Error, socket.error), e:
      raise e
    return message

  def start(self, artifact, force_update=False, package_name='', revision='',
      timestamp='', http_url='', start_script='', **config_files):
    '''
    Start the job.
    '''
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
      if self.instance_id == -1:
        message = self.proxy.deployment.start(self.service, self.cluster,
          self.job, config_dict)
      else:
        message = self.proxy.deployment.start(self.service, self.cluster,
          self.job, config_dict, self.instance_id)
    except (xmlrpclib.Error, socket.error), e:
      message = str(e)
    return message

  def stop(self):
    '''
    Stop the job.
    '''
    try:
      if self.instance_id == -1:
        message = self.proxy.deployment.stop(self.service, self.cluster,
          self.job, dict())
      else:
        message = self.proxy.deployment.stop(self.service, self.cluster,
          self.job, dict(), self.instance_id)
    except (xmlrpclib.Error, socket.error), e:
      message = str(e)
    return message

  def show(self):
    '''
    Show the running status the job.
    '''
    try:
      if self.instance_id == -1:
        message = self.proxy.deployment.show(self.service, self.cluster,
          self.job, dict())
      else:
        message = self.proxy.deployment.show(self.service, self.cluster,
          self.job, dict(), self.instance_id)
    except (xmlrpclib.Error, socket.error), e:
      message = str(e)
    return message

  def restart(self, start_script, **config_files):
    '''
    Restart the job.
    '''
    if self.stop() == 'OK':
      return self.start(start_script, **config_files)
    else:
      real_instance_id = self.instance_id
      real_instance_id = 0 if (real_instance_id == -1) else real_instance_id
      return 'Stop %s-%s-%s%s failed' % (self.service, self.cluster, self.job, real_instance_id)

  def cleanup(self, cleanup_token, cleanup_script):
    '''
    Cleanup the job's data and log directories.
    '''
    try:
      config_dict = {
        'cleanup_token': cleanup_token,
        'cleanup.sh': cleanup_script,
      }
      if self.instance_id == -1:
        message = self.proxy.deployment.cleanup(self.service, self.cluster,
          self.job, config_dict)
      else:
        message = self.proxy.deployment.cleanup(self.service, self.cluster,
          self.job, config_dict, self.instance_id)
    except (xmlrpclib.Error, socket.error), e:
      message = str(e)
    return message

if __name__ == '__main__':
  test()
