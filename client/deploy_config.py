import ConfigParser
import os

from log import Log

DEPLOY_CONFIG = "../deploy.cfg"

class DeployConfig:
  '''
  The deploy config class.
  '''
  def __init__(self, file_name):
    self.config_file = os.path.abspath(file_name)
    self.config_parser = ConfigParser.SafeConfigParser()
    self.config_parser.optionxform = str
    self.config_parser.read([self.config_file])

  def get_supervisor_config(self):
    '''
    Get the supervisor config items from the deploy config file.
    '''
    config = {
      'server_port': self.config_parser.getint('supervisor', 'server_port'),
      'user': self.config_parser.get('supervisor', 'user'),
      'password': self.config_parser.get('supervisor', 'password'),
    }
    return config

  def get_tank_config(self):
    '''
    Get the tank config items from the deploy config file.
    '''
    config = {
      'server_host': self.config_parser.get('tank', 'server_host'),
      'server_port': self.config_parser.getint('tank', 'server_port'),
    }
    return config

  def get_config_dir(self):
    '''
    Get the service config file's root directory.
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'config_dir'))

  def get_zookeeper_root(self):
    '''
    Get the local zookeeper root directory.
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'zookeeper_root'))

  def get_zookeeper_package_dir(self):
    '''
    Get the local zookeeper tarball directory.
    '''
    return '%s/build' % self.get_zookeeper_root()

  def get_hadoop_root(self):
    '''
    Get the local hadoop root directory.
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'hadoop_root'))

  def get_hadoop_package_dir(self):
    '''
    Get the local hadoop tarball directory.
    '''
    return '%s/hadoop-dist/target' % self.get_hadoop_root()

  def get_hbase_root(self):
    '''
    Get the local hbase root directory.
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'hbase_root'))

  def get_hbase_package_dir(self):
    '''
    Get the local hbase tarball directory.
    '''
    return '%s/target' % self.get_hbase_root()

  def get_impala_root(self):
    '''
    Get the local impala root directory
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'impala_root'))

  def get_imapala_package_dir(self):
    '''
    Get the local impala tarball directory
    '''
    return '%s/release' % self.get_impala_root()

  def get_kafka_root(self):
    '''
    Get the local kafka root directory
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'kafka_root'))

  def get_kafka_package_dir(self):
    '''
    Get the local kafka tarball directory
    '''
    return '%s/release' % self.get_kafka_root()

  def get_storm_root(self):
    '''
    Get the local storm root directory
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'storm_root'))

  def get_storm_package_dir(self):
    '''
    Get the local storm tarball directory
    '''
    return '%s/release' % self.get_storm_root()

  def get_galaxy_root(self):
    '''
    Get the local galaxy root directory
    '''
    return self._get_real_path(self.config_parser.get(
          'default', 'galaxy_root'))

  def get_galaxy_package_dir(self):
    '''
    Get the local galaxy dist tarball directory
    '''
    return '%s/galaxy-dist/target' % self.get_galaxy_root()

  def get_package_download_root(self):
    '''
    Get the local packages download root directory
    '''
    return "%s/packages" % self._get_real_path(
      self.config_parser.get('default', 'minos_home'))

  def get_admin_list(self):
    '''
    Get the administrators list.
    '''
    return self.config_parser.get('default', 'admin_list').split(',')

  def _get_deploy_root(self):
    return os.path.dirname(self.config_file)

  def _get_real_path(self, path):
    if path.startswith('/'):
      return path
    elif path.startswith('~'):
      return os.path.expanduser(path)
    else:
      return os.path.abspath('%s/%s' % (
        self._get_deploy_root(), path))


def get_deploy_config():
  '''
  A factory method to construct the deploy config object.
  '''
  config_file = os.getenv('MINOS_CONFIG_FILE')
  if config_file:
    if not config_file.startswith('/'):
      config_file = '%s/%s' % (os.path.dirname(__file__), config_file)
  else:
    config_file = '%s/%s' % (os.path.dirname(__file__), DEPLOY_CONFIG)

  if os.path.exists(config_file):
    return DeployConfig(config_file)

  Log.print_critical('Cannot find the config file: deploy.cfg, you should'
      ' specify it by defining the environment variable MINOS_CONFIG_FILE'
      ', or just put the file under the directory: %s' % os.path.dirname(
        os.path.abspath('%s/%s' % (os.path.dirname(__file__), DEPLOY_CONFIG))))

