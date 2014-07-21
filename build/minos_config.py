import os
import sys

import deploy_config
from log import Log

CONFIG_DIR = deploy_config.get_deploy_config().get_config_dir()

# client
CLIENT_PREREQUISITE_PYTHON_LIBS = [
  ('configobj', 'configobj', '4.7.2'),
  ('yaml', 'PyYAML', '3.11'),
]

# tank
TANK_PREREQUISITE_PYTHON_LIBS = [
  ('django', 'django', '1.5.5'),
]

TANK_DEFAULT_PORT = 8000
TANK_DEFAULT_IP = '0.0.0.0'

# supervisor
SUPERVISOR_PREREQUISITE_PYTHON_LIBS = [
  ('setuptools', 'setuptools', '1.4.0'),
  ('meld3', 'meld3', '0.6.10'),
  ('elementtree', 'elementtree', '1.2.6-20050316'),
  ('pexpect', 'pexpect', '3.0'),
]

SUPERVISOR_DEPLOYMENT_DIRS = [
  'app',
  'data',
  'log',
  'packages',
]

# owl
OWL_PREREQUISITE_PYTHON_LIBS = [
  ('django', 'django', '1.5.5'),
  ('ldap', 'python-ldap', '2.4.13'),
  ('django_auth_ldap', 'django-auth-ldap', '1.1.6'),
  ('twisted', 'twisted', '13.2.0'),
  ('MySQLdb', 'Mysql-python', '1.2.5'),
  ('DBUtils', 'dbutils', '1.1'),
]
OWL_CONFIG_ROOT = os.path.join(CONFIG_DIR, 'owl')
OWL_CONFIG_FILE = os.path.join(OWL_CONFIG_ROOT, 'owl_config.py')
OWL_COLLECTOR_FILE = os.path.join(OWL_CONFIG_ROOT, 'collector.cfg')

OPENTSDB_CONFIG_ROOT = os.path.join(CONFIG_DIR, 'opentsdb')
OPENTSDB_COLLECTOR_CONFIG_FILE = os.path.join(
  OPENTSDB_CONFIG_ROOT, 'metrics_collector_config.py')
OPENTSDB_REPOSITORY = 'git://github.com/OpenTSDB/opentsdb.git'
OPENTSDB_PORT = '4242'

HBASE_VERSION = 'hbase-0.94.14'
HBASE_ROOT = os.path.join(os.getenv("BUILD_DOWNLOAD_ROOT"), HBASE_VERSION)
HBASE_CONFIG_ROOT = os.path.join(HBASE_ROOT, 'conf')
HBASE_CONFIG_FILE = os.path.join(HBASE_CONFIG_ROOT, 'hbase-site.xml')
HBASE_TARBALL = "http://www.apache.org/dist/hbase/%s/%s.tar.gz" % (
  HBASE_VERSION, HBASE_VERSION)
