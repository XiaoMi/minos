import getpass
import os
import subprocess
import warnings

import build_utils
from build_utils import MINOS_ROOT

from minos_config import HBASE_CONFIG_FILE
from minos_config import HBASE_CONFIG_ROOT
from minos_config import HBASE_ROOT
from minos_config import HBASE_TARBALL
from minos_config import Log
from minos_config import OPENTSDB_COLLECTOR_CONFIG_FILE
from minos_config import OPENTSDB_CONFIG_ROOT
from minos_config import OPENTSDB_PORT
from minos_config import OPENTSDB_REPOSITORY
from minos_config import OWL_CONFIG_FILE
from minos_config import OWL_PREREQUISITE_PYTHON_LIBS

BUILD_BIN_ROOT = os.getenv("BUILD_BIN_ROOT")
BUILD_DOWNLOAD_ROOT = os.getenv("BUILD_DOWNLOAD_ROOT")
ENV_PYTHON = os.getenv("ENV_PYTHON")
HBASE_CONFIG_TEMPLATE = os.getenv("HBASE_CONFIG_TEMPLATE")
HBASE_PID_FILE = os.getenv("HBASE_PID_FILE")
OPENTSDB_BIN_PATH = os.getenv("OPENTSDB_BIN_PATH")
OPENTSDB_COLLECTOR_CONFIG_TEMPLATE = os.getenv("OPENTSDB_COLLECTOR_CONFIG_TEMPLATE")
OPENTSDB_COLLECTOR_PID_FILE = os.getenv("OPENTSDB_COLLECTOR_PID_FILE")
OPENTSDB_COLLECTOR_ROOT = os.getenv("OPENTSDB_COLLECTOR_ROOT")
OPENTSDB_PID_FILE = os.getenv("OPENTSDB_PID_FILE")
OPENTSDB_ROOT = os.getenv("OPENTSDB_ROOT")
OWL_COLLECTOR_PID_FILE = os.getenv("OWL_COLLECTOR_PID_FILE")
OWL_CONFIG_TEMPLATE = os.getenv("OWL_CONFIG_TEMPLATE")
OWL_MONITOR_PID_FILE = os.getenv("OWL_MONITOR_PID_FILE")
OWL_ROOT = os.getenv("OWL_ROOT")
OWL_SETTING_FILE = os.getenv("OWL_SETTING_FILE")
OWL_SETTING_TEMPLATE = os.getenv("OWL_SETTING_TEMPLATE")
QUOTA_UPDATER_PID_FILE = os.getenv("QUOTA_UPDATER_PID_FILE")
STOP_PROCESS_SCRIPT = os.getenv("STOP_PROCESS_SCRIPT")

# Check third-party tool exists
def check_third_party_tool_exists(tool_name):
  cmd = "which %s" % tool_name
  error_message = "Please install %s firstly" % tool_name
  build_utils.check_command_output(cmd, error_message=error_message)

def create_owl_database(args, database_name, host="", port=""):
  root_pwd = getpass.getpass("Please enter password of the Mysql root user: ")

  # Create owl
  import MySQLdb as db
  try:
    if host and port:
      conn = db.connect(host=host, user='root', passwd=root_pwd, port=int(port))
    else:
      conn = db.connect(user='root', passwd=root_pwd)
  except db.Error, e:
    Log.print_critical("ERROR: %s" % str(e))

  cursor = conn.cursor()
  warnings.filterwarnings('ignore', "Can't create .*")
  cursor.execute("create database if not exists %s;" % database_name)
  cursor.execute("use mysql;")
  cursor.execute("GRANT ALL ON %s.* TO 'owl'@'localhost' identified by 'owl';"
    % database_name)
  cursor.execute("GRANT ALL ON %s.* TO 'owl'@'%s' identified by 'owl';"
    % (database_name, args.owl_ip))
  cursor.execute("flush privileges;")

  cursor.close()
  conn.close()

def configure_mysql_for_owl(database_name, host='localhost', port='3306'):
  Log.print_info("Configuring mysql for owl in %s" % OWL_SETTING_FILE)
  owl_setting_dict = {
    'DATABASE': database_name,
    'HOST': host,
    'PORT': port,
  }
  build_utils.generate_config_file(OWL_SETTING_TEMPLATE,
    OWL_SETTING_FILE, owl_setting_dict)

def create_and_configure_mysql_for_owl(args):
  if build_utils.get_build_info_option('owl', 'mysql') == 'created':
    return
  # Support both local and remote database
  choice = raw_input("Please choose Mysql server you want to use " \
    "(1 for Local, 2 for Remote): ")
  owl_prefix = raw_input("Please enter the prefix of your owl database name " \
    "(default: %s): " % getpass.getuser())
  if not owl_prefix:
    owl_prefix = getpass.getuser()
  database_name = "%s_owl" % owl_prefix

  # Using local mysql
  if int(choice) == 1:
    # Check mysql server is running
    cmd = 'ps -ef | grep mysqld | grep -v grep'
    error_message = "Please start mysql server firstly"
    build_utils.check_command_output(cmd, error_message=error_message)
    # Create owl database
    create_owl_database(args, database_name)
    # Configure mysql for owl
    configure_mysql_for_owl(database_name)

  # Using remote mysql
  elif int(choice) == 2:
    remote_address = raw_input("Please input the remote mysql " \
      "server's address (ip:port): ")
    remote_host, remote_port = remote_address.split(":")
    # Create owl database
    create_owl_database(args, database_name, host=remote_host, port=remote_port)
    # Configure mysql for owl
    configure_mysql_for_owl(database_name, remote_host, remote_port)
  else:
    Log.print_critical("ERROR: invalid choice")

  # Mark mysql database created
  build_utils.output_build_info('owl', 'mysql', 'created')

def create_django_database():
  django_entry = os.path.join(OWL_ROOT, 'manage.py')
  cmd = [ENV_PYTHON, "%s" % django_entry, "syncdb"]
  build_utils.execute_command(cmd)

def deploy_opentsdb():
  if not os.path.exists(OPENTSDB_ROOT):
    log_message = "Checkout opentsdb in %s" % OPENTSDB_ROOT
    cmd = ["git", "clone", "%s" % OPENTSDB_REPOSITORY, "%s" % OPENTSDB_ROOT]
    build_utils.execute_command(cmd, log_message=log_message)
    # copy the startup script to the OPENTSDB_ROOT
    cmd = ["cp", "%s/start_opentsdb.sh" % BUILD_BIN_ROOT, OPENTSDB_ROOT]
    build_utils.execute_command(cmd)

  # Compile opentsdb
  os.chdir(OPENTSDB_ROOT)
  log_message = "Compiling opentsdb in %s" % OPENTSDB_ROOT
  cmd = ["./build.sh"]
  build_utils.execute_command(cmd, log_message=log_message)
  os.chdir(MINOS_ROOT)

def generate_hbase_configuration():
  Log.print_info("Modify hbase-site.xml in %s" % HBASE_CONFIG_ROOT)
  cmd = "hbase_rootdir=${TMPDIR-'/tmp'}/tsdhbase;" \
    "iface=lo`uname | sed -n s/Darwin/0/p`; echo $hbase_rootdir,$iface"
  hbase_rootdir, iface = build_utils.get_command_variable(cmd).split(',')

  configuration_dict = {
    'hbase_rootdir': hbase_rootdir,
    'iface': iface,
  }
  build_utils.generate_config_file(HBASE_CONFIG_TEMPLATE,
    HBASE_CONFIG_FILE, configuration_dict)

def build_hbase():
  if build_utils.get_build_info_option('owl', 'hbase') == 'built':
    return

  if not os.path.exists(BUILD_DOWNLOAD_ROOT):
    os.mkdir(BUILD_DOWNLOAD_ROOT)
  os.chdir(BUILD_DOWNLOAD_ROOT)

  log_message = "Setup hbase in %s" % BUILD_DOWNLOAD_ROOT
  if not os.path.exists(os.path.basename(HBASE_TARBALL)):
    cmd = ["wget", "%s" % HBASE_TARBALL]
    build_utils.execute_command(cmd, log_message=log_message)

  if not os.path.exists(HBASE_ROOT):
    cmd = ["tar", "xfz", "%s" % os.path.basename(HBASE_TARBALL)]
    build_utils.execute_command(cmd)

  generate_hbase_configuration()
  os.chdir(MINOS_ROOT)

  # Mark hbase built
  build_utils.output_build_info('owl', 'hbase', 'built')

def create_hbase_table():
  if build_utils.get_build_info_option('owl', 'hbase_table') == 'created':
    return
  os.chdir(OPENTSDB_ROOT)
  log_message = "Creating hbase table for opentsdb in %s" % OPENTSDB_ROOT
  cmd = ["env", "COMPRESSION=NONE", "HBASE_HOME=%s" % HBASE_ROOT, "./src/create_table.sh"]
  build_utils.execute_command(cmd, log_message=log_message)
  os.chdir(MINOS_ROOT)

  # Mark hbase table created
  build_utils.output_build_info('owl', 'hbase_table', 'created')

def configure_opentsdb_collector(owl_port):
  # Configure opentsdb collector config file
  Log.print_info("Configuring opentsdb collector in %s" %
    OPENTSDB_COLLECTOR_CONFIG_FILE)
  opentsdb_collector_dict = {
    'owl_monitor_http_port': owl_port,
    'tsdb': OPENTSDB_BIN_PATH,
  }
  build_utils.generate_config_file(OPENTSDB_COLLECTOR_CONFIG_TEMPLATE,
    OPENTSDB_COLLECTOR_CONFIG_FILE, opentsdb_collector_dict)

def configure_owl_config(args):
  Log.print_info("Configure owl config file: %s" % OWL_CONFIG_FILE)
  owl_config_dict = {
    'owl_ip': args.owl_ip,
    'opentsdb_port': OPENTSDB_PORT,
  }
  build_utils.generate_config_file(OWL_CONFIG_TEMPLATE,
    OWL_CONFIG_FILE, owl_config_dict)

def check_input(input, yes='y'):
  return input.strip().lower() == yes.lower()

def start_hbase():
  # Start the stand-alone hbase
  build_utils.start_daemon_process('Hbase', HBASE_PID_FILE, HBASE_ROOT,
    './bin/start-hbase.sh')

def start_opentsdb():
  # Create hbase table for opentsdb
  create_hbase_table()
  # Start a TSD
  build_utils.start_daemon_process('Opentsdb', OPENTSDB_PID_FILE,
    OPENTSDB_ROOT, './start_opentsdb.sh', OPENTSDB_PORT)

def start_opentsdb_collector():
  build_utils.start_daemon_process('Opentsdb collector', OPENTSDB_COLLECTOR_PID_FILE,
    OPENTSDB_COLLECTOR_ROOT, './start_opentsdb_collector.sh')

def start_owl_collector():
  build_utils.start_daemon_process('Owl collector', OWL_COLLECTOR_PID_FILE,
    OWL_ROOT, './start_owl_collector.sh')

def start_quota_updater():
  build_utils.start_daemon_process('Quota updater', QUOTA_UPDATER_PID_FILE,
    OWL_ROOT, './start_quota_updater.sh')

def start_owl_monitor():
  owl_monitor_http_port = build_utils.get_build_info_option('owl', 'owl_port')
  if not owl_monitor_http_port:
    Log.print_critical("Owl port is null")

  build_utils.start_daemon_process('Owl monitor', OWL_MONITOR_PID_FILE,
    OWL_ROOT, './start_owl_monitor.sh', owl_monitor_http_port)

def stop_opentsdb_collector():
  build_utils.stop_daemon_process('Opentsdb collector', OPENTSDB_COLLECTOR_PID_FILE,
    OPENTSDB_COLLECTOR_ROOT, STOP_PROCESS_SCRIPT)

def stop_owl_collector():
  build_utils.stop_daemon_process('Owl collector', OWL_COLLECTOR_PID_FILE,
    OWL_ROOT, STOP_PROCESS_SCRIPT)

def stop_quota_updater():
  build_utils.stop_daemon_process('Quota updater', QUOTA_UPDATER_PID_FILE,
    OWL_ROOT, STOP_PROCESS_SCRIPT)

def stop_owl_monitor():
  build_utils.stop_daemon_process('Owl monitor', OWL_MONITOR_PID_FILE,
    OWL_ROOT, STOP_PROCESS_SCRIPT)

def _build(args):
  if args.owl_ip == '127.0.0.1' or args.owl_port == 0:
    Log.print_critical("ERROR: Building owl needs to specify the localhost ip " \
      "with '--owl_ip' and the owl monitor http port with '--owl_port'")

  Log.print_info("Building owl")
  # Check and install prerequisite python libraries
  Log.print_info("Check and install prerequisite python libraries")
  build_utils.check_and_install_modules(OWL_PREREQUISITE_PYTHON_LIBS)

  check_third_party_tool_exists("gnuplot")
  check_third_party_tool_exists("mysql")
  create_and_configure_mysql_for_owl(args)
  create_django_database()

  # Deploy hbase
  if not args.skip_setup_hbase:
    build_hbase()
    start_hbase()

  # Deploy opentsdb
  deploy_opentsdb()
  if not args.skip_setup_hbase:
    start_opentsdb()

  # Configure opentsdb collector
  configure_opentsdb_collector(str(args.owl_port))
  # Configure owl config
  configure_owl_config(args)

  # Output build information
  build_utils.output_build_info(args.component, 'owl_port', args.owl_port)
  build_utils.output_build_info(args.component, 'build_status', 'success')
  Log.print_info("The component %s is built successfully" % args.component)

def _do_start(args):
  start_owl_collector()

  if not args.skip_setup_hbase:
    start_opentsdb_collector()
  if args.quota_updater:
    start_quota_updater()

  start_owl_monitor()

def _do_stop():
  stop_owl_collector()
  stop_opentsdb_collector()
  stop_quota_updater()
  stop_owl_monitor()

def start(args):
  if not build_utils.get_build_info_option('owl', 'build_status') == 'success':
    _build(args)
  _do_start(args)

def stop(args):
  input = raw_input("Do you really want to do this ? (y/n)")
  if check_input(input):
    _do_stop()
  else:
    Log.print_info("Skip stopping owl component")

