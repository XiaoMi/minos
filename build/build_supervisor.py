import getpass
import os
from string import Template

import build_utils
from build_utils import MINOS_ROOT

from minos_config import Log
from minos_config import SUPERVISOR_DEPLOYMENT_DIRS
from minos_config import SUPERVISOR_PREREQUISITE_PYTHON_LIBS
from minos_config import TANK_DEFAULT_IP

SUPERVISOR_CONFIG_FILE = os.getenv("SUPERVISOR_CONFIG_FILE")
SUPERVISOR_CONFIG_TEMPLATE = os.getenv("SUPERVISOR_CONFIG_TEMPLATE")
SUPERVISOR_PID_FILE = os.getenv("SUPERVISOR_PID_FILE")
SUPERVISOR_ROOT = os.getenv("SUPERVISOR_ROOT")

def _create_deployment_directory(deploy_path):
  # Create deployment directory for supervisor
  for dir in SUPERVISOR_DEPLOYMENT_DIRS:
    deploy_dir = os.path.join(deploy_path, dir)
    if not os.path.exists(deploy_dir):
      Log.print_info('Creating the %s root %s' % (dir, deploy_dir))
      os.makedirs(deploy_dir)

def _deploy_supervisor(args, deploy_path):
  Log.print_info("Deploying supervisor in %s" % SUPERVISOR_ROOT)

  # Generate supervisord.conf according to the deploying information
  deploy_info_dict = {
    'DEPLOY_PATH': deploy_path,
    'PACKAGE_SERVER': "%s:%d" % (args.tank_ip, args.tank_port),
  }
  build_utils.generate_config_file(SUPERVISOR_CONFIG_TEMPLATE,
    SUPERVISOR_CONFIG_FILE, deploy_info_dict)

def _build(args):
  if args.tank_ip == TANK_DEFAULT_IP:
    Log.print_critical("ERROR: Building supervisor needs to specify the package server " \
      "with '--tank_ip' and '--tank_port'")

  Log.print_info("Building supervisor")
  # Check and install prerequisite python libraries
  Log.print_info("Check and install prerequisite python libraries")
  build_utils.check_and_install_modules(SUPERVISOR_PREREQUISITE_PYTHON_LIBS)

  # Create deployment directory
  deploy_path = raw_input("Please input the root directory to deploy services " \
    "(default: /home/%s): " % getpass.getuser())

  if deploy_path:
    deploy_path = os.path.abspath(os.path.realpath(deploy_path))
  else:
    deploy_path = "/home/%s" % getpass.getuser()
  _create_deployment_directory(deploy_path)

  # Deploy supervisor
  _deploy_supervisor(args, deploy_path)

  # Output build information
  build_utils.output_build_info(args.component, 'build_status', 'success')
  Log.print_info("The component %s is built successfully" % args.component)

def _do_start():
  build_utils.start_daemon_process('Supervisor', SUPERVISOR_PID_FILE,
    SUPERVISOR_ROOT, './start_supervisor.sh')

def _do_stop():
  build_utils.stop_daemon_process('Supervisor', SUPERVISOR_PID_FILE,
    SUPERVISOR_ROOT, './stop_supervisor.sh')

def start(args):
  if not build_utils.get_build_info_option('supervisor', 'build_status') == 'success':
    _build(args)
  _do_start()

def stop(args):
  _do_stop()

