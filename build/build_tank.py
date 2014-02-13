import os

import build_utils

from build_utils import MINOS_ROOT

from minos_config import Log
from minos_config import TANK_DEFAULT_IP
from minos_config import TANK_DEFAULT_PORT
from minos_config import TANK_PREREQUISITE_PYTHON_LIBS

STOP_PROCESS_SCRIPT = os.getenv("STOP_PROCESS_SCRIPT")
TANK_ROOT = os.getenv("TANK_ROOT")
TANK_PID_FILE = os.getenv("TANK_PID_FILE")

def _build(args):
  Log.print_info("Building tank server")

  # Check and install prerequisite python libraries
  Log.print_info("Check and install prerequisite python libraries")
  build_utils.check_and_install_modules(TANK_PREREQUISITE_PYTHON_LIBS)

  # Output build information
  if args.tank_ip != TANK_DEFAULT_IP or args.tank_port != TANK_DEFAULT_PORT:
    build_utils.output_build_info(args.component, 'tank_ip', args.tank_ip)
    build_utils.output_build_info(args.component, 'tank_port', args.tank_port)

  build_utils.output_build_info(args.component, 'build_status', 'success')
  Log.print_info("The component %s is built successfully" % args.component)

def _do_start(args):
  tank_ip = build_utils.get_build_info_option('tank', 'tank_ip')
  tank_port = build_utils.get_build_info_option('tank', 'tank_port')
  if tank_ip and tank_port:
    args.tank_ip = tank_ip
    args.tank_port = int(tank_port)

  build_utils.start_daemon_process('Tank server', TANK_PID_FILE,
    TANK_ROOT, './start_tank.sh', args.tank_ip, str(args.tank_port))

def _do_stop():
  build_utils.stop_daemon_process('Tank server', TANK_PID_FILE,
    TANK_ROOT, STOP_PROCESS_SCRIPT)

def start(args):
  if not build_utils.get_build_info_option('tank', 'build_status') == 'success':
    _build(args)
  _do_start(args)

def stop(args):
  _do_stop()
