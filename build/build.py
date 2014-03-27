import argparse

import build_owl
import build_supervisor
import build_tank
import build_utils

from minos_config import Log
from minos_config import TANK_DEFAULT_IP
from minos_config import TANK_DEFAULT_PORT

COMPONENT_BUILD_TOOL_MAP = {
  "tank": build_tank,
  "supervisor": build_supervisor,
  "owl": build_owl,
}

def add_component_arguments(parser):
  parser.add_argument("component",
    choices=COMPONENT_BUILD_TOOL_MAP.keys(),
    help="The component to built.")
  parser.add_argument("--tank_ip", type=str, nargs="?",
    default=TANK_DEFAULT_IP,
    help="The ip of localhost to use for tank server.")
  parser.add_argument("--tank_port", type=int, nargs="?",
    default=TANK_DEFAULT_PORT,
    help="The port to use for tank server.")
  parser.add_argument("--owl_ip", type=str, nargs="?",
    default='127.0.0.1',
    help="The localhost ip for owl configuration.")
  parser.add_argument("--owl_port", type=int, nargs="?",
    default=0,
    help="The port to use for owl monitor.")

def parse_command_line():
  parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description="Manage the Minos components.")

  subparsers = parser.add_subparsers(
    title="commands",
    help="Type '%(prog)s command -h' to get more help for individual command.")

  sub_parser = subparsers.add_parser(
    "start",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    help="Start the specified Minos component.")
  sub_parser.add_argument("--skip_setup_hbase", action="store_true", default=False,
    help="Whether skip setting up the default stand-alone hbase or not.")
  sub_parser.add_argument("--quota_updater", action="store_true", default=False,
    help="Whether starting quota updater or not.")
  add_component_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_start)

  sub_parser = subparsers.add_parser(
    "stop",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    help="Stop the specified Minos component.")
  add_component_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_stop)

  sub_parser = subparsers.add_parser(
    "build",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    help="Build Minos client, Tank, Supervisor offline.")
  sub_parser.add_argument("--offline", action="store_true", default=False,
    help="Whether build offline or not for Client, Tank, Supervisor.")
  sub_parser.add_argument("--package_dir", type=str, nargs="?",
    default=None, help="The offline packages directory.")
  sub_parser.set_defaults(handler=process_command_build)

  args = parser.parse_args()
  return args

def process_command_start(args):
  build_tool = COMPONENT_BUILD_TOOL_MAP.get(args.component)
  if build_tool:
    return build_tool.start(args)
  Log.print_critical("Unknown component name: %s.", args.component)

def process_command_stop(args):
  build_tool = COMPONENT_BUILD_TOOL_MAP.get(args.component)
  if build_tool:
    return build_tool.stop(args)
  Log.print_critical("Unknown component name: %s.", args.component)

def process_command_build(args):
  if not args.offline or not args.package_dir:
    Log.print_critical("ERROR: Building Minos offline needs to specify " \
      "the arguments '--offline' and the offline packages directory " \
      "'--package_dir' explicitly")

  build_utils.pip_install_offline(args.package_dir)

def main():
  args = parse_command_line()
  return args.handler(args)

if __name__ == '__main__':
  main()

