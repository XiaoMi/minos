import argparse
import os
import platform
import pwd
import sys

import deploy_hbase
import deploy_hdfs
import deploy_utils
import deploy_zookeeper
import deploy_yarn
import deploy_impala
import deploy_kafka
import deploy_storm
import deploy_fds
import deploy_chronos
import deploy_mapreduce

from log import Log

SERVICE_DEPLOY_TOOL_MAP = {
  "hdfs": deploy_hdfs,
  "yarn": deploy_yarn,
  "hbase": deploy_hbase,
  "zookeeper": deploy_zookeeper,
  "impala": deploy_impala,
  "kafka": deploy_kafka,
  "storm": deploy_storm,
  "fds": deploy_fds,
  "chronos": deploy_chronos,
  "mapreduce": deploy_mapreduce,
}

LOG_LEVEL_RANGE_MAP = [
  "trace", "debug", "info", "warn", "error", "fatal"
]

def add_service_arguments(parser):
  # NOTE: add_service_arguments must be called lastly.
  parser.add_argument("service",
      choices=SERVICE_DEPLOY_TOOL_MAP.keys(),
      help="The service type to be deployed.")
  parser.add_argument("cluster",
      help="The cluster name where the service would be deployed.")
  parser.add_argument("--job", type=str, nargs="+",
      help="The list of jobs to be manipulated, separated by space. If empty, "
           "all jobs would be manipulated.")
  parser.add_argument("--log_level", type=str, default="",
      choices=LOG_LEVEL_RANGE_MAP,
      help="The global log level to be configured for the service.")
  parser.add_argument("--thread_num", type=int, default=1,
      help="The number of threads used to deploy data nodes parallelly.")
  task_group = parser.add_mutually_exclusive_group()
  task_group.add_argument("--task", type=str, nargs="+",
      help="The list of tasks to be manipulated, separated by space. If task "
           "and host are all empty, all tasks would be manipulated. "
           "Option --task is exclusive with --host.")
  task_group.add_argument("--host", type=str, nargs="+",
      help="The list of task hosts to be manipulated, separated by space. If "
           "task and host are all empty, all tasks would be manipulated. "
           "Option --task is exclusive with --host. "
           "--host option is only supported in hbase cluster now.")

def parse_command_line():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description="Manage the hadoop cluster.")

  parser.add_argument("--version", action="version",
      version="%(prog)s 1.0.0-beta")
  parser.add_argument("-v", "--verbosity", default=0, type=int,
      help="The verbosity level of log, higher value, more details.")

  parser.add_argument("--remote_user", default="work",
      help="The user to login remote machines.")

  subparsers = parser.add_subparsers(
      title="commands",
      help="Type '%(prog)s command -h' to get more help for individual "
           "command.")

  sub_parser = subparsers.add_parser(
      "install",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Install binary packages to a cluster according to specified "
           "configuration. Only binary package (tarball) would be installed, "
           "config files and start/stop scripts would NOT be installed.")
  sub_parser.add_argument("--make_current", action="store_false",
      help="Make the installed pacakge as current version.")
  # NOTE: add_service_arguments must be called lastly.
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_install)

  sub_parser = subparsers.add_parser(
      "cleanup",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Cleanup all data files of a service. Used when you want to "
           "re-deploy a service and discard all old data.\n"
           "NOTE: before using it, make sure you know what's going to happen!")
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_cleanup)

  sub_parser = subparsers.add_parser(
      "bootstrap",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Bootstrap a new cluster for a service. "
           "It would fail if old data of this service exists.")
  add_specify_version_options(sub_parser)
  sub_parser.add_argument("--update_config", action="store_true",
      default=False, help="Update the config files")
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_bootstrap)

  sub_parser = subparsers.add_parser(
      "start",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Start whole service on the specified cluster. Config files and "
           "control scripts (start/stop/restart, etc) would be generated at "
           "this phase and copied to destination hosts.")
  add_specify_version_options(sub_parser)
  sub_parser.add_argument("--skip_confirm", action="store_true",
      default=False, help="Whether skip the confirmation or not")
  sub_parser.add_argument("--update_config", action="store_true",
      default=False, help="Update the config files")
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_start)

  sub_parser = subparsers.add_parser(
      "stop",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Stop whole service on the specified cluster.")
  add_service_arguments(sub_parser)
  sub_parser.add_argument("--skip_confirm", action="store_true",
      default=False, help="Whether skip the confirmation or not")
  sub_parser.set_defaults(handler=process_command_stop)

  sub_parser = subparsers.add_parser(
      "restart",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Restart whole service on the specified cluster.")
  add_specify_version_options(sub_parser)
  sub_parser.add_argument("--skip_confirm", action="store_true",
      default=False, help="Whether skip the confirmation or not")
  sub_parser.add_argument("--update_config", action="store_true",
      default=False, help="Update the config files")
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_restart)

  sub_parser = subparsers.add_parser(
      "show",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Show status of packages/services/jobs/tasks.")
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_show)

  sub_parser = subparsers.add_parser(
      "shell",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Run the shell of specified service %s" % SERVICE_DEPLOY_TOOL_MAP.keys(),
      )
  add_specify_version_options(sub_parser)
  add_service_arguments(sub_parser)
  sub_parser.add_argument("command", nargs=argparse.REMAINDER,
      help="The command to execute")
  sub_parser.set_defaults(handler=process_command_shell)

  sub_parser = subparsers.add_parser(
      "pack",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Pack client utilities of Hadoop/Hbase/Zookeeper for users")
  add_specify_version_options(sub_parser)
  sub_parser.add_argument("--package_root", default="./packages",
      help="The local root to store the packed pacakges")
  sub_parser.add_argument("--skip_tarball", action="store_true",
      help="Skip make the tarball of the packed package")
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_pack)

  sub_parser = subparsers.add_parser(
      "rolling_update",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Rolling update the specified job, users must specify "
           "the job through the --job option")
  sub_parser.add_argument("--skip_confirm", action="store_true",
      default=False, help="Whether skip the confirmation or not")
  sub_parser.add_argument("--vacate_rs", action="store_true",
      default=False, help="Whether to vacate region server before restart it")
  sub_parser.add_argument("--time_interval", default=120, type=int,
      help="The time interval between rolling update tasks")
  sub_parser.add_argument("--update_config", action="store_true",
      default=False, help="Update the config files")
  add_specify_version_options(sub_parser)
  add_service_arguments(sub_parser)
  sub_parser.set_defaults(handler=process_command_rolling_update)

  args = parser.parse_args()
  Log.verbosity = args.verbosity
  return args

def add_specify_version_options(sub_parser):
  sub_parser.add_argument("--package_name", default="",
      help="Specify a package to bootstrap")
  sub_parser.add_argument("--revision", default="",
      help="Specify a revision of a package to bootstrap, should be "
           "specified along with --package_name, otherwise, will be ignored")
  sub_parser.add_argument("--timestamp", default="",
      help="Specify a timestamp of a package to bootstrap, should be "
           "specified along with --package_name and --revision, otherwise "
           "will be ignored")
  sub_parser.add_argument("--update_package", action="store_true",
      help="Force the supervisor server to download the latest package from "
           "the package server, if the package_name, revsion and timestamp "
           "are specified, this option will be ignored")

def process_command_install(args):
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.install(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_cleanup(args):
  deploy_utils.check_admin_priviledge(args)
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.cleanup(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_bootstrap(args):
  deploy_utils.check_admin_priviledge(args)
  args.update_config = True
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.bootstrap(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_start(args):
  deploy_utils.check_admin_priviledge(args)
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.start(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_stop(args):
  deploy_utils.check_admin_priviledge(args)
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.stop(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_restart(args):
  deploy_utils.check_admin_priviledge(args)
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.restart(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_show(args):
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.show(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_shell(args):
  deploy_utils.check_admin_priviledge(args)
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.run_shell(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_pack(args):
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.pack(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def process_command_rolling_update(args):
  deploy_utils.check_admin_priviledge(args)
  deploy_tool = SERVICE_DEPLOY_TOOL_MAP.get(args.service)
  if deploy_tool:
    return deploy_tool.rolling_update(args)
  Log.print_critical("Not implemented for service: %s", args.service)

def main():
  args = parse_command_line()
  return args.handler(args)

if __name__ == '__main__':
  main()
