#!/usr/bin/env python
#

import argparse
import os
import pprint
import sys
import subprocess

# add the deploy client module path
cur_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath("%s/../../" % cur_dir))

from client import tank_client
from client import supervisor_client
from client import deploy_utils
from client.deploy_utils import Log

# assume infra and thirdparty are under the same parent directory
SCRIBE_BUILD_DIR = os.path.abspath(
    "%s/../../../../thirdparty/scribe" % cur_dir)
SCRIBE_BINARY_DIR = os.path.abspath(
    "%s/../../../../blade-bin/thirdparty/scribe" % cur_dir)

def parse_command_line():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description="A simple deploy tool for scribe")

  parser.add_argument("--hosts_file", default="hosts.txt",
      help="The host list file of the scribe services")

  parser.add_argument("--cluster", default="hadoop",
      help="The cluster name")

  parser.add_argument("--artifact", default="scribe",
      help="The artifact name")

  parser.add_argument("--version", default="1.0.0",
      help="The version of the scribed")

  subparsers = parser.add_subparsers(
      title="commands",
      help="Type '%(prog)s command -h' to get more help for individual "
           "command.")

  sub_parser = subparsers.add_parser(
      "build",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Build the local binary package")
  sub_parser.set_defaults(handler=build)

  sub_parser = subparsers.add_parser(
      "install",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Install binary packages to the package server")
  sub_parser.set_defaults(handler=install)

  sub_parser = subparsers.add_parser(
      "bootstrap",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Bootstrap the scribe service")
  sub_parser.set_defaults(handler=bootstrap)

  sub_parser = subparsers.add_parser(
      "start",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Start the scribe service")
  sub_parser.set_defaults(handler=start)

  sub_parser = subparsers.add_parser(
      "stop",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Stop the scribe service")
  sub_parser.set_defaults(handler=stop)

  sub_parser = subparsers.add_parser(
      "cleanup",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Cleanup the scribe service")
  sub_parser.set_defaults(handler=cleanup)

  sub_parser = subparsers.add_parser(
      "show",
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help="Show the scribe service status")
  sub_parser.set_defaults(handler=show)

  args = parser.parse_args()
  return args

def get_host_list(file_name):
  host_list = list()
  fp = open(file_name, 'rb')
  for line in fp:
    if line.startswith('#') or len(line.strip()) == 0:
      continue
    host_list.append(line.strip())
  fp.close()
  return host_list

def get_revision():
  env = os.environ
  env["LC_ALL"] = "C"
  revision_prefix = "Revision: "
  cmd = ["svn", "info", SCRIBE_BUILD_DIR]
  content = subprocess.check_output(cmd, env=env)
  for line in content.splitlines():
   if line.startswith(revision_prefix):
     return "r%s" % line[len(revision_prefix):]

def build(args):
  cmd = ["blade", "build", SCRIBE_BUILD_DIR]
  subprocess.check_call(cmd)

  cmd = ["mkdir", "-p", "%s/%s-%s" % (cur_dir, args.artifact, args.version)]
  subprocess.check_call(cmd)

  cmd = ["ln", "-sf", "%s/scribed" % SCRIBE_BINARY_DIR,
    "%s/%s-%s/" % (cur_dir, args.artifact, args.version)]
  subprocess.check_call(cmd)

  subprocess.check_call("rm -rf *.tar.gz", shell=True)

  cmd = ["tar", "-zchf", "%s-%s.tar.gz" % (args.artifact, args.version),
    "%s-%s" % (args.artifact, args.version)]
  subprocess.check_call(cmd)

def install(args):
  package_path = "%s-%s.tar.gz" % (args.artifact, args.version)
  Log.print_info("Uploading package: %s" % package_path)
  revision = get_revision()
  checksum = deploy_utils.generate_checksum(package_path)

  tank_client = deploy_utils.get_tank_client()
  package_info = tank_client.check_package(args.artifact, checksum)
  if not package_info:
    if 200 == tank_client.upload(package_path, args.artifact, revision):
      Log.print_success("Upload package %s success" % package_path)
      package_info = tank_client.check_package(args.artifact, checksum)
      pprint.pprint(eval(package_info))
  else:
    Log.print_warning("Package %s has already uploaded, skip uploading"
        % package_path)
    pprint.pprint(eval(package_info))

def bootstrap(args):
  host_list = get_host_list(args.hosts_file)
  for host in host_list:
    bootstrap_task(args, host)
    start_task(args, host)

def bootstrap_task(args, host):
  Log.print_info("Bootstraping scribed on %s" % host)
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "scribe", args.cluster, "scribed")

  bootstrap_script = open("%s/bootstrap.sh" % cur_dir, "r").read()
  message = supervisor_client.bootstrap(args.artifact,
      force_update=True, bootstrap_script=bootstrap_script)
  if deploy_utils.SUPERVISOR_SUCCESS == message:
    Log.print_success("Bootstrap scribed on %s success" % host)
  else:
    Log.print_error("Bootstrap scribed on %s fail: %s" % (host, message))

def start(args):
  host_list = get_host_list(args.hosts_file)
  for host in host_list:
    start_task(args, host)

def start_task(args, host):
  Log.print_info("Starting scribed on %s" % host)
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "scribe", args.cluster, "scribed")

  run_dir = supervisor_client.get_run_dir()
  start_template_dict = {
    "run_dir": run_dir,
  }
  start_script = deploy_utils.create_run_script("%s/start.sh" % cur_dir,
      start_template_dict)

  config_files = {
    "scribe.conf": open("%s/scribe.conf" % cur_dir, "r").read(),
  }
  message = supervisor_client.start(args.artifact,
      start_script=start_script, **config_files)
  if deploy_utils.SUPERVISOR_SUCCESS == message:
    Log.print_success("Start scribed on %s success" % host)
  else:
    Log.print_error("Start scribed on %s fail: %s" % (host, message))

def stop(args):
  host_list = get_host_list(args.hosts_file)
  for host in host_list:
    stop_task(args, host)

def stop_task(args, host):
  Log.print_info("Stopping scribed on %s" % host)
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "scribe", args.cluster, "scribed")

  message = supervisor_client.stop()
  if deploy_utils.SUPERVISOR_SUCCESS == message:
    Log.print_success("Stop scribed on %s success" % host)
  else:
    Log.print_error("Stop scribed on %s fail: %s" % (host, message))

def cleanup(args):
  host_list = get_host_list(args.hosts_file)
  for host in host_list:
    cleanup_task(args, host)

def cleanup_task(args, host):
  Log.print_info("Cleanuping scribed on %s" % host)
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "scribe", args.cluster, "scribed")

  message = supervisor_client.cleanup('', '')
  if deploy_utils.SUPERVISOR_SUCCESS == message:
    Log.print_success("Cleanup scribed on %s success" % host)
  else:
    Log.print_error("Cleanup scirbed on %s fail: %s" % (host, message))

def show(args):
  host_list = get_host_list(args.hosts_file)
  for host in host_list:
    show_task(args, host)

def show_task(args, host):
  Log.print_info("Showing scribed on %s" % host)
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "scribe", args.cluster, "scribed")
  state = supervisor_client.show()
  if state == 'RUNNING':
    Log.print_success("scribed on %s is %s" % (host, state))
  else:
    Log.print_error("scribed on %s is %s" % (host, state))

def main():
  args = parse_command_line()
  return args.handler(args)

if __name__ == '__main__':
  main()
