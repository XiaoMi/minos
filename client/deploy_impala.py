#!/usr/bin/env python
#

import argparse
import os
import subprocess
import sys
import urlparse

import deploy_hdfs
import deploy_utils

from log import Log


ALL_JOBS = ["statestored", "impalad"]

def get_impala_service_config(args):
  args.impala_config = deploy_utils.get_service_config(args)

def generate_configs(args):
  core_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["core-site.xml"])
  hdfs_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["hdfs-site.xml"])
  hive_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["hive-site.xml"])
  log4j_xml = args.impala_config.configuration.raw_files["log4j.xml"]

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "hive-site.xml": hive_site_xml,
    "log4j.xml": log4j_xml,
  }
  return config_files

def generate_run_scripts_params(args, host, job_name):
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "impala", args.impala_config.cluster.name, job_name)
  job = args.impala_config.jobs[job_name]
  impalad = args.impala_config.jobs["impalad"]
  statestored = args.impala_config.jobs["statestored"]

  artifact_and_version = "impala-" + args.impala_config.cluster.version
  script_dict = {
    "artifact": artifact_and_version,
    "job_name": job_name,
    "run_dir": supervisor_client.get_run_dir(),
    "ticket_cache": "$run_dir/impala.tc",
    "params":
      "-webserver_port=%d " % (job.base_port + 1) +
      "-be_port=%d " % (impalad.base_port + 2) +
      "-planservice_port=%d " % (impalad.base_port + 3) +
      "-state_store_port=%d " % statestored.base_port +
      "-state_store_subscriber_port=%d " % (statestored.base_port + 1) +
      "-mem_limit=20% " + # TODO make this configurable
      "-state_store_host=%s " % statestored.hosts[0] +
      "-kerberos_reinit_interval=1200 " + # 20hours
      "-webserver_doc_root=$run_dir/package " +
      "-webserver_interface=%s " % host +
      #"-use_statestore=false " +
      "-log_dir=$run_dir/log " +
      "-v=2 " +
      "-logbuflevel=-1 " +
      "-sasl_path=$run_dir/package/lib/sasl2 ",
  }

  if job_name == "impalad":
    script_dict["params"] += "-beeswax_port=%d " % impalad.base_port
    script_dict["params"] += "-hs2_port=%d " % (impalad.base_port + 4)

  if deploy_utils.is_security_enabled(args):
    script_dict["params"] += "-principal=%s/hadoop@%s " % (
        args.impala_config.cluster.kerberos_username or "impala",
        args.impala_config.cluster.kerberos_realm)
    script_dict["params"] += "-keytab_file=%s/%s.keytab " % (
        deploy_utils.HADOOP_CONF_PATH,
        args.impala_config.cluster.kerberos_username or "impala")
    script_dict["params"] += "-tgt_file=$run_dir/impala.tc "

  return script_dict

def generate_start_script(args, host, job_name):
  script_params = generate_run_scripts_params(args, host, job_name)
  return deploy_utils.create_run_script(
      "%s/impala/start.sh.tmpl" % deploy_utils.get_template_dir(),
      script_params)

def install(args):
  get_impala_service_config(args)
  deploy_utils.install_service(args, "impala", args.impala_config, "impala")

def cleanup(args):
  get_impala_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "impala", args.impala_config)
  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.cleanup_job("impala", args.impala_config,
          hosts[id], job_name, cleanup_token)

def bootstrap_job(args, host, job_name, cleanup_token):
  deploy_utils.bootstrap_job(args, "impala", "impala",
      args.impala_config, host, job_name, cleanup_token, '0')
  start_job(args, host, job_name)

def bootstrap(args):
  get_impala_service_config(args)
  cleanup_token = deploy_utils.confirm_bootstrap("impala", args.impala_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      bootstrap_job(args, hosts[id], job_name, cleanup_token)

def start_job(args, host, job_name):
  config_files = generate_configs(args)
  start_script = generate_start_script(args, host, job_name)
  http_url = "http://%s:%d" % (host,
      args.impala_config.jobs[job_name].base_port + 1)
  deploy_utils.start_job(args, "impala", "impala", args.impala_config,
      host, job_name, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    if args.host is not None:
      args.task = deploy_utils.get_task_by_hostname(hosts, args.host)
    for id in args.task or hosts.iterkeys():
      start_job(args, hosts[id], job_name)

def stop_job(args, host, job_name):
  deploy_utils.stop_job("impala", args.impala_config, host, job_name)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    if args.host is not None:
      args.task = deploy_utils.get_task_by_hostname(hosts, args.host)
    for id in args.task or hosts.iterkeys():
      stop_job(args, hosts[id], job_name)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    if args.host is not None:
      args.task = deploy_utils.get_task_by_hostname(hosts, args.host)
    for id in args.task or hosts.iterkeys():
      stop_job(args, hosts[id], job_name)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.wait_for_job_stopping("impala",
          args.impala_config.cluster.name, job_name, hosts[id])
      start_job(args, hosts[id], job_name)

def show(args):
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    if args.host is not None:
      args.task = deploy_utils.get_task_by_hostname(hosts, args.host)
    for id in args.task or hosts.iterkeys():
      deploy_utils.show_job("impala", args.impala_config,
          hosts[id], job_name)

def run_shell(args):
  get_impala_service_config(args)

  os.environ['IMPALA_HOME'] = deploy_utils.get_root_dir("impala")
  shell_script = "%s/bin/impala-shell.sh" % deploy_utils.get_root_dir("impala")

  if not args.command:
    args.command.append("-h")

  cmd = ["bash", shell_script] + args.command
  p = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
  p.wait()

def pack(args):
  get_impala_service_config(args)
  version = args.impala_config.cluster.version
  deploy_utils.make_package_dir(args, "impala-shell", version)

  if not args.skip_tarball:
    deploy_utils.pack_package(args, "impala-shell",
        args.impala_config.cluster.version)
  Log.print_success("Pack client utilities for hadoop success!\n")

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  get_impala_service_config(args)
  job_name = args.job[0]

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.impala_config.jobs[job_name].hosts
  wait_time = 0
  for id in hosts.iterkeys():
    deploy_utils.confirm_rolling_update(id, wait_time)
    stop_job(args, hosts[id], job_name)
    deploy_utils.wait_for_job_stopping("impala",
        args.impala_config.cluster.name, job_name, hosts[id])
    start_job(args, hosts[id], job_name)
    deploy_utils.wait_for_job_starting("impala",
        args.impala_config.cluster.name, job_name, hosts[id])
    wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
