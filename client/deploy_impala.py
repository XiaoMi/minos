import argparse
import os
import parallel_deploy
import service_config
import subprocess
import sys
import urlparse

import deploy_utils

from log import Log

ALL_JOBS = ["statestored", "catalogd", "impalad"]

def get_impala_service_config(args):
  args.impala_config = deploy_utils.get_service_config(args)

def generate_configs(args):
  core_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["core-site.xml"])
  hdfs_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["hdfs-site.xml"])
  hive_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["hive-site.xml"])
  hbase_site_xml = deploy_utils.generate_site_xml(args,
    args.impala_config.configuration.generated_files["hbase-site.xml"])

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "hive-site.xml": hive_site_xml,
    "hbase-site.xml": hbase_site_xml,
  }
  config_files.update(args.impala_config.configuration.raw_files)

  return config_files

def generate_run_scripts_params(args, host, job_name, host_id, instance_id):
  supervisor_client = deploy_utils.get_supervisor_client(host,
      "impala", args.impala_config.cluster.name, job_name, instance_id=instance_id)
  job = args.impala_config.jobs[job_name]

  artifact_and_version = "impala-" + args.impala_config.cluster.version
  log_level = deploy_utils.get_service_log_level(args, args.impala_config)

  params = job.get_arguments(args, args.impala_config.cluster, args.impala_config.jobs,
    args.impala_config.arguments_dict, job_name, host_id, instance_id)

  script_dict = {
    "artifact": artifact_and_version,
    "job_name": job_name,
    "run_dir": supervisor_client.get_run_dir(),
    "ticket_cache": "$run_dir/impala.tc",
    "log_level": log_level,
    "params": params,
  }

  return script_dict

def generate_start_script(args, host, job_name, host_id, instance_id):
  script_params = generate_run_scripts_params(args, host, job_name, host_id, instance_id)
  return deploy_utils.create_run_script(
      "%s/impala/start.sh.tmpl" % deploy_utils.get_template_dir(),
      script_params)

def install(args):
  get_impala_service_config(args)
  deploy_utils.install_service(args, "impala", args.impala_config, "impala")

def cleanup_job(args, host, job_name, host_id, instance_id, cleanup_token, active):
  deploy_utils.cleanup_job("impala", args.impala_config,
    host, job_name, instance_id, cleanup_token)

def cleanup(args):
  get_impala_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "impala", args.impala_config)
  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'cleanup', cleanup_token=cleanup_token)
    parallel_deploy.start_deploy_threads(cleanup_job, task_list)

def bootstrap_job(args, host, job_name, host_id, instance_id, cleanup_token, active):
  # parse the service_config according to the instance_id
  args.impala_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  deploy_utils.bootstrap_job(args, "impala", "impala",
      args.impala_config, host, job_name, instance_id, cleanup_token, '0')
  start_job(args, host, job_name, host_id, instance_id)

def bootstrap(args):
  get_impala_service_config(args)
  cleanup_token = deploy_utils.confirm_bootstrap("impala", args.impala_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'bootstrap', cleanup_token=cleanup_token)
    parallel_deploy.start_deploy_threads(bootstrap_job, task_list)

def start_job(args, host, job_name, host_id, instance_id, is_wait=False):
  if is_wait:
    deploy_utils.wait_for_job_stopping("impala",
      args.impala_config.cluster.name, job_name, host, instance_id)

  # parse the service_config according to the instance_id
  args.impala_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  config_files = generate_configs(args)
  start_script = generate_start_script(args, host, job_name, host_id, instance_id)
  http_url = deploy_utils.get_http_service_uri(host,
    args.impala_config.jobs[job_name].base_port, instance_id)
  deploy_utils.start_job(args, "impala", "impala", args.impala_config,
      host, job_name, instance_id, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'start')
    parallel_deploy.start_deploy_threads(start_job, task_list)

def stop_job(args, host, job_name, instance_id):
  deploy_utils.stop_job("impala", args.impala_config, host, job_name, instance_id)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'stop')
    parallel_deploy.start_deploy_threads(stop_job, task_list)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'stop')
    parallel_deploy.start_deploy_threads(stop_job, task_list)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'start', is_wait=True)
    parallel_deploy.start_deploy_threads(start_job, task_list)

def show_job(args, host, job_name, instance_id):
  deploy_utils.show_job("impala", args.impala_config, host, job_name, instance_id)

def show(args):
  get_impala_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.impala_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'show')
    parallel_deploy.start_deploy_threads(show_job, task_list)

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
  deploy_utils.make_package_dir(args, "impala-shell", args.impala_config.cluster)

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

  args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
  for host_id in args.task_map.keys() or hosts.iterkeys():
    for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
      instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
      deploy_utils.confirm_rolling_update(host_id, instance_id, wait_time)
      stop_job(args, hosts[host_id].ip, job_name, instance_id)
      deploy_utils.wait_for_job_stopping("impala",
        args.impala_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)
      deploy_utils.wait_for_job_starting("impala",
        args.impala_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
