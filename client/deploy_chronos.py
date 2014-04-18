#!/usr/bin/env python

import argparse
import os
import service_config
import subprocess
import sys
import urlparse

import deploy_utils

from log import Log

ALL_JOBS = ["chronos"]

def _get_chronos_service_config(args):
  args.chronos_config = deploy_utils.get_service_config(args)

def generate_zk_jaas_config(args):
  if not deploy_utils.is_security_enabled(args):
    return ""

  config_dict = args.chronos_config.configuration.generated_files["jaas.conf"]

  for key, value in config_dict.items()[1:]:
    if value != "true" and value != "false" and value.find("\"") == -1:
      config_dict[key] = "\"" + value + "\""

  header_line = config_dict["headerLine"]
  return "Client {\n  %s\n%s;\n};" % (header_line,
    "\n".join(["  %s=%s" % (key, value)
      for (key, value) in config_dict.iteritems() if key != config_dict.keys()[0]]))

def generate_configs(args, job_name, host_id, instance_id):
  chronos_cfg_dict = args.chronos_config.configuration.generated_files["chronos.cfg"]
  hosts = args.chronos_config.jobs[job_name].hosts
  chronos_cfg = deploy_utils.generate_properties_file(args, chronos_cfg_dict)

  config_files = {
    "chronos.cfg": chronos_cfg,
    "jaas.conf" : generate_zk_jaas_config(args),
  }
  config_files.update(args.chronos_config.configuration.raw_files) # what's this?

  return config_files

def generate_run_scripts_params(args, host, job_name, host_id, instance_id):
  job = args.chronos_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "chronos", args.chronos_config.cluster.name, job_name, instance_id=instance_id)

  artifact_and_version = "chronos-" + args.chronos_config.cluster.version

  jar_dirs = "$package_dir/lib/*"
  log_level = deploy_utils.get_service_log_level(args, args.chronos_config)

  params = job.get_arguments(args, args.chronos_config.cluster, args.chronos_config.jobs,
    args.chronos_config.arguments_dict, job_name, host_id, instance_id)

  script_dict = {
      "artifact": artifact_and_version,
      "job_name": job_name,
      "jar_dirs": jar_dirs,
      "run_dir": supervisor_client.get_run_dir(),
      "params": params,
  }

  return script_dict

def generate_start_script(args, host, job_name, host_id, instance_id):
  script_params = generate_run_scripts_params(args, host, job_name, host_id, instance_id)
  return deploy_utils.create_run_script(
      "%s/start.sh.tmpl" % deploy_utils.get_template_dir(),
      script_params)

def install(args):
  _get_chronos_service_config(args)
  deploy_utils.install_service(args, "chronos", args.chronos_config, "chronos")

def cleanup(args):
  _get_chronos_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "chronos", args.chronos_config)
  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.cleanup_job("chronos", args.chronos_config,
          hosts[host_id].ip, job_name, instance_id, cleanup_token)

def bootstrap_job(args, host, job_name, host_id, instance_id, cleanup_token):
  # parse the service_config according to the instance_id
  args.chronos_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  deploy_utils.bootstrap_job(args, "chronos", "chronos",
      args.chronos_config, host, job_name, instance_id, cleanup_token, '0')
  start_job(args, host, job_name, host_id, instance_id)

def bootstrap(args):
  _get_chronos_service_config(args)
  cleanup_token = deploy_utils.confirm_bootstrap("chronos", args.chronos_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        bootstrap_job(args, hosts[host_id].ip, job_name, host_id, instance_id, cleanup_token)

def start_job(args, host, job_name, host_id, instance_id):
  # parse the service_config according to the instance_id
  args.chronos_config.parse_generated_config_files(args, job_name, host_id, instance_id)

  config_files = generate_configs(args, job_name, host_id, instance_id)
  start_script = generate_start_script(args, host, job_name, host_id, instance_id)
  http_url = deploy_utils.get_http_service_uri(host,
    args.chronos_config.jobs[job_name].base_port, instance_id)
  deploy_utils.start_job(args, "chronos", "chronos", args.chronos_config,
      host, job_name, instance_id, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  _get_chronos_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)

def stop_job(args, host, job_name, instance_id):
  deploy_utils.stop_job("chronos", args.chronos_config, host, job_name, instance_id)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  _get_chronos_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        stop_job(args, hosts[host_id].ip, job_name, instance_id)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  _get_chronos_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        stop_job(args, hosts[host_id].ip, job_name, instance_id)

  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.wait_for_job_stopping("chronos",
          args.chronos_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
        start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)

def show(args):
  _get_chronos_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.chronos_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.show_job("chronos", args.chronos_config,
          hosts[host_id].ip, job_name, instance_id)

def run_shell(args):
  Log.print_critical("'shell' command is not supported!")

def pack(args):
  Log.print_critical("'pack' command is not supported!")

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  _get_chronos_service_config(args)
  job_name = args.job[0]

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.chronos_config.jobs[job_name].hosts
  wait_time = 0

  args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
  for host_id in args.task_map.keys() or hosts.iterkeys():
    for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
      instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
      deploy_utils.confirm_rolling_update(host_id, instance_id, wait_time)
      stop_job(args, hosts[host_id].ip, job_name, instance_id)
      deploy_utils.wait_for_job_stopping("chronos",
        args.chronos_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)
      deploy_utils.wait_for_job_starting("chronos",
        args.chronos_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
