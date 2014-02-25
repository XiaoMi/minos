import deploy_utils

from log import Log

ALL_JOBS = ["nimbus", "supervisor", "ui", "logviewer"]

def generate_run_scripts_params(args, host, job_name, host_id, instance_id):
  job = args.storm_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "storm", args.storm_config.cluster.name, job_name, instance_id=instance_id)

  artifact_and_version = "storm-" + args.storm_config.cluster.version

  jar_dirs = "$package_dir/*"
  log_level = deploy_utils.get_service_log_level(args, args.storm_config)

  params = job.get_arguments(args, args.storm_config.cluster, args.storm_config.jobs,
    args.storm_config.arguments_dict, job_name, host_id, instance_id)

  service_env = "export SUPERVISOR_LOG_DIR=%s" % deploy_utils.get_supervisor_client(host,
    "storm", args.storm_config.cluster.name, 'supervisor', instance_id=instance_id).get_log_dir()

  script_dict = {
      "artifact": artifact_and_version,
      "job_name": job_name,
      "jar_dirs": jar_dirs,
      "run_dir": supervisor_client.get_run_dir(),
      "service_env": service_env,
      "params": params,
  }

  return script_dict

def generate_start_script(args, host, job_name, host_id, instance_id):
  script_params = generate_run_scripts_params(args, host, job_name, host_id, instance_id)
  return deploy_utils.create_run_script(
      "%s/start.sh.tmpl" % deploy_utils.get_template_dir(), script_params)

def generate_configs(args, host, job_name, instance_id):
  storm_yaml = deploy_utils.generate_yaml_file(
    args.storm_config.configuration.generated_files["storm.yaml"])
  config_files = {
    "storm.yaml": storm_yaml,
  }
  config_files.update(args.storm_config.configuration.raw_files)

  return config_files

def _get_storm_service_config(args):
  args.storm_config = deploy_utils.get_service_config(args)
  if not args.storm_config.cluster.zk_cluster:
    Log.print_critical(
        "storm cluster must depends on a zookeeper clusters: %s" %
        args.storm_config.cluster.name)

  nimbus_hosts = args.storm_config.jobs["nimbus"].hosts
  supervisor_hosts = args.storm_config.jobs["supervisor"].hosts
  args.storm_config.jobs["ui"].hosts = nimbus_hosts.copy()
  args.storm_config.jobs["logviewer"].hosts = supervisor_hosts.copy()

def install(args):
  _get_storm_service_config(args)
  deploy_utils.install_service(args, "storm", args.storm_config, "storm")

def bootstrap_job(args, host, job_name, host_id, instance_id, cleanup_token):
  # parse the service_config according to the instance_id
  args.storm_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  deploy_utils.bootstrap_job(args, "storm", "storm",
      args.storm_config, host, job_name, instance_id, cleanup_token, '0')
  start_job(args, host, job_name, host_id, instance_id)

def bootstrap(args):
  _get_storm_service_config(args)
  cleanup_token = deploy_utils.confirm_bootstrap("storm", args.storm_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        bootstrap_job(args, hosts[host_id].ip, job_name, host_id, instance_id, cleanup_token)

def start_job(args, host, job_name, host_id, instance_id):
  # parse the service_config according to the instance_id
  args.storm_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  config_files = generate_configs(args, host, job_name, instance_id)
  start_script = generate_start_script(args, host, job_name, host_id, instance_id)
  http_url = deploy_utils.get_http_service_uri(host,
    args.storm_config.jobs[job_name].base_port, instance_id)
  deploy_utils.start_job(args, "storm", "storm", args.storm_config,
      host, job_name, instance_id, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  _get_storm_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)

def stop_job(args, host, job_name, instance_id):
  deploy_utils.stop_job("storm", args.storm_config,
      host, job_name, instance_id)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  _get_storm_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        stop_job(args, hosts[host_id].ip, job_name, instance_id)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  _get_storm_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        stop_job(args, hosts[host_id].ip, job_name, instance_id)

  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.wait_for_job_stopping("storm",
          args.storm_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
        start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)

def cleanup(args):
  _get_storm_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "storm", args.storm_config)
  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.cleanup_job("storm", args.storm_config,
          hosts[host_id].ip, job_name, instance_id, cleanup_token)

def show(args):
  _get_storm_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.storm_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.show_job("storm", args.storm_config,
          hosts[host_id].ip, job_name, instance_id)

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  _get_storm_service_config(args)
  job_name = args.job[0]

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.storm_config.jobs[job_name].hosts
  wait_time = 0

  args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
  for host_id in args.task_map.keys() or hosts.iterkeys():
    for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
      instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
      deploy_utils.confirm_rolling_update(host_id, instance_id, wait_time)
      stop_job(args, hosts[host_id].ip, job_name, instance_id)
      deploy_utils.wait_for_job_stopping("storm",
        args.storm_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)
      deploy_utils.wait_for_job_starting("storm",
        args.storm_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

def run_shell(args):
  Log.print_critical("'shell' command is not supported!")

def pack(args):
  Log.print_critical("'pack' command is not supported!")

if __name__ == '__main__':
  test()
