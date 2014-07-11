import deploy_utils

from log import Log

ALL_JOBS = ["restserver", "proxy", "cleaner"]

def _get_fds_service_config(args):
  args.fds_config = deploy_utils.get_service_config(args)

def install(args):
  _get_fds_service_config(args)
  deploy_utils.install_service(args, "fds", args.fds_config, "galaxy")

def bootstrap_job(args, host, job_name, host_id, instance_id, cleanup_token):
  # parse the service_config according to the instance_id
  args.fds_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  deploy_utils.bootstrap_job(args, "galaxy", "fds",
      args.fds_config, host, job_name, instance_id, cleanup_token, '0')
  start_job(args, host, job_name, host_id, instance_id)

def bootstrap(args):
  _get_fds_service_config(args)
  cleanup_token = deploy_utils.confirm_bootstrap("fds", args.fds_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        bootstrap_job(args, hosts[host_id].ip, job_name, host_id, instance_id, cleanup_token)

def generate_run_scripts_params(args, host, job_name, host_id, instance_id):
  job = args.fds_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "fds", args.fds_config.cluster.name, job_name, instance_id=instance_id)

  artifact_and_version = "galaxy-fds-" + args.fds_config.cluster.version

  component_dir = "$package_dir"
  jar_dirs = "%s/lib/guava-11.0.2.jar:%s/:%s/lib/*" % (
    component_dir, component_dir, component_dir)
  log_level = deploy_utils.get_service_log_level(args, args.fds_config)

  params = job.get_arguments(args, args.fds_config.cluster, args.fds_config.jobs,
    args.fds_config.arguments_dict, job_name, host_id, instance_id)

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
      "%s/start.sh.tmpl" % deploy_utils.get_template_dir(), script_params)

def generate_configs(args, host, job_name, instance_id):
  core_site_xml = deploy_utils.generate_site_xml(args,
    args.fds_config.configuration.generated_files["core-site.xml"])
  hdfs_site_xml = deploy_utils.generate_site_xml(args,
    args.fds_config.configuration.generated_files["hdfs-site.xml"])
  hbase_site_xml = deploy_utils.generate_site_xml(args,
    args.fds_config.configuration.generated_files["hbase-site.xml"])
  galaxy_site_xml = deploy_utils.generate_site_xml(args,
    args.fds_config.configuration.generated_files["galaxy-site.xml"])
  zookeeper_properties = deploy_utils.generate_properties_file(args,
      args.fds_config.configuration.generated_files["zookeeper.properties"])
  mapred_site_xml = deploy_utils.generate_site_xml(args,
    args.fds_config.configuration.generated_files["mapred-site.xml"])
  yarn_site_xml = deploy_utils.generate_site_xml(args,
    args.fds_config.configuration.generated_files["yarn-site.xml"])
  passport_properties = deploy_utils.generate_properties_file(args,
      args.fds_config.configuration.generated_files["passport.properties"])

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "hbase-site.xml": hbase_site_xml,
    "galaxy-site.xml": galaxy_site_xml,
    "zookeeper.properties": zookeeper_properties,
    "mapred-site.xml": mapred_site_xml,
    "yarn-site.xml": yarn_site_xml,
    "passport.properties": passport_properties,
  }
  config_files.update(args.fds_config.configuration.raw_files)

  return config_files

def start_job(args, host, job_name, host_id, instance_id):
  # parse the service_config according to the instance_id
  args.fds_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  config_files = generate_configs(args, host, job_name, instance_id)
  start_script = generate_start_script(args, host, job_name, host_id, instance_id)
  http_url = deploy_utils.get_http_service_uri(host,
    args.fds_config.jobs[job_name].base_port, instance_id)
  deploy_utils.start_job(args, "galaxy", "fds", args.fds_config,
      host, job_name, instance_id, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  _get_fds_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)

def stop_job(args, host, job_name, instance_id):
  deploy_utils.stop_job("fds", args.fds_config,
      host, job_name, instance_id)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  _get_fds_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        stop_job(args, hosts[host_id].ip, job_name, instance_id)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  _get_fds_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        stop_job(args, hosts[host_id].ip, job_name, instance_id)

  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.wait_for_job_stopping("fds",
          args.fds_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
        start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)

def cleanup(args):
  _get_fds_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "fds", args.fds_config)
  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.cleanup_job("fds", args.fds_config,
          hosts[host_id].ip, job_name, instance_id, cleanup_token)

def show(args):
  _get_fds_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.fds_config.jobs[job_name].hosts
    args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
    for host_id in args.task_map.keys() or hosts.keys():
      for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
        instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
        deploy_utils.show_job("fds", args.fds_config,
          hosts[host_id].ip, job_name, instance_id)

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  _get_fds_service_config(args)
  job_name = args.job[0]

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.fds_config.jobs[job_name].hosts
  wait_time = 0

  args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
  for host_id in args.task_map.keys() or hosts.iterkeys():
    for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
      instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
      deploy_utils.confirm_rolling_update(host_id, instance_id, wait_time)
      stop_job(args, hosts[host_id].ip, job_name, instance_id)
      deploy_utils.wait_for_job_stopping("fds",
        args.fds_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)
      deploy_utils.wait_for_job_starting("fds",
        args.fds_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

def run_shell(args):
  Log.print_critical("'shell' command is not supported!")

def pack(args):
  Log.print_critical("'pack' command is not supported!")

if __name__ == '__main__':
  test()
