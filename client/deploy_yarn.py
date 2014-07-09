import argparse
import deploy_hdfs
import deploy_utils
import parallel_deploy
import subprocess
import sys
import urlparse

from log import Log

ALL_JOBS = ["resourcemanager", "nodemanager", "historyserver", "proxyserver"]

SHELL_COMMAND_INFO = {
  "rmadmin": ("org.apache.hadoop.yarn.server.resourcemanager.tools.RMAdmin",
      "admin tools"),
  "version": ("org.apache.hadoop.util.VersionInfo", "print the version"),
  "jar": ("org.apache.hadoop.util.RunJar", "run a jar file"),
  "logs": ("org.apache.hadoop.yarn.logaggregation.LogDumper",
      "dump container logs"),
  "daemonlog": ("org.apache.hadoop.log.LogLevel",
      "get/set the log level for each daemon"),
}

def get_yarn_service_config(args):
  args.yarn_config = deploy_utils.get_service_config(args)
  if not args.yarn_config.cluster.zk_cluster:
    Log.print_critical(
        "yarn cluster must depends on a zookeeper clusters: %s" %
        args.yarn_config.cluster.name)

def generate_metrics_config(args, host, job_name, instance_id=-1):
  job = args.yarn_config.jobs[job_name]
  supervisor_client = deploy_utils.get_supervisor_client(host, "yarn",
      args.yarn_config.cluster.name, job_name, instance_id=instance_id)

  ganglia_switch = "# "
  if args.yarn_config.cluster.ganglia_address:
    ganglia_switch = ""
  config_dict = {
      "job_name": job_name,
      "period": 10,
      "data_dir": supervisor_client.get_log_dir(),
      "ganglia_address": args.yarn_config.cluster.ganglia_address,
      "ganglia_switch": ganglia_switch,
  }

  local_path = "%s/hadoop-metrics2.properties.tmpl" % deploy_utils.get_template_dir()
  template = deploy_utils.Template(open(local_path, "r").read())
  return template.substitute(config_dict)

def generate_configs(args, host, job_name, instance_id):
  core_site_xml = deploy_utils.generate_site_xml(args,
    args.yarn_config.configuration.generated_files["core-site.xml"])
  hdfs_site_xml = deploy_utils.generate_site_xml(args,
    args.yarn_config.configuration.generated_files["hdfs-site.xml"])
  mapred_site_xml = deploy_utils.generate_site_xml(args,
    args.yarn_config.configuration.generated_files["mapred-site.xml"])
  yarn_site_xml = deploy_utils.generate_site_xml(args,
    args.yarn_config.configuration.generated_files["yarn-site.xml"])
  hadoop_metrics2_properties = generate_metrics_config(args, host, job_name, instance_id)

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "mapred-site.xml": mapred_site_xml,
    "yarn-site.xml": yarn_site_xml,
    "hadoop-metrics2.properties": hadoop_metrics2_properties,
  }
  config_files.update(args.yarn_config.configuration.raw_files)

  return config_files

def generate_run_scripts_params(args, host, job_name, host_id, instance_id):
  job = args.yarn_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "yarn", args.yarn_config.cluster.name, job_name, instance_id=instance_id)

  artifact_and_version = "hadoop-" + args.yarn_config.cluster.version

  jar_dirs = ""
  for component in ["common", "mapreduce", "yarn", "hdfs"]:
    if jar_dirs: jar_dirs += ":"
    component_dir = ("$package_dir/share/hadoop/%s" % component)
    jar_dirs += "%s/:%s/lib/*:%s/*" % (
        component_dir, component_dir, component_dir)

  service_env = ""
  for component_path in ["HADOOP_COMMON_HOME", "HADOOP_HDFS_HOME", "YARN_HOME"]:
    service_env += "export %s=$package_dir\n" % (component_path)
  log_level = deploy_utils.get_service_log_level(args, args.yarn_config)

  params = job.get_arguments(args, args.yarn_config.cluster, args.yarn_config.jobs,
    args.yarn_config.arguments_dict, job_name, host_id, instance_id)

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
      "%s/start.sh.tmpl" % deploy_utils.get_template_dir(),
      script_params)

def install(args):
  get_yarn_service_config(args)
  deploy_utils.install_service(args, "yarn", args.yarn_config, "hadoop")

def cleanup_job(args, host, job_name, host_id, instance_id, cleanup_token, active):
  deploy_utils.cleanup_job("yarn", args.yarn_config,
    host, job_name, instance_id, cleanup_token)

def cleanup(args):
  get_yarn_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "yarn", args.yarn_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'cleanup', cleanup_token=cleanup_token)
    parallel_deploy.start_deploy_threads(cleanup_job, task_list)

def bootstrap_job(args, host, job_name, host_id, instance_id, cleanup_token, active):
  # parse the service_config according to the instance_id
  args.yarn_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  deploy_utils.bootstrap_job(args, "hadoop", "yarn",
      args.yarn_config, host, job_name, instance_id, cleanup_token, '0')
  start_job(args, host, job_name, host_id, instance_id)

def bootstrap(args):
  get_yarn_service_config(args)
  cleanup_token = deploy_utils.confirm_bootstrap("yarn", args.yarn_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'bootstrap', cleanup_token=cleanup_token)
    parallel_deploy.start_deploy_threads(bootstrap_job, task_list)

def start_job(args, host, job_name, host_id, instance_id, is_wait=False):
  if is_wait:
    deploy_utils.wait_for_job_stopping("yarn",
      args.yarn_config.cluster.name, job_name, host, instance_id)

  # parse the service_config according to the instance_id
  args.yarn_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  config_files = generate_configs(args, host, job_name, instance_id)
  start_script = generate_start_script(args, host, job_name, host_id, instance_id)
  http_url = deploy_utils.get_http_service_uri(host,
    args.yarn_config.jobs[job_name].base_port, instance_id)
  deploy_utils.start_job(args, "hadoop", "yarn", args.yarn_config,
      host, job_name, instance_id, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'start')
    parallel_deploy.start_deploy_threads(start_job, task_list)

def stop_job(args, host, job_name, instance_id):
  deploy_utils.stop_job("yarn", args.yarn_config,
      host, job_name, instance_id)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'stop')
    parallel_deploy.start_deploy_threads(stop_job, task_list)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'stop')
    parallel_deploy.start_deploy_threads(stop_job, task_list)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'start', is_wait=True)
    parallel_deploy.start_deploy_threads(start_job, task_list)

def show_job(args, host, job_name, instance_id):
  deploy_utils.show_job("yarn", args.yarn_config, host, job_name, instance_id)

def show(args):
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'show')
    parallel_deploy.start_deploy_threads(show_job, task_list)

def run_shell(args):
  get_yarn_service_config(args)

  main_class, options = deploy_utils.parse_shell_command(
      args, SHELL_COMMAND_INFO)
  if not main_class:
    return

  # parse the service_config, suppose the instance_id is -1
  args.yarn_config.parse_generated_config_files(args)
  core_site_dict = args.yarn_config.configuration.generated_files["core-site.xml"]
  hdfs_site_dict = args.yarn_config.configuration.generated_files["hdfs-site.xml"]
  mapred_site_dict = args.yarn_config.configuration.generated_files["mapred-site.xml"]
  yarn_site_dict = args.yarn_config.configuration.generated_files["yarn-site.xml"]

  hadoop_opts = list()
  for key, value in core_site_dict.iteritems():
    hadoop_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))
  for key, value in hdfs_site_dict.iteritems():
    hadoop_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))
  for key, value in mapred_site_dict.iteritems():
    hadoop_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))
  for key, value in yarn_site_dict.iteritems():
    hadoop_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))

  if deploy_utils.is_security_enabled(args):
    hadoop_opts.append(
        "-Djava.security.krb5.conf=%s/krb5-hadoop.conf" %
        deploy_utils.get_config_dir())

  package_root = deploy_utils.get_artifact_package_root(args,
      args.yarn_config.cluster, "hadoop")
  lib_root = "%s/share/hadoop" % package_root
  class_path = "%s/etc/hadoop" % package_root
  for component in ["common", "hdfs", "mapreduce", "yarn"]:
    component_dir = "%s/%s" % (lib_root, component)
    class_path += ":%s/:%s/*:%s/lib/*" % (component_dir,
        component_dir, component_dir)

  cmd = (["java", "-cp", class_path] + hadoop_opts +
      [main_class] + options)
  p = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
  p.wait()

def generate_client_config(args, artifact, version):
  config_path = "%s/%s/%s-%s/etc/hadoop" % (args.package_root,
      args.cluster, artifact, version)
  deploy_utils.write_file("%s/mapred-site.xml" % config_path,
      deploy_utils.generate_site_xml(args,
        args.yarn_config.configuration.generated_files["mapred-site.xml"]))
  deploy_utils.write_file("%s/yarn-site.xml" % config_path,
      deploy_utils.generate_site_xml(args,
        args.yarn_config.configuration.generated_files["yarn-site.xml"]))
  deploy_utils.write_file("%s/krb5.conf" % config_path,
      args.yarn_config.configuration.raw_files["krb5.conf"])
  deploy_hdfs.update_hadoop_env_sh(args, artifact, version, "YARN_OPTS")

def pack(args):
  get_yarn_service_config(args)
  args.yarn_config.parse_generated_config_files(args)
  version = args.yarn_config.cluster.version
  deploy_utils.make_package_dir(args, "hadoop", args.yarn_config.cluster)
  args.hdfs_config.parse_generated_config_files(args)
  deploy_hdfs.generate_client_config(args, "hadoop", version)
  generate_client_config(args, "hadoop", version)

  if not args.skip_tarball:
    deploy_utils.pack_package(args, "hadoop", args.yarn_config.cluster.version)
  Log.print_success("Pack client utilities for hadoop success!\n")

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  get_yarn_service_config(args)
  job_name = args.job[0]

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.yarn_config.jobs[job_name].hosts
  wait_time = 0

  args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
  for host_id in args.task_map.keys() or hosts.iterkeys():
    for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
      instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
      deploy_utils.confirm_rolling_update(host_id, instance_id, wait_time)
      stop_job(args, hosts[host_id].ip, job_name, instance_id)
      deploy_utils.wait_for_job_stopping("yarn",
        args.yarn_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)
      deploy_utils.wait_for_job_starting("yarn",
        args.yarn_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
