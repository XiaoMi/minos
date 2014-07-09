import argparse
import deploy_utils
import deploy_zookeeper
import os
import parallel_deploy
import pwd
import socket
import subprocess
import sys
import tempfile
import urlparse

from log import Log

# regionserver must start before master
ALL_JOBS = ["regionserver", "master"]

SHELL_COMMAND_INFO = {
  "shell": ("org.jruby.Main", "run the HBase shell"),
  "ruby": ("org.jruby.Main", "run the ruby shell"),
  "hbck": ("org.apache.hadoop.hbase.util.HBaseFsck",
      "run the hbase 'fsck' tool"),
  "htck": ("com.xiaomi.infra.hbase.AvailabilityTool",
      "run the hbase table availability check tool"),
  "hlog": ("org.apache.hadoop.hbase.regionserver.wal.HLogPrettyPrinter",
      "write-ahead-log analyzer"),
  "hfile": ("org.apache.hadoop.hbase.io.hfile.HFile", "store file analyzer"),
  "version": ("org.apache.hadoop.hbase.util.VersionInfo", "print the version"),
}

def generate_metrics_config(args, host, job_name, instance_id=-1):
  job = args.hbase_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "hbase", args.hbase_config.cluster.name, job_name, instance_id=instance_id)

  ganglia_switch = "# "
  if args.hbase_config.cluster.ganglia_address:
    ganglia_switch = ""
  config_dict = {
      "job_name": job_name,
      "period": 10,
      "data_dir": supervisor_client.get_log_dir(),
      "ganglia_address": args.hbase_config.cluster.ganglia_address,
      "ganglia_switch": ganglia_switch,
  }

  local_path = "%s/hadoop-metrics.properties.tmpl" % deploy_utils.get_template_dir()
  template = deploy_utils.Template(open(local_path, "r").read())
  return template.substitute(config_dict)

def generate_zk_jaas_config(args):
  if not deploy_utils.is_security_enabled(args):
    return ""

  config_dict = args.hbase_config.configuration.generated_files["jaas.conf"]

  for key, value in config_dict.items()[1:]:
    if value != "true" and value != "false" and value.find("\"") == -1:
      config_dict[key] = "\"" + value + "\""

  header_line = config_dict["headerLine"]
  return "Client {\n  %s\n%s;\n};" % (header_line,
    "\n".join(["  %s=%s" % (key, value)
      for (key, value) in config_dict.iteritems() if key != config_dict.keys()[0]]))


def generate_configs(args, host, job_name, instance_id):
  core_site_xml = deploy_utils.generate_site_xml(args,
    args.hbase_config.configuration.generated_files["core-site.xml"])
  hdfs_site_xml = deploy_utils.generate_site_xml(args,
    args.hbase_config.configuration.generated_files["hdfs-site.xml"])
  hbase_site_xml = deploy_utils.generate_site_xml(args,
    args.hbase_config.configuration.generated_files["hbase-site.xml"])
  hadoop_metrics_properties = generate_metrics_config(args, host, job_name, instance_id)
  zk_jaas_conf = generate_zk_jaas_config(args)

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "hbase-site.xml": hbase_site_xml,
    "hadoop-metrics.properties": hadoop_metrics_properties,
    "jaas.conf": zk_jaas_conf,
  }
  config_files.update(args.hbase_config.configuration.raw_files)

  return config_files

def generate_run_scripts_params(args, host, job_name, host_id, instance_id):
  job = args.hbase_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "hbase", args.hbase_config.cluster.name, job_name, instance_id=instance_id)

  artifact_and_version = "hbase-" + args.hbase_config.cluster.version

  component_dir = "$package_dir/"
  # must include both [dir]/ and [dir]/* as [dir]/* only import all jars under
  # this dir but we also need access the webapps under this dir.
  jar_dirs = "%s/:%s/lib/*:%s/*" % (component_dir, component_dir, component_dir)
  log_level = deploy_utils.get_service_log_level(args, args.hbase_config)

  params = job.get_arguments(args, args.hbase_config.cluster, args.hbase_config.jobs,
    args.hbase_config.arguments_dict, job_name, host_id, instance_id)

  script_dict = {
      "artifact": artifact_and_version,
      "job_name": job_name,
      "jar_dirs": jar_dirs,
      "run_dir": supervisor_client.get_run_dir(),
      "params": params,
  }

  return script_dict

def get_hbase_service_config(args):
  args.hbase_config = deploy_utils.get_service_config(args)
  if not args.hbase_config.cluster.zk_cluster:
    Log.print_critical(
        "hdfs cluster must depends on a zookeeper clusters: %s" %
        args.hbase_config.cluster.name)

def generate_start_script(args, host, job_name, host_id, instance_id):
  script_params = generate_run_scripts_params(args, host, job_name, host_id, instance_id)
  return deploy_utils.create_run_script(
      "%s/start.sh.tmpl" % deploy_utils.get_template_dir(),
      script_params)

def install(args):
  get_hbase_service_config(args)
  deploy_utils.install_service(args, "hbase", args.hbase_config, "hbase")

def cleanup_job(args, host, job_name, host_id, instance_id, cleanup_token, active):
  deploy_utils.cleanup_job("hbase", args.hbase_config,
    host, job_name, instance_id, cleanup_token)

def cleanup(args):
  get_hbase_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "hbase", args.hbase_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'cleanup', cleanup_token=cleanup_token)
    parallel_deploy.start_deploy_threads(cleanup_job, task_list)

def bootstrap_job(args, host, job_name, host_id, instance_id, cleanup_token, active):
  # parse the service_config according to the instance_id
  args.hbase_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  deploy_utils.bootstrap_job(args, "hbase", "hbase",
      args.hbase_config, host, job_name, instance_id, cleanup_token, '0')
  start_job(args, host, job_name, host_id, instance_id)

def bootstrap(args):
  get_hbase_service_config(args)

  cleanup_token = deploy_utils.confirm_bootstrap("hbase", args.hbase_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'bootstrap', cleanup_token=cleanup_token)
    parallel_deploy.start_deploy_threads(bootstrap_job, task_list)

def start_job(args, host, job_name, host_id, instance_id, is_wait=False):
  if is_wait:
    deploy_utils.wait_for_job_stopping("hbase",
      args.hbase_config.cluster.name, job_name, host, instance_id)
  # parse the service_config according to the instance_id
  args.hbase_config.parse_generated_config_files(args, job_name, host_id, instance_id)
  config_files = generate_configs(args, host, job_name, instance_id)
  start_script = generate_start_script(args, host, job_name, host_id, instance_id)
  http_url = deploy_utils.get_http_service_uri(host,
    args.hbase_config.jobs[job_name].base_port, instance_id)
  deploy_utils.start_job(args, "hbase", "hbase", args.hbase_config,
      host, job_name, instance_id, start_script, http_url, **config_files)

def start(args):
  if not args.skip_confirm:
    deploy_utils.confirm_start(args)
  get_hbase_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'start')
    parallel_deploy.start_deploy_threads(start_job, task_list)

def stop_job(args, host, job_name, instance_id):
  deploy_utils.stop_job("hbase", args.hbase_config,
      host, job_name, instance_id)

def stop(args):
  if not args.skip_confirm:
    deploy_utils.confirm_stop(args)
  get_hbase_service_config(args)

  for job_name in args.job or reversed(ALL_JOBS):
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'stop')
    parallel_deploy.start_deploy_threads(stop_job, task_list)

def restart(args):
  if not args.skip_confirm:
    deploy_utils.confirm_restart(args)
  get_hbase_service_config(args)

  for job_name in args.job or reversed(ALL_JOBS):
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'stop')
    parallel_deploy.start_deploy_threads(stop_job, task_list)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name,
      'start', is_wait=True)
    parallel_deploy.start_deploy_threads(start_job, task_list)

def show_job(args, host, job_name, instance_id):
  deploy_utils.show_job("hbase", args.hbase_config, host, job_name, instance_id)

def show(args):
  get_hbase_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hbase_config.jobs[job_name].hosts
    task_list = deploy_utils.schedule_task_for_threads(args, hosts, job_name, 'show')
    parallel_deploy.start_deploy_threads(show_job, task_list)

def run_shell(args):
  get_hbase_service_config(args)

  main_class, options = deploy_utils.parse_shell_command(
      args, SHELL_COMMAND_INFO)
  if not main_class:
    return

  # parse the service_config, suppose the instance_id is -1
  args.hbase_config.parse_generated_config_files(args)
  core_site_dict = args.hbase_config.configuration.generated_files["core-site.xml"]
  hdfs_site_dict = args.hbase_config.configuration.generated_files["hdfs-site.xml"]
  hbase_site_dict = args.hbase_config.configuration.generated_files["hbase-site.xml"]

  hbase_opts = list()
  for key, value in core_site_dict.iteritems():
    hbase_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))
  for key, value in hdfs_site_dict.iteritems():
    hbase_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))
  for key, value in hbase_site_dict.iteritems():
    hbase_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))

  if deploy_utils.is_security_enabled(args):
    hbase_opts.append("-Djava.security.krb5.conf=%s/krb5-hadoop.conf" %
        deploy_utils.get_config_dir())

    (jaas_fd, jaas_file) = tempfile.mkstemp()
    args.zookeeper_config.parse_generated_config_files(args)
    os.write(jaas_fd, deploy_zookeeper.generate_client_jaas_config(args))
    os.close(jaas_fd)
    hbase_opts.append("-Djava.security.auth.login.config=%s" % jaas_file)

  package_root = deploy_utils.get_artifact_package_root(args,
      args.hbase_config.cluster, "hbase")
  class_path = "%s/:%s/lib/*:%s/*" % (package_root, package_root, package_root)

  cmd = ["java", "-cp", class_path] + hbase_opts + [main_class]
  if args.command[0] == "shell":
    cmd += ["-X+O", "%s/bin/hirb.rb" % package_root]
  cmd += options
  p = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
  return p.wait()

def update_hbase_env_sh(args, artifact, version):
  current_path = os.path.abspath(os.path.dirname(
        os.path.realpath(args.package_root)))
  conf_path = "%s/%s/%s/%s-%s/conf" % (current_path, args.package_root,
    args.cluster, artifact, version)
  hbase_opts = "-Djava.security.auth.login.config=$HBASE_CONF_DIR/jaas.conf "
  hbase_opts += "-Djava.security.krb5.conf=$HBASE_CONF_DIR/krb5.conf"
  deploy_utils.append_to_file("%s/hbase-env.sh" % conf_path,
      'export HBASE_OPTS="$HBASE_OPTS %s"\n' % hbase_opts)

def generate_client_config(args, artifact, version):
  config_path = "%s/%s/%s-%s/conf" % (args.package_root,
      args.cluster, artifact, version)
  master_host = args.hbase_config.jobs["master"].hosts[0].ip
  config_path = "%s/%s/%s-%s/conf" % (args.package_root,
      args.cluster, artifact, version)
  deploy_utils.write_file("%s/hbase-site.xml" % config_path,
      deploy_utils.generate_site_xml(args,
        args.hbase_config.configuration.generated_files["hbase-site.xml"]))
  deploy_utils.write_file("%s/hadoop-metrics.properties" % config_path,
      generate_metrics_config(args, master_host, "master"))
  deploy_utils.write_file("%s/core-site.xml" % config_path,
      deploy_utils.generate_site_xml(args,
        args.hbase_config.configuration.generated_files["core-site.xml"]))
  deploy_utils.write_file("%s/hdfs-site.xml" % config_path,
      deploy_utils.generate_site_xml(args,
        args.hbase_config.configuration.generated_files["hdfs-site.xml"]))
  args.zookeeper_config.parse_generated_config_files(args)
  deploy_utils.write_file("%s/jaas.conf" % config_path,
      deploy_zookeeper.generate_client_jaas_config(args))
  deploy_utils.write_file("%s/krb5.conf" % config_path,
      args.hbase_config.configuration.raw_files["krb5.conf"])
  update_hbase_env_sh(args, artifact, version)

def pack(args):
  get_hbase_service_config(args)
  args.hbase_config.parse_generated_config_files(args)
  version = args.hbase_config.cluster.version
  deploy_utils.make_package_dir(args, "hbase", args.hbase_config.cluster)
  generate_client_config(args, "hbase", version)

  if not args.skip_tarball:
    deploy_utils.pack_package(args, "hbase", version)
  Log.print_success("Pack client utilities for hbase success!\n")

def vacate_region_server(args, ip, port):
  package_root = deploy_utils.get_artifact_package_root(args,
      args.hbase_config.cluster, "hbase")
  Log.print_info("Vacate region server: " + ip);
  try:
    host = socket.gethostbyaddr(ip)[0]
  except:
    host = ip
  args.command = ["ruby", "%s/bin/region_mover.rb" % package_root,
    "unload", "%s:%d" % (host, port)]
  if run_shell(args) != 0:
    Log.print_critical("Unload host %s failed." % host);

def recover_region_server(args, ip, port):
  package_root = deploy_utils.get_artifact_package_root(args,
      args.hbase_config.cluster, "hbase")
  Log.print_info("Recover region server: " + ip);
  try:
    host = socket.gethostbyaddr(ip)[0]
  except:
    host = ip
  args.command = ["ruby", "%s/bin/region_mover.rb" % package_root,
    "load", "%s:%d" % (host, port)]
  if run_shell(args) != 0:
    Log.print_critical("Load host %s failed." % host);

def balance_switch(args, flag):
  fd, filename = tempfile.mkstemp()
  f = os.fdopen(fd, 'w+')
  if flag:
    Log.print_info("balance_switch on for cluster: %s" % args.cluster)
    print >> f, 'balance_switch true'
  else:
    Log.print_info("balance_switch off for cluster: %s" % args.cluster)
    print >> f, 'balance_switch false'
  print >> f, 'exit'
  f.close()
  args.command = ["shell", filename]
  ret = run_shell(args)
  os.remove(filename)
  if ret != 0:
    Log.print_critical("balance_switch off for cluster: %s failed!" %
        args.cluster);

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  get_hbase_service_config(args)
  job_name = args.job[0]

  if job_name != 'regionserver':
    args.vacate_rs = False

  if args.vacate_rs:
    balance_switch(args, False)

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.hbase_config.jobs[job_name].hosts
  wait_time = 0

  args.task_map = deploy_utils.parse_args_host_and_task(args, hosts)
  for host_id in args.task_map.keys() or hosts.iterkeys():
    for instance_id in args.task_map.get(host_id) or range(hosts[host_id].instance_num):
      instance_id = -1 if not deploy_utils.is_multiple_instances(host_id, hosts) else instance_id
      if not args.skip_confirm:
        deploy_utils.confirm_rolling_update(host_id, instance_id, wait_time)

      port = deploy_utils.get_base_port(
        args.hbase_config.jobs[job_name].base_port, instance_id)
      if args.vacate_rs:
        vacate_region_server(args, hosts[host_id].ip, port)

      stop_job(args, hosts[host_id].ip, job_name, instance_id)
      deploy_utils.wait_for_job_stopping("hbase",
        args.hbase_config.cluster.name, job_name, hosts[host_id].ip, instance_id)
      start_job(args, hosts[host_id].ip, job_name, host_id, instance_id)
      deploy_utils.wait_for_job_starting("hbase",
        args.hbase_config.cluster.name, job_name, hosts[host_id].ip, instance_id)

      if args.vacate_rs:
        recover_region_server(args, hosts[host_id].ip, port)
      wait_time = args.time_interval

  if args.vacate_rs:
    balance_switch(args, True)
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
