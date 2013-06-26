#!/usr/bin/env python

import argparse
import subprocess
import sys
import urlparse

import deploy_utils
import deploy_hdfs

from deploy_utils import Log


ALL_JOBS = ["resourcemanager", "nodemanager", "historyserver", "proxyserver"]

RESOURCEMANAGER_JOB_SCHEMA = {
  # "param_name": (type, default_value)
  # type must be in {bool, int, float, str}
  # if default_value is None, it means it's NOT an optional parameter.
  "hdfs_root": (str, None),
}

NODEMANAGER_JOB_SCHEMA = {
}

HISTORYSERVER_JOB_SCHEMA = {
}

PROXYSERVER_JOB_SCHEMA = {
}

YARN_SERVICE_MAP = {
  "resourcemanager": RESOURCEMANAGER_JOB_SCHEMA,
  "nodemanager": NODEMANAGER_JOB_SCHEMA,
  "historyserver": HISTORYSERVER_JOB_SCHEMA,
  "proxyserver": PROXYSERVER_JOB_SCHEMA,
}

JOB_MAIN_CLASS = {
  "resourcemanager":
    "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager",
  "nodemanager": "org.apache.hadoop.yarn.server.nodemanager.NodeManager",
  "historyserver": "org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer",
  "proxyserver": "org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer",
}

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
  args.yarn_config = deploy_utils.get_service_config_full(
      args, YARN_SERVICE_MAP)
  if not args.yarn_config.cluster.zk_cluster:
    Log.print_critical(
        "yarn cluster must depends on a zookeeper clusters: %s" %
        args.yarn_config.cluster.name)

  hdfs_root = args.yarn_config.jobs["resourcemanager"].hdfs_root
  url = urlparse.urlparse(hdfs_root)
  if url.scheme != "hdfs":
    Log.print_critical(
        "Only hdfs supported as data root: %s" % hdfs_root)
  args.yarn_config.jobs["resourcemanager"].hdfs_root = hdfs_root.rstrip("/")

  hdfs_args = argparse.Namespace()
  hdfs_args.root = deploy_utils.get_root_dir("hdfs")
  hdfs_args.service = "hdfs"
  hdfs_args.cluster = url.netloc

  args.hdfs_config = deploy_utils.get_service_config(
      hdfs_args, deploy_hdfs.HDFS_SERVICE_MAP)

def generate_mapred_site_dict(args, host, job_name):
  zk_job = args.zk_config.jobs["zookeeper"]
  zk_hosts = ",".join(zk_job.hosts.itervalues())

  resourcemanager_job = args.yarn_config.jobs["resourcemanager"]
  nodemanager_job = args.yarn_config.jobs["nodemanager"]

  cluster_name = args.yarn_config.cluster.name
  config_dict = {
    "mapreduce.framework.name": "yarn",
    "mapreduce.jobhistory.address": "0.0.0.0:%d" % (
        args.yarn_config.jobs["historyserver"].base_port + 0),
    "mapreduce.jobhistory.webapp.address": "0.0.0.0:%d" % (
        args.yarn_config.jobs["historyserver"].base_port + 1),
    "yarn.app.mapreduce.am.staging-dir": "/tmp/hadoop-yarn/staging",
    "mapreduce.shuffle.port": args.yarn_config.jobs["nodemanager"].base_port + 8,
  }

  if host:
    supervisor_client = deploy_utils.get_supervisor_client(host,
        "yarn", args.yarn_config.cluster.name, job_name)
    package_dir = supervisor_client.get_current_package_dir()
    config_dict.update({
        "mapreduce.admin.user.env":
          "LD_LIBRARY_PATH=%s/lib/native:/usr/lib" % package_dir,
    })

  username = args.yarn_config.cluster.kerberos_username
  if args.yarn_config.cluster.enable_security:
    config_dict.update({
        "mapreduce.jobhistory.keytab": "%s/%s.keytab" % (
          deploy_utils.HADOOP_CONF_PATH, username),
        "mapreduce.jobhistory.principal": "%s/hadoop@%s" % (
          args.yarn_config.cluster.kerberos_username or "yarn",
          args.yarn_config.cluster.kerberos_realm),
    })
  return config_dict

def generate_mapred_site_xml(args, host, job_name):
  config_dict = generate_mapred_site_dict(args, host, job_name)
  local_path = "%s/site.xml.tmpl" % deploy_utils.get_template_dir()
  return deploy_utils.generate_site_xml(args, local_path, config_dict)

def generate_yarn_site_dict(args, host, job_name):
  zk_job = args.zk_config.jobs["zookeeper"]
  zk_hosts = ",".join(zk_job.hosts.itervalues())

  resourcemanager_job = args.yarn_config.jobs["resourcemanager"]
  nodemanager_job = args.yarn_config.jobs["nodemanager"]
  proxyserver_job = args.yarn_config.jobs["proxyserver"]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "yarn", args.yarn_config.cluster.name, job_name)

  cluster_name = args.yarn_config.cluster.name
  package_path = supervisor_client.get_current_package_dir()
  class_path_root = "%s/share/hadoop" % package_path
  jar_dirs = ""
  for component in ["common", "mapreduce", "yarn", "hdfs"]:
    if jar_dirs: jar_dirs += ":"
    component_dir = ("%s/%s" % (class_path_root, component))
    jar_dirs += "%s/:%s/lib/*:%s/*" % (
        component_dir, component_dir, component_dir)

  config_dict = {}
  config_dict.update(args.yarn_config.cluster.site_xml)
  config_dict.update({
    # global config
    "yarn.log-aggregation-enable": "true",

    # config resouremanager
    "yarn.resourcemanager.address": "%s:%d" % (resourcemanager_job.hosts[0],
        resourcemanager_job.base_port + 0),
    "yarn.resourcemanager.webapp.address": "%s:%d" % (
        resourcemanager_job.hosts[0], resourcemanager_job.base_port + 1),
    "yarn.resourcemanager.scheduler.address": "%s:%d" % (
        resourcemanager_job.hosts[0], resourcemanager_job.base_port + 2),
    "yarn.resourcemanager.resource-tracker.address": "%s:%d" % (
        resourcemanager_job.hosts[0], resourcemanager_job.base_port + 3),
    "yarn.resourcemanager.admin.address": "%s:%d" % (
        resourcemanager_job.hosts[0], resourcemanager_job.base_port + 4),

    # config nodemanager
    "yarn.nodemanager.aux-services": "mapreduce.shuffle",
    "yarn.nodemanager.aux-services.mapreduce.shuffle.class":
      "org.apache.hadoop.mapred.ShuffleHandler",
    "yarn.nodemanager.remote-app-log-dir": "/var/log/hadoop-yarn/apps",
    "yarn.nodemanager.address": "0.0.0.0:%d" % (
        nodemanager_job.base_port + 0),
    "yarn.nodemanager.webapp.address": "0.0.0.0:%d" % (
        nodemanager_job.base_port + 1),
    "yarn.nodemanager.localizer.address": "0.0.0.0:%d" % (
        nodemanager_job.base_port + 2),
    "yarn.nodemanager.vmem-pmem-ratio": 10,
    "yarn.nodemanager.log.retain-seconds": 86400,

    # config class_path
    "yarn.application.classpath": jar_dirs,

    # config proxy server
    "yarn.web-proxy.address": "%s:%d" % (proxyserver_job.hosts[0],
        proxyserver_job.base_port + 1),
  })

  if job_name == "nodemanager":
    data_dirs = ",".join(supervisor_client.get_available_data_dirs())
    config_dict.update({
      "yarn.nodemanager.local-dirs": data_dirs,
      "yarn.nodemanager.log-dirs": supervisor_client.get_log_dir(),
    })
  elif job_name == "resourcemanager":
    run_dir = supervisor_client.get_run_dir()
    config_dict.update({
      "yarn.resourcemanager.nodes.exclude-path": "%s/excludes" % run_dir,
    })

  username = args.yarn_config.cluster.kerberos_username
  if args.yarn_config.cluster.enable_security:
    config_dict.update({
        "yarn.resourcemanager.keytab": "%s/%s.keytab" % (
          deploy_utils.HADOOP_CONF_PATH, username),
        "yarn.resourcemanager.principal": "%s/hadoop@%s" % (
          args.yarn_config.cluster.kerberos_username or "yarn",
          args.yarn_config.cluster.kerberos_realm),
        "yarn.nodemanager.keytab": "%s/%s.keytab" % (
          deploy_utils.HADOOP_CONF_PATH, username),
        "yarn.nodemanager.principal": "%s/hadoop@%s" % (
          args.yarn_config.cluster.kerberos_username or "yarn",
          args.yarn_config.cluster.kerberos_realm),
        "yarn.web-proxy.keytab": "%s/%s.keytab" % (
          deploy_utils.HADOOP_CONF_PATH, username),
        "yarn.web-proxy.principal": "%s/hadoop@%s" % (
          args.yarn_config.cluster.kerberos_username or "yarn",
          args.yarn_config.cluster.kerberos_realm),
    })
  return config_dict

def generate_yarn_site_xml(args, host, job_name):
  config_dict = generate_yarn_site_dict(args, host, job_name)
  local_path = "%s/site.xml.tmpl" % deploy_utils.get_template_dir()
  return deploy_utils.generate_site_xml(args, local_path, config_dict)

def generate_metrics_config(args, host, job_name):
  job = args.yarn_config.jobs[job_name]
  supervisor_client = deploy_utils.get_supervisor_client(host, "yarn",
      args.yarn_config.cluster.name, job_name)

  ganglia_switch = "# "
  if args.yarn_config.cluster.ganglia_address:
    ganglia_switch = ""
  config_dict = {
      "job_name": job_name,
      "period": job.metrics_period,
      "data_dir": supervisor_client.get_log_dir(),
      "ganglia_address": args.yarn_config.cluster.ganglia_address,
      "ganglia_switch": ganglia_switch,
  }

  local_path = "%s/hadoop-metrics2.properties.tmpl" % deploy_utils.get_template_dir()
  template = deploy_utils.Template(open(local_path, "r").read())
  return template.substitute(config_dict)

def generate_configs(args, host, job_name):
  job = args.yarn_config.jobs[job_name]
  core_site_xml = deploy_hdfs.generate_core_site_xml(args, job_name,
      "yarn", args.yarn_config.cluster.enable_security, job)
  hdfs_site_xml = deploy_hdfs.generate_hdfs_site_xml_client(args)
  mapred_site_xml = generate_mapred_site_xml(args, host, job_name)
  yarn_site_xml = generate_yarn_site_xml(args, host, job_name)
  hadoop_metrics2_properties = generate_metrics_config(args, host, job_name)
  configuration_xsl = open("%s/configuration.xsl" % deploy_utils.get_template_dir()).read()
  log4j_xml = open("%s/yarn/log4j.xml" % deploy_utils.get_template_dir()).read()
  krb5_conf = open("%s/krb5-hadoop.conf" % deploy_utils.get_config_dir()).read()
  excludes = str()

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "mapred-site.xml": mapred_site_xml,
    "yarn-site.xml": yarn_site_xml,
    "hadoop-metrics2.properties": hadoop_metrics2_properties,
    "configuration.xsl": configuration_xsl,
    "log4j.xml": log4j_xml,
    "krb5.conf": krb5_conf,
    "excludes": excludes,
  }
  return config_files

def generate_run_scripts_params(args, host, job_name):
  job = args.yarn_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "yarn", args.yarn_config.cluster.name, job_name)

  artifact_and_version = "hadoop-" + args.yarn_config.cluster.version

  jar_dirs = ""
  for component in ["common", "mapreduce", "yarn", "hdfs"]:
    if jar_dirs: jar_dirs += ":"
    component_dir = ("$package_dir/share/hadoop/%s" % component)
    jar_dirs += "%s/:%s/lib/*:%s/*" % (
        component_dir, component_dir, component_dir)

  script_dict = {
      "artifact": artifact_and_version,
      "job_name": job_name,
      "jar_dirs": jar_dirs,
      "run_dir": supervisor_client.get_run_dir(),
      "params":
          '-Xmx%dm ' % job.xmx +
          '-Xms%dm ' % job.xms +
          '-Xmn%dm ' % job.xmn +
          '-XX:MaxDirectMemorySize=%dm ' % job.max_direct_memory +
          '-XX:MaxPermSize=%dm ' % job.max_perm_size +
          '-XX:+DisableExplicitGC ' +
          '-XX:+HeapDumpOnOutOfMemoryError ' +
          '-XX:HeapDumpPath=$log_dir ' +
          '-XX:+PrintGCApplicationStoppedTime ' +
          '-XX:+UseConcMarkSweepGC ' +
          '-XX:CMSInitiatingOccupancyFraction=80 ' +
          '-XX:+UseMembar ' +
          '-verbose:gc ' +
          '-XX:+PrintGCDetails ' +
          '-XX:+PrintGCDateStamps ' +
          '-Xloggc:$log_dir/%s_gc_${start_time}.log ' % job_name +
          '-Dproc_%s ' % job_name +
          '-Djava.net.preferIPv4Stack=true ' +
          '-Dyarn.log.dir=$log_dir ' +
          '-Dyarn.pid=$pid ' +
          '-Dyarn.cluster=%s ' % args.yarn_config.cluster.name +
          '-Dhadoop.policy.file=hadoop-policy.xml ' +
          '-Dhadoop.home.dir=$package_dir ' +
          '-Dhadoop.id.str=%s ' % args.remote_user +
          '-Djava.security.krb5.conf=$run_dir/krb5.conf ' +
          get_job_specific_params(args, job_name)
  }

  if args.yarn_config.cluster.enable_security:
    class_path_root = "$package_dir/share/hadoop"
    boot_class_path = ("%s/common/lib/hadoop-security-%s.jar" % (
          class_path_root, args.hdfs_config.cluster.version))
    script_dict["params"] += "-Xbootclasspath/p:%s " % boot_class_path

  script_dict["params"] += JOB_MAIN_CLASS[job_name]
  return script_dict

def get_job_specific_params(args, job_name):
  return ""

def generate_start_script(args, host, job_name):
  script_params = generate_run_scripts_params(args, host, job_name)
  return deploy_utils.create_run_script(
      "%s/start.sh.tmpl" % deploy_utils.get_template_dir(),
      script_params)

def install(args):
  get_yarn_service_config(args)
  deploy_utils.install_service(args, "yarn", args.yarn_config, "hadoop")

def cleanup(args):
  get_yarn_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "yarn", args.yarn_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.cleanup_job("yarn", args.yarn_config,
          hosts[id], job_name, cleanup_token)

def bootstrap_job(args, host, job_name, cleanup_token):
  deploy_utils.bootstrap_job(args, "hadoop", "yarn",
      args.yarn_config, host, job_name, cleanup_token, '0')
  start_job(args, host, job_name)

def bootstrap(args):
  get_yarn_service_config(args)

  cleanup_token = deploy_utils.confirm_bootstrap("yarn", args.yarn_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      bootstrap_job(args, hosts[id], job_name, cleanup_token)

def start_job(args, host, job_name):
  config_files = generate_configs(args, host, job_name)
  start_script = generate_start_script(args, host, job_name)
  http_url = 'http://%s:%d' % (host,
    args.yarn_config.jobs[job_name].base_port + 1)
  deploy_utils.start_job(args, "hadoop", "yarn", args.yarn_config,
      host, job_name, start_script, http_url, **config_files)

def start(args):
  deploy_utils.confirm_start(args)
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      start_job(args, hosts[id], job_name)

def stop_job(args, host, job_name):
  deploy_utils.stop_job("yarn", args.yarn_config,
      host, job_name)

def stop(args):
  deploy_utils.confirm_stop(args)
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      stop_job(args, hosts[id], job_name)

def restart(args):
  deploy_utils.confirm_restart(args)
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      stop_job(args, hosts[id], job_name)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.wait_for_job_stopping("yarn",
          args.yarn_config.cluster.name, job_name, hosts[id])
      start_job(args, hosts[id], job_name)

def show(args):
  get_yarn_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.yarn_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.show_job("yarn", args.yarn_config,
          hosts[id], job_name)

def run_shell(args):
  get_yarn_service_config(args)

  main_class, options = deploy_utils.parse_shell_command(
      args, SHELL_COMMAND_INFO)
  if not main_class:
    return

  core_site_dict = deploy_hdfs.generate_core_site_dict(args,
      "namenode", "hdfs", args.hdfs_config.cluster.enable_security)
  hdfs_site_dict = deploy_hdfs.generate_hdfs_site_dict_client(args)
  mapred_site_dict = generate_mapred_site_dict(args,
      args.yarn_config.jobs["resourcemanager"].hosts[0], "resourcemanager")
  yarn_site_dict = generate_yarn_site_dict(args,
      args.yarn_config.jobs["resourcemanager"].hosts[0], "resourcemanager")

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

  if args.yarn_config.cluster.enable_security:
    hadoop_opts.append(
        "-Djava.security.krb5.conf=%s/krb5-hadoop.conf" %
        deploy_utils.get_config_dir())

  package_root = deploy_utils.get_hadoop_package_root(
      args.yarn_config.cluster.version)
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
      generate_mapred_site_xml(args,
        args.yarn_config.jobs["nodemanager"].hosts[0],
        "nodemanager"))
  deploy_utils.write_file("%s/yarn-site.xml" % config_path,
      generate_yarn_site_xml(args,
        args.yarn_config.jobs["nodemanager"].hosts[0],
        "nodemanager"))
  deploy_utils.write_file("%s/krb5.conf" % config_path,
      open('%s/krb5-hadoop.conf' % deploy_utils.get_config_dir()).read())
  deploy_hdfs.update_hadoop_env_sh(args, artifact, version, "YARN_OPTS")

def pack(args):
  get_yarn_service_config(args)
  version = args.yarn_config.cluster.version
  deploy_utils.make_package_dir(args, "hadoop", version)
  deploy_hdfs.generate_client_config(args, "hadoop", version)
  generate_client_config(args, "hadoop", version)

  if not args.skip_tarball:
    deploy_utils.pack_package(args, "hadoop", args.hdfs_config.cluster.version)
  Log.print_success("Pack client utilities for hadoop success!\n")

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  get_yarn_service_config(args)
  job_name = args.job[0]

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.yarn_config.jobs[job_name].hosts
  wait_time = 0
  for id in hosts.iterkeys():
    deploy_utils.confirm_rolling_update(id, wait_time)
    stop_job(args, hosts[id], job_name)
    deploy_utils.wait_for_job_stopping("yarn",
        args.yarn_config.cluster.name, job_name, hosts[id])
    start_job(args, hosts[id], job_name)
    wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
