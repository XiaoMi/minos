#!/usr/bin/env python

import subprocess
import sys
import time

import deploy_utils

from deploy_utils import Log


ALL_JOBS = ["journalnode", "zkfc", "namenode", "datanode"]

JOURNALNODE_JOB_SCHEMA = {
    # "param_name": (type, default_value)
    # type must be in {bool, int, float, str}
    # if default_value is None, it means it's NOT an optional parameter.
}

NAMENODE_JOB_SCHEMA = {
}

ZKFC_JOB_SCHEMA = {
}

DATANODE_JOB_SCHEMA = {
}

HDFS_SERVICE_MAP = {
    "journalnode": JOURNALNODE_JOB_SCHEMA,
    "namenode": NAMENODE_JOB_SCHEMA,
    "zkfc": ZKFC_JOB_SCHEMA,
    "datanode": DATANODE_JOB_SCHEMA,
}

JOB_MAIN_CLASS = {
    "journalnode": "org.apache.hadoop.hdfs.qjournal.server.JournalNode",
    "namenode": "org.apache.hadoop.hdfs.server.namenode.NameNode",
    "zkfc": "org.apache.hadoop.hdfs.tools.DFSZKFailoverController",
    "datanode": "org.apache.hadoop.hdfs.server.datanode.DataNode",
}

SHELL_COMMAND_INFO = {
  "dfs": ("org.apache.hadoop.fs.FsShell",
      "run a filesystem command on the file systems supported in Hadoop"),
  "dfsadmin": ("org.apache.hadoop.hdfs.tools.DFSAdmin",
      "run a DFS admin client"),
  "haadmin": ("org.apache.hadoop.hdfs.tools.DFSHAAdmin",
      "run a DFS HA admin client"),
  "fsck": ("org.apache.hadoop.hdfs.tools.DFSck",
      "run a DFS filesystem checking utility"),
  "balancer": ("org.apache.hadoop.hdfs.server.balancer.Balancer",
      "run a cluster balancing utility"),
  "jmxget": ("org.apache.hadoop.hdfs.tools.JMXGet",
      "get JMX exported values from NameNode or DataNode"),
  "oiv": ("org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer",
      "apply the offline fsimage viewer to an fsimage"),
  "oev": ("org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsViewer",
      "apply the offline edits viewer to an edits file"),
  "fetchdt": ("org.apache.hadoop.hdfs.tools.DelegationTokenFetcher",
      "fetch a delegation token from the NameNode"),
  "getconf": ("org.apache.hadoop.hdfs.tools.GetConf",
      "get config values from configuration"),
  "groups": ("org.apache.hadoop.hdfs.tools.GetGroups",
      "get the groups which users belong to"),
}

def generate_core_site_dict(args, job_name, http_user,
    security_enabled=False, job=None):
  if not job:
    job = args.hdfs_config.jobs[job_name]

  zk_job = args.zk_config.jobs["zookeeper"]
  zk_hosts = ",".join(
      ["%s:%d" % (host, zk_job.base_port + 0)
          for id, host in zk_job.hosts.iteritems()])

  config_dict = {
      "fs.defaultFS": "hdfs://" + args.hdfs_config.cluster.name,
      "ha.zookeeper.quorum": zk_hosts,
      "hadoop.tmp.dir": "/tmp/hadoop",
      "io.file.buffer.size": 131072,
      "hadoop.http.staticuser.user": http_user,
  }

  # config security
  if security_enabled:
    config_dict.update({
      "hadoop.security.authentication": "kerberos",
      "hadoop.security.authorization": "true",
      "hadoop.security.use-weak-http-crypto": "false",
    })
  return config_dict

def generate_core_site_xml(args, job_name, http_user,
    security_enabled=False, job=None):
  config_dict = generate_core_site_dict(args, job_name,
      http_user, security_enabled, job)
  local_path = "%s/site.xml.tmpl" % deploy_utils.get_template_dir()
  return deploy_utils.generate_site_xml(args, local_path, config_dict)

def generate_hdfs_site_dict(args, job_host, job_name):
  cluster_name = args.hdfs_config.cluster.name

  job = args.hdfs_config.jobs["namenode"]
  namenode_hosts = ",".join(
      ["host%d" % id for id, host in job.hosts.iteritems()])

  job = args.hdfs_config.jobs["journalnode"]
  journalnode_hosts = ";".join(
      ["%s:%d" % (host, job.base_port + 0)
          for id, host in job.hosts.iteritems()])

  config_dict = {}
  config_dict.update(args.hdfs_config.cluster.site_xml)
  config_dict.update(args.hdfs_config.jobs[job_name].site_xml)
  config_dict.update({
      # config for client (mainly for namenode)
      "dfs.nameservices": cluster_name,
      "dfs.ha.namenodes." + cluster_name: namenode_hosts,

      "dfs.client.failover.proxy.provider." + cluster_name:
          "org.apache.hadoop.hdfs.server.namenode.ha."
          "ConfiguredFailoverProxyProvider",
  })

  job = args.hdfs_config.jobs["namenode"]
  for id, host in job.hosts.iteritems():
    config_dict["dfs.namenode.rpc-address.%s.host%d" % (cluster_name, id)] = (
        "%s:%d" % (host, job.base_port + 0))
    config_dict["dfs.namenode.http-address.%s.host%d" % (cluster_name, id)] = (
        "%s:%d" % (host, job.base_port + 1))

  config_dict.update({
      # config for journalnode:
      "dfs.journalnode.rpc-address": "0.0.0.0:%d" %
          (args.hdfs_config.jobs["journalnode"].base_port + 0),
      "dfs.journalnode.http-address": "0.0.0.0:%d" %
          (args.hdfs_config.jobs["journalnode"].base_port + 1),

      # config for namenode and zkfc:
      "dfs.namenode.shared.edits.dir":
          "qjournal://%s/%s" % (journalnode_hosts, cluster_name),

      "dfs.ha.zkfc.port": "%d" % (args.hdfs_config.jobs["zkfc"].base_port + 0),

      # TODO: make following options configurable, or finalize them.
      "dfs.ha.fencing.methods": "sshfence&#xA;shell(/bin/true)",
      "dfs.ha.fencing.ssh.private-key-files":
          "/home/%s/.ssh/id_rsa" % args.remote_user,

      "dfs.ha.fencing.ssh.connect-timeout": 2000,
      "dfs.ha.automatic-failover.enabled": "true",

      # config for datanode
      "dfs.block.local-path-access.user": "%s, hbase, hbase_srv, impala" % args.remote_user,

      "dfs.datanode.ipc.address": "0.0.0.0:%d" %
          (args.hdfs_config.jobs["datanode"].base_port + 0),
      "dfs.datanode.http.address": "0.0.0.0:%d" %
          (args.hdfs_config.jobs["datanode"].base_port + 1),
      "dfs.datanode.address": "0.0.0.0:%d" %
          (args.hdfs_config.jobs["datanode"].base_port + 2),

      # TODO: make following options configurable, or finalize them.
      "dfs.datanode.max.xcievers": 4096,
      "dfs.permissions": "false",
      "dfs.namenode.handler.count": 64,
      "dfs.block.size": "128m",
      # NOTE: comment this out because we need to format namenode.
      #"dfs.namenode.support.allow.format": "false",
      # NOTE: comment this out because if we have data dirs less than to equal
      # to 2, the datanode would fail to start.
      #"dfs.datanode.failed.volumes.tolerated": 2,
      "dfs.client.read.shortcircuit": "true",
      # Number of minutes between trash checkpoints.
      "fs.trash.interval": 10080, # 7 days
      # Number of minutes after which the checkpoint gets deleted.
      "fs.trash.checkpoint.interval": 1440, # 1 day
  })

  if job_host:
    supervisor_client = deploy_utils.get_supervisor_client(job_host,
        "hdfs", cluster_name, job_name)
    if job_name == "namenode":
      run_dir = supervisor_client.get_run_dir()
      config_dict.update({
          "dfs.namenode.name.dir":
              supervisor_client.get_available_data_dirs()[0],
          "net.topology.table.file.name": "%s/rackinfo.txt" % run_dir,
          "net.topology.node.switch.mapping.impl":
              "org.apache.hadoop.net.TableMapping",
          "dfs.hosts.exclude": "%s/excludes" % run_dir,
      })
    elif job_name == "datanode":
      datanode_dirs = ",".join(supervisor_client.get_available_data_dirs())
      config_dict.update({
          "dfs.datanode.data.dir": datanode_dirs,
      })
    elif job_name == "journalnode":
      config_dict.update({
          "dfs.journalnode.edits.dir":
              supervisor_client.get_available_data_dirs()[0],
      })

  # config dfs acl
  if args.hdfs_config.cluster.enable_acl:
    config_dict.update({
        "dfs.permissions": "true",
        "dfs.web.ugi": "hdfs,supergroup",
        "dfs.permissions.superusergroup": "supergroup",
        "dfs.permissions.superuser": "hdfs_admin",
        "dfs.namenode.upgrade.permission": "0777",
        "fs.permissions.umask-mode": "022",
        "dfs.cluster.administrators": "hdfs_admin",
        "hadoop.security.group.mapping":
          "org.apache.hadoop.security.SimpleGroupsMapping",
    })

  # config security
  username = args.hdfs_config.cluster.kerberos_username
  if args.hdfs_config.cluster.enable_security:
    config_dict.update({
      # general HDFS security config
      "dfs.block.access.token.enable": "true",
      "ignore.secure.ports.for.testing": "true",

      # namenode security config
      # TODO: make the realm name(here is for_hadoop) configurable
      "dfs.namenode.keytab.file": "%s/%s.keytab" % (
        deploy_utils.HADOOP_CONF_PATH, username),
      "dfs.namenode.kerberos.principal": "%s/hadoop@%s" % (
        args.hdfs_config.cluster.kerberos_username or "hdfs",
        args.hdfs_config.cluster.kerberos_realm),
      "dfs.namenode.kerberos.internal.spnego.principal":
        "HTTP/hadoop@%s" % args.hdfs_config.cluster.kerberos_realm,

      # secondary namenode security config
      "dfs.secondary.namenode.keytab.file": "%s/%s.keytab" % (
        deploy_utils.HADOOP_CONF_PATH, username),
      "dfs.secondary.namenode.kerberos.principal": "%s/hadoop@%s" % (
        args.hdfs_config.cluster.kerberos_username or "hdfs",
        args.hdfs_config.cluster.kerberos_realm),
      "dfs.secondary.namenode.kerberos.internal.spnego.principal":
        "HTTP/hadoop@%s" % args.hdfs_config.cluster.kerberos_realm,

      # datanode security config
      "dfs.datanode.data.dir.perm": "700",
      "dfs.datanode.keytab.file": "%s/%s.keytab" % (
          deploy_utils.HADOOP_CONF_PATH, username),
      "dfs.datanode.kerberos.principal": "%s/hadoop@%s" % (
          args.hdfs_config.cluster.kerberos_username or "hdfs",
          args.hdfs_config.cluster.kerberos_realm),

      # journalnode security config
      "dfs.journalnode.keytab.file": "%s/%s.keytab" % (
          deploy_utils.HADOOP_CONF_PATH, username),
      "dfs.journalnode.kerberos.principal": "%s/hadoop@%s" % (
          args.hdfs_config.cluster.kerberos_username or "hdfs",
          args.hdfs_config.cluster.kerberos_realm),
      "dfs.journalnode.kerberos.internal.spnego.principal":
        "HTTP/hadoop@%s" % args.hdfs_config.cluster.kerberos_realm,

      # config web
      "dfs.web.authentication.kerberos.principal":
        "HTTP/hadoop@%s" % args.hdfs_config.cluster.kerberos_realm,
      "dfs.web.authentication.kerberos.keytab":
        "%s/%s.keytab" % (deploy_utils.HADOOP_CONF_PATH, username),
    })
  return config_dict

def generate_hdfs_site_xml(args, job_host, job_name):
  config_dict = generate_hdfs_site_dict(args, job_host, job_name)
  local_path = "%s/site.xml.tmpl" % deploy_utils.get_template_dir()
  return deploy_utils.generate_site_xml(args, local_path, config_dict)

def generate_hdfs_site_dict_client(args):
  # We assume client use the same config as namenode:
  return generate_hdfs_site_dict(args, "", "namenode")

def generate_hdfs_site_xml_client(args):
  # We assume client use the same config as namenode:
  return generate_hdfs_site_xml(args,
      args.hdfs_config.jobs["namenode"].hosts[0],
      "namenode")

def generate_metrics_config(args, host, job_name):
  job = args.hdfs_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "hdfs", args.hdfs_config.cluster.name, job_name)

  ganglia_switch = "# "
  if args.hdfs_config.cluster.ganglia_address:
    ganglia_switch = ""
  config_dict = {
      "job_name": job_name,
      "period": job.metrics_period,
      "data_dir": supervisor_client.get_log_dir(),
      "ganglia_address": args.hdfs_config.cluster.ganglia_address,
      "ganglia_switch": ganglia_switch,
  }

  local_path = "%s/hadoop-metrics2.properties.tmpl" % deploy_utils.get_template_dir()
  template = deploy_utils.Template(open(local_path, "r").read())
  return template.substitute(config_dict)

def generate_configs(args, host, job_name):
  core_site_xml = generate_core_site_xml(args, job_name, "hdfs",
      args.hdfs_config.cluster.enable_security)
  hdfs_site_xml = generate_hdfs_site_xml(args, host, job_name)
  hadoop_metrics2_properties = generate_metrics_config(args, host, job_name)
  configuration_xsl = open("%s/configuration.xsl" % deploy_utils.get_template_dir()).read()
  log4j_xml = open("%s/hdfs/log4j.xml" % deploy_utils.get_template_dir()).read()
  rackinfo_txt = open("%s/rackinfo.txt" % deploy_utils.get_config_dir()).read()
  krb5_conf = open("%s/krb5-hadoop.conf" % deploy_utils.get_config_dir()).read()
  slaves = "\n".join(args.hdfs_config.jobs["datanode"].hosts.values())
  excludes = str()

  config_files = {
    "core-site.xml": core_site_xml,
    "hdfs-site.xml": hdfs_site_xml,
    "hadoop-metrics2.properties": hadoop_metrics2_properties,
    "configuration.xsl": configuration_xsl,
    "log4j.xml": log4j_xml,
    "krb5.conf": krb5_conf,
    "rackinfo.txt": rackinfo_txt,
    "slaves": slaves,
    "excludes": excludes,
  }
  return config_files

def get_job_specific_params(args, job_name):
  return ""

def generate_run_scripts_params(args, host, job_name):
  job = args.hdfs_config.jobs[job_name]

  supervisor_client = deploy_utils.get_supervisor_client(host,
      "hdfs", args.hdfs_config.cluster.name, job_name)

  artifact_and_version = "hadoop-" + args.hdfs_config.cluster.version

  jar_dirs = ""
  # must include both [dir]/ and [dir]/* as [dir]/* only import all jars under
  # this dir but we also need access the webapps under this dir.
  for component in ["common", "hdfs"]:
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
          '-Dhdfs.log.dir=$log_dir ' +
          '-Dhdfs.pid=$pid ' +
          '-Dhdfs.cluster=%s ' % args.hdfs_config.cluster.name +
          '-Dhadoop.policy.file=hadoop-policy.xml ' +
          '-Dhadoop.home.dir=$package_dir ' +
          '-Djava.security.krb5.conf=$run_dir/krb5.conf ' +
          '-Dhadoop.id.str=%s ' % args.remote_user,
  }

  # config security-related params
  if args.hdfs_config.cluster.enable_security:
    class_path_root = "$package_dir/share/hadoop/"
    boot_class_path = "%s/common/lib/hadoop-security-%s.jar" % (
      class_path_root, args.hdfs_config.cluster.version)
    script_dict["params"] += ('-Xbootclasspath/p:%s ' % boot_class_path +
      '-Dkerberos.instance=hadoop ')

  # finally, add the job's main class name
  script_dict["params"] += (get_job_specific_params(args, job_name) +
    JOB_MAIN_CLASS[job_name])
  return script_dict

def get_hdfs_service_config(args):
  args.hdfs_config = deploy_utils.get_service_config_full(
      args, HDFS_SERVICE_MAP)
  if not args.hdfs_config.cluster.zk_cluster:
    Log.print_critical(
        "hdfs cluster must depends on a zookeeper clusters: %s" %
        args.hdfs_config.cluster.name)

  namenode_hosts = args.hdfs_config.jobs["namenode"].hosts
  args.hdfs_config.jobs["zkfc"].hosts = namenode_hosts.copy()
  args.skip_gen_config_files = False

def generate_bootstrap_script(args, host, job_name, active):
  option = str()
  script_params = generate_run_scripts_params(args, host, job_name)
  script_params['ha_status'] = 'standby'
  if job_name == "zkfc":
    if active:
      option = "-formatZK"
      script_params['ha_status'] = 'active'
  elif job_name == "namenode":
    if active:
      option = "-format -nonInteractive"
    else:
      option = "-bootstrapStandby -skipSharedEditsCheck -nonInteractive"
  script_params['params'] += " %s" % option

  return deploy_utils.create_run_script(
      '%s/bootstrap_hdfs.sh.tmpl' % deploy_utils.get_template_dir(),
      script_params)

def generate_cleanup_script(args, host, job_name, active):
  script_params = generate_run_scripts_params(args, host, job_name)
  script_params['params'] += " -clearZK"
  if active:
    script_params['ha_status'] = 'active'
  else:
    script_params['ha_status'] = 'standby'
  return deploy_utils.create_run_script(
      '%s/cleanup_hdfs.sh.tmpl' % deploy_utils.get_template_dir(),
      script_params)

def generate_start_script(args, host, job_name):
  script_params = generate_run_scripts_params(args, host, job_name)
  return deploy_utils.create_run_script(
      '%s/start.sh.tmpl' % deploy_utils.get_template_dir(),
      script_params)

def check_journalnode_all_started(args):
  job = args.hdfs_config.jobs["journalnode"]
  hosts = job.hosts
  for id in hosts.iterkeys():
    if not deploy_utils.check_service(hosts[id], job.base_port):
      return False
  return True

def get_data_dir_indexes(args, job_name, host):
  if job_name != "datanode":
    return "0"
  else:
    supervisor_client = deploy_utils.get_supervisor_client(host,
        "hdfs", args.hdfs_config.cluster.name, job_name)
    data_dirs = supervisor_client.get_available_data_dirs()
    return ",".join([str(i) for i in range(len(data_dirs))])

def install(args):
  get_hdfs_service_config(args)
  deploy_utils.install_service(args, "hdfs", args.hdfs_config, "hadoop")

def cleanup_job(args, host, job_name, active, cleanup_token):
  cleanup_script = str()
  if job_name == "zkfc":
    cleanup_script = generate_cleanup_script(args, host, job_name, active)
  deploy_utils.cleanup_job("hdfs", args.hdfs_config,
      host, job_name, cleanup_token, cleanup_script)

def cleanup(args):
  get_hdfs_service_config(args)

  cleanup_token = deploy_utils.confirm_cleanup(args,
      "hdfs", args.hdfs_config)

  first = True
  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      cleanup_job(args, hosts[id], job_name, first, cleanup_token)
      if job_name == "zkfc":
        first = False

def bootstrap_job(args, host, job_name, active, cleanup_token):
  data_dir_indexes = get_data_dir_indexes(args, job_name, host)
  config_files = generate_configs(args, host, job_name)
  if job_name == "namenode" or job_name == "zkfc":
    bootstrap_script = generate_bootstrap_script(args, host, job_name, active)
    deploy_utils.bootstrap_job(args, "hadoop", "hdfs", args.hdfs_config,
        host, job_name, cleanup_token, data_dir_indexes, bootstrap_script,
        **config_files)
  else:
    deploy_utils.bootstrap_job(args, "hadoop", "hdfs", args.hdfs_config,
        host, job_name, cleanup_token, data_dir_indexes, '', **config_files)
  # start job after bootstrapping
  args.skip_gen_config_files = True
  start_job(args, host, job_name)

def bootstrap(args):
  get_hdfs_service_config(args)

  cleanup_token = deploy_utils.confirm_bootstrap("hdfs", args.hdfs_config)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    first = True
    if job_name == "namenode":
      while not check_journalnode_all_started(args):
        Log.print_warning("Wait for journalnode starting")
        time.sleep(2)

    for id in args.task or hosts.iterkeys():
      if job_name == "namenode" and not first:
        while not deploy_utils.check_service(hosts[0],
            args.hdfs_config.jobs["namenode"].base_port):
          Log.print_warning("Wait for active namenode starting")
          time.sleep(2)

      bootstrap_job(args, hosts[id], job_name, first, cleanup_token)
      first = False

def start_job(args, host, job_name):
  start_script = generate_start_script(args, host, job_name)
  http_url = 'http://%s:%d' % (host,
    args.hdfs_config.jobs[job_name].base_port + 1)
  config_files = dict()
  if not args.skip_gen_config_files:
    config_files = generate_configs(args, host, job_name)
  deploy_utils.start_job(args, "hadoop", "hdfs", args.hdfs_config,
      host, job_name, start_script, http_url, **config_files)

def start(args):
  deploy_utils.confirm_start(args)
  get_hdfs_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      start_job(args, hosts[id], job_name)

def stop_job(args, host, job_name):
  deploy_utils.stop_job("hdfs", args.hdfs_config,
      host, job_name)

def stop(args):
  deploy_utils.confirm_stop(args)
  get_hdfs_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      stop_job(args, hosts[id], job_name)

def restart(args):
  deploy_utils.confirm_restart(args)
  get_hdfs_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      stop_job(args, hosts[id], job_name)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.wait_for_job_stopping("hdfs",
          args.hdfs_config.cluster.name, job_name, hosts[id])
      start_job(args, hosts[id], job_name)

def show(args):
  get_hdfs_service_config(args)

  for job_name in args.job or ALL_JOBS:
    hosts = args.hdfs_config.jobs[job_name].hosts
    for id in args.task or hosts.iterkeys():
      deploy_utils.show_job("hdfs", args.hdfs_config,
          hosts[id], job_name)

def run_shell(args):
  get_hdfs_service_config(args)

  main_class, options = deploy_utils.parse_shell_command(
      args, SHELL_COMMAND_INFO)
  if not main_class:
    return

  core_site_dict = generate_core_site_dict(args, "namenode",
      "hdfs", args.hdfs_config.cluster.enable_security)
  hdfs_site_dict = generate_hdfs_site_dict_client(args)

  hadoop_opts = list()
  for key, value in core_site_dict.iteritems():
    hadoop_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))
  for key, value in hdfs_site_dict.iteritems():
    hadoop_opts.append("-D%s%s=%s" % (deploy_utils.HADOOP_PROPERTY_PREFIX,
          key, value))

  package_root = deploy_utils.get_hadoop_package_root(
      args.hdfs_config.cluster.version)
  lib_root = "%s/share/hadoop" % package_root
  class_path = "%s/etc/hadoop" % package_root
  for component in ["common", "hdfs"]:
    component_dir = "%s/%s" % (lib_root, component)
    class_path += ":%s/:%s/*:%s/lib/*" % (component_dir,
        component_dir, component_dir)

  if args.hdfs_config.cluster.enable_security:
    boot_class_path = "%s/common/lib/hadoop-security-%s.jar" % (lib_root,
        args.hdfs_config.cluster.version)
    hadoop_opts.append("-Xbootclasspath/p:%s" % boot_class_path)
    hadoop_opts.append("-Dkerberos.instance=hadoop")
    hadoop_opts.append(
        "-Djava.security.krb5.conf=%s/krb5-hadoop.conf" %
        deploy_utils.get_config_dir())

  cmd = (["java", "-cp", class_path] + hadoop_opts +
      [main_class] + options)
  p = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
  p.wait()

def generate_client_config(args, artifact, version):
  config_path = "%s/%s/%s-%s/etc/hadoop" % (args.package_root,
      args.cluster, artifact, version)
  deploy_utils.write_file("%s/core-site.xml" % config_path,
      generate_core_site_xml(args, "namenode", "hdfs",
        args.hdfs_config.cluster.enable_security))
  deploy_utils.write_file("%s/hdfs-site.xml" % config_path,
      generate_hdfs_site_xml_client(args))
  deploy_utils.write_file("%s/hadoop-metrics2.properties" % config_path,
      generate_metrics_config(args, args.hdfs_config.jobs["namenode"].hosts[0],
        "namenode"))
  deploy_utils.write_file("%s/krb5.conf" % config_path,
      open('%s/krb5-hadoop.conf' % deploy_utils.get_config_dir()).read())
  update_hadoop_env_sh(args, artifact, version, "HADOOP_OPTS")

def update_hadoop_env_sh(args, artifact, version, opts_name):
  config_path = "%s/%s/%s-%s/etc/hadoop" % (args.package_root,
      args.cluster, artifact, version)
  hadoop_opts = "-Djava.security.krb5.conf=$HADOOP_CONF_DIR/krb5.conf"
  deploy_utils.append_to_file("%s/hadoop-env.sh" % config_path,
      'export %s="$%s %s"\n' % (opts_name, opts_name, hadoop_opts))

def pack(args):
  get_hdfs_service_config(args)
  version = args.hdfs_config.cluster.version
  deploy_utils.make_package_dir(args, "hadoop", version)
  generate_client_config(args, "hadoop", version)

  if not args.skip_tarball:
    deploy_utils.pack_package(args, "hadoop", args.hdfs_config.cluster.version)
  Log.print_success("Pack client utilities for hadoop success!\n")

def rolling_update(args):
  if not args.job:
    Log.print_critical("You must specify the job name to do rolling update")

  get_hdfs_service_config(args)
  job_name = args.job[0]

  if not args.skip_confirm:
    deploy_utils.confirm_action(args, "rolling_update")

  Log.print_info("Rolling updating %s" % job_name)
  hosts = args.hdfs_config.jobs[job_name].hosts
  wait_time = 0
  for id in hosts.iterkeys():
    deploy_utils.confirm_rolling_update(id, wait_time)
    stop_job(args, hosts[id], job_name)
    deploy_utils.wait_for_job_stopping("hdfs",
        args.hdfs_config.cluster.name, job_name, hosts[id])
    start_job(args, hosts[id], job_name)
    deploy_utils.wait_for_job_starting("hdfs",
        args.hdfs_config.cluster.name, job_name, hosts[id])
    wait_time = args.time_interval
  Log.print_success("Rolling updating %s success" % job_name)

if __name__ == '__main__':
  test()
