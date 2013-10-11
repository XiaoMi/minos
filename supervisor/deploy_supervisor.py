#!/usr/bin/env python
#

import ConfigParser
import os
import pexpect
import sys

def scp(host, user, passwd, local_file, remote_file):
  child = pexpect.spawn('scp -r %s %s@%s:%s' % (local_file,
        user, host, remote_file))
  print child.args

  ret = child.expect(['yes/no.*', 'password.*', pexpect.EOF,
      pexpect.TIMEOUT], timeout=30)
  if ret == 0:
    child.sendline('yes')
    child.expect('password.*', timeout=30)
    child.sendline(passwd)
    child.expect(pexpect.EOF)
  elif ret == 1:
    child.sendline(passwd)
    child.expect(pexpect.EOF)

def remote_exec(host, user, passwd, cmd):
  child = pexpect.spawn('ssh %s@%s "%s"' % (user, host, cmd))
  print child.args

  ret = child.expect(['yes/no.*', 'password.*', pexpect.EOF,
      pexpect.TIMEOUT], timeout=30)
  if ret == 0:
    child.sendline('yes')
    child.expect('password.*', timeout=30)
    child.sendline(passwd)
    child.expect(pexpect.EOF)
  elif ret == 1:
    child.sendline(passwd)
    child.expect(pexpect.EOF)

class Config:
  class NodeConfig:
    def __init__(self, config_dict):
      self.password = str()
      self.hosts = dict()
      for key, value in config_dict.iteritems():
        if key.startswith('host.'):
          self.hosts.update({key.split('.')[1]: value})
        else:
          setattr(self, key, value)

  def __init__(self, config_file):
    self.config = ConfigParser.SafeConfigParser()
    self.config.read([config_file])
    self.groups = set()

  def parse(self):
    for section in self.config.sections():
      config_dict = dict()
      for option in self.config.options(section):
        value = self.config.get(section, option)
        config_dict.update({option: value})
      node_config = Config.NodeConfig(config_dict)
      self.groups.add(section)
      setattr(self, section, node_config)

def generate_supervisor_config(run_dir, config, file):
  parser = ConfigParser.SafeConfigParser()
  parser.read([file])
  parser.set('rpcinterface:deployment', 'data_dirs', config.data_dirs)
  parser.write(open('%s/%s.tmp' % (run_dir, os.path.basename(file)), 'w'))

def deploy(supervisor_config, config):
  run_dir = os.path.dirname(sys.argv[0])
  generate_supervisor_config(run_dir, config, supervisor_config)

  for host in config.hosts.itervalues():
    user = config.user
    password = config.password
    dest_path = '%s/supervisor/' % config.root_dir
    remote_exec(host, user, password,
        'cd %s; mkdir -p supervisor' % config.root_dir)
    scp(host, user, password, '%s/conf' % run_dir, dest_path)
    scp(host, user, password, '%s/deployment' % run_dir, dest_path)
    scp(host, user, password, '%s/metrics' % run_dir, dest_path)
    scp(host, user, password, '%s/superlance' % run_dir, dest_path)
    scp(host, user, password, '%s/supervisor' % run_dir, dest_path)
    scp(host, user, password, '%s/start_supervisor.sh' % run_dir, dest_path)
    scp(host, user, password, '%s/stop_supervisor.sh' % run_dir, dest_path)
    scp(host, user, password, '%s/supervisorctl.py' % run_dir, dest_path)
    scp(host, user, password, '%s/supervisord.py' % run_dir, dest_path)
    scp(host, user, password, '%s/%s.tmp' % (run_dir,
          os.path.basename(supervisor_config)),
        '%s/supervisord.conf' % dest_path)
    remote_exec(host, user, password,
        'cd %s/supervisor; ./start_supervisor.sh' % config.root_dir)

def main(supervisor_config, deploy_config):
  config = Config(deploy_config)
  config.parse()
  for group in config.groups:
    deploy(supervisor_config, getattr(config, group))

if __name__ == '__main__':
  sys.path.append('%s/../client' % os.path.dirname(__file__))
  from deploy import deploy_utils
  supervisor_config = '%s/supervisord.conf' % deploy_utils.get_config_dir()
  deploy_config = '%s/deploy_supervisor.cfg' % deploy_utils.get_config_dir()
  main(supervisor_config, deploy_config)
