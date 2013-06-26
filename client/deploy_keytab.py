#!/usr/bin/env python
#

import argparse
import csv
import os
import pexpect
import sys

HADOOP_CONF_PATH = '/etc/hadoop/conf'

def parse_command_line():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description='Keytab deploy tool')

  parser.add_argument('cluster_type', type=str,
      choices=['srv', 'prc', 'tst'], help='The cluster type')

  parser.add_argument('--host_file', type=str, default='hosts',
      help='The host file list in csv format')

  parser.add_argument('--keytab_dir', type=str, default='keytab',
      help='The keytab file directory')

  parser.add_argument('--prod_user', type=str, default='work',
      help='The production enviroment user')

  parser.add_argument('--root_password', type=str,
      help='The root password of the production enviroment')

  args = parser.parse_args()
  return args

def check_args(args):
  if not os.path.exists(args.host_file):
    print 'Invalid host_file: %s' % args.host_file
    sys.exit(-4)

  if not os.path.exists(args.keytab_dir):
    print 'Invalid keytab_dir: %s' % args.keytab_dir
    sys.exit(-5)

def parse_host_file(host_file):
  file = open(host_file, 'r')
  csv_reader = csv.reader(file, delimiter=' ', skipinitialspace=True)

  host_list = list()
  for line in csv_reader:
    if line[0].lstrip().startswith('#'):
      continue
    host_list.append(line)
  file.close()
  return host_list

def scp(host, user, passwd, local_file, remote_file):
  child = pexpect.spawn('scp %s %s@%s:%s' % (local_file,
        user, host, remote_file))
  print child.args

  ret = child.expect(['yes/no.*', 'password.*', pexpect.EOF])
  if ret == 0:
    child.sendline('yes')
    child.expect('password.*', timeout=10)
    child.sendline(passwd)
  elif ret == 1:
    child.sendline(passwd)
  else:
    print 'Error occured when execute expect()'
    sys.exit(-2)

  return child.expect([pexpect.EOF, pexpect.TIMEOUT])

def remote_exec(host, user, passwd, cmd):
  child = pexpect.spawn('ssh %s@%s "%s"' % (user, host, cmd))
  print child.args

  ret = child.expect(['yes/no.*', 'password.*', pexpect.EOF], timeout=30)
  if ret == 0:
    child.sendline('yes')
    child.expect('password.*', timeout=10)
    child.sendline(passwd)
  elif ret == 1:
    child.sendline(passwd)
  else:
    print 'Error occured when execute expect()'
    sys.exit(-3)

  return child.expect([pexpect.EOF, pexpect.TIMEOUT])

def deploy(args, host):
  # mkdir -p HADOOP_CONF_PATH
  remote_exec(host, 'root', args.root_password,
      'mkdir -p %s' % HADOOP_CONF_PATH)

  keytabs = [
    'hdfs_%s.keytab' % args.cluster_type,
    'hbase_%s.keytab' % args.cluster_type,
    'yarn_%s.keytab' % args.cluster_type,
    'zookeeper.keytab',
    'impala.keytab',
  ]

  for keytab in keytabs:
    # scp keytab to HADOOP_CONF_PATH
    scp(host, 'root', args.root_password,
        '%s/%s' % (args.keytab_dir, keytab), HADOOP_CONF_PATH)

    # chown of keytab to prod_user:prod_user
    remote_exec(host, 'root', args.root_password,
        '"chown %s:%s %s/%s"' % (args.prod_user, args.prod_user,
          HADOOP_CONF_PATH, keytab))

    # chmod of keytab to 400
    remote_exec(host, 'root', args.root_password,
        '"chmod 400 %s/%s"' % (HADOOP_CONF_PATH, keytab))

  print '\033[0;32mDeploy keytab on %s successfully\033[0m' % host

def main():
  args = parse_command_line()
  check_args(args)

  host_list = parse_host_file(args.host_file)
  for host_info in host_list:
    deploy(args, host_info[0])

if __name__ == '__main__':
  main()
