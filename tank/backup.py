import argparse
import os
import subprocess
import time

def parse_command_line():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description='A utility used to backup tank data')

  parser.add_argument('--hadoop_home', default=os.getcwd(),
      help='The local hadoop home directory')

  parser.add_argument('--cluster', default='lgprc-xiaomi',
      help='The hadoop cluster name')

  parser.add_argument('--backup_root', default='/user/h_tank',
      help='The backup root directory')

  parser.add_argument('--tank_home', default=os.getcwd(),
      help='The tank home directory')

  args = parser.parse_args()
  return args

def backup_sqlite(args):
  cmd = ['%s/bin/hdfs' % args.hadoop_home, 'dfs', '-mkdir',
    '-p', '%s/sqlite/' % args.backup_root]
  print cmd
  subprocess.check_call(cmd)

  cmd = ['%s/bin/hdfs' % args.hadoop_home, 'dfs', '-copyFromLocal',
    '%s/sqlite/tank.db' % args.tank_home,
    '%s/sqlite/tank.db.%d' % (args.backup_root, int(time.time()))]
  print cmd
  subprocess.check_call(cmd)

def backup_data(args):
  for dir in os.listdir('%s/data' % args.tank_home):
    if dir.startswith('.'):
      continue

    cmd = ['%s/bin/hdfs' % args.hadoop_home, 'dfs', '-mkdir',
      '-p', '%s/data/%s' % (args.backup_root, dir)]
    print cmd
    subprocess.check_call(cmd)

    tag_file = '%s/data/%s/tags' % (args.tank_home, dir)
    fp = open(tag_file, 'a+')
    print tag_file
    backed_dirs = [d.strip() for d in fp.readlines()]
    total_dirs = [d for d in os.listdir(
        '%s/data/%s' % (args.tank_home, dir)) if not d.startswith('.')]
    diff_dirs = list(set(total_dirs) - set(backed_dirs) - set(['tags']))

    for d in diff_dirs:
      # only backup package whose modification time is older than 30min
      mod_time = os.path.getmtime('%s/data/%s/%s' % (
            args.tank_home, dir, d))
      if time.time() - mod_time < 1800:
        continue

      cmd = ['%s/bin/hdfs' % args.hadoop_home, 'dfs', '-copyFromLocal',
        '%s/data/%s/%s' % (args.tank_home, dir, d),
        '%s/data/%s/' % (args.backup_root, dir)]
      print cmd
      subprocess.check_call(cmd)
      fp.write('%s\n' % d)

def main():
  args = parse_command_line()
  backup_sqlite(args)
  backup_data(args)

if __name__ == '__main__':
  main()
