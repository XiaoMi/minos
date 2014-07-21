import argparse
import glob
import hashlib
import os
import pprint
import subprocess
import yaml

import deploy_config

from log import Log
from tank_client import TankClient


def check_directory(path):
  if not os.path.exists(path):
    Log.print_critical(
        'Directory doesn''t exist: ' + path)
  if not os.path.isdir(path):
    Log.print_critical(
        'NOT a directory: ' + path)
  if not os.access(path, os.X_OK):
    Log.print_critical(
        'Can''t cd to: ' + path)

def check_file(path):
  if not os.path.exists(path):
    Log.print_critical(
        'File doesn''t exist: ' + path)
  if not os.path.isfile(path):
    Log.print_critical(
        'NOT a file: ' + path)
  if not os.access(path, os.R_OK):
    Log.print_critical(
        'Can''t read file: ' + path)

def get_package_config_dir():
  return deploy_config.get_deploy_config().get_config_dir() + '/package'

def get_package_config_file(package):
  return '%s/%s.yaml' % (get_package_config_dir(), package)

def get_pacakge_config(package):
  return yaml.load(open(get_package_config_file(package)))

def get_tank_client():
  '''
  A factory method to construct a tank(package server) client object.
  '''
  tank_config = deploy_config.get_deploy_config().get_tank_config()

  return TankClient(tank_config.get('server_host'),
      tank_config.get('server_port'))

def get_revision_number(cmd, output_prefix, work_space_dir):
  env = os.environ
  # Enforce English locale.
  env["LC_ALL"] = "C"
  current_work_dir = os.getcwd()
  os.chdir(work_space_dir)
  content = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
  os.chdir(current_work_dir)
  for line in content.splitlines():
    if line.startswith(output_prefix):
      return line[len(output_prefix):]

def generate_package_revision(root):
  '''
  Get the revision of the package. Currently, svn revision and git commit are
  supported. If the package directory is neither a svn working directory nor
  a git working directory, a fake revision will be returned.

  @param  root   the local package root directory
  @return string the revision of the package
  '''
  if os.path.islink(root):
    real_path = os.readlink(root)
    if not real_path.startswith('/'):
      abs_path = "%s/%s" % (os.path.dirname(root), real_path)
    else:
      abs_path = real_path
  else:
    abs_path = root

  try:
    try:
      cmd = ["svn", "info"]
      revision_prefix = "Revision: "
      return "r%s" % get_revision_number(cmd, revision_prefix, abs_path)
    except:
      cmd = ["git", "show"]
      commit_prefix = "commit "
      return get_revision_number(cmd, commit_prefix, abs_path)
  except:
    # We cannot get the version No., just return a fake one
    return "r%s" % FAKE_SVN_VERSION

def generate_checksum(path):
  '''
  Generate the SHA-1 digest of specified file.

  @param  path   the path of the file
  @return string the SHA-1 digest
  '''
  fd = open(path, "r")
  sha1 = hashlib.sha1()
  while True:
    buffer = fd.read(4096)
    if not buffer: break
    sha1.update(buffer)
  fd.close()
  return sha1.hexdigest()

def upload_package(artifact, package_tarball, package_source):
  '''
  Upload the specified package to the package server(Tank). Note that
  if the file with the same checksum is already uploaded, this uploading
  will be skipped.

  @param  artifact the artifact of the package
  @return dict     the package information return by the package server
  '''
  Log.print_info("Uploading pacakge: %s" % package_tarball)

  revision = generate_package_revision(package_source)
  Log.print_success("Revision is: %s" % revision)

  Log.print_info("Generating checksum of package: %s" % package_tarball)
  checksum = generate_checksum(package_tarball)
  Log.print_success("Checksum is: %s" % checksum)

  tank_client = get_tank_client()
  package_info = tank_client.check_package(artifact, checksum)

  if not package_info:
    if 200 == tank_client.upload(package_tarball, artifact, revision):
      Log.print_success("Upload package %s success" % package_tarball)
      package_info = tank_client.check_package(artifact, checksum)
      return eval(package_info)
  else:
    Log.print_warning("Package %s has already uploaded, skip uploading" %
        package_tarball)
    return eval(package_info)
  return None

def parse_command_line():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description='Manage Minos packages.')

  parser.add_argument('--version', action='version',
      version='%(prog)s 1.0.0-beta')
  parser.add_argument('-v', '--verbosity', default=0, type=int,
      help='The verbosity level of log, higher value, more details.')

  subparsers = parser.add_subparsers(
      title='commands',
      help='Type \'%(prog)s command -h\' to get more help for individual '
           'command.')

  sub_parser = subparsers.add_parser(
      'list',
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help='List packages, locally or remotely.')
  sub_parser.add_argument('--remote', action='store_true',
      help='List remote packages.')
  sub_parser.set_defaults(handler=process_command_list)

  sub_parser = subparsers.add_parser(
      'build',
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help='Build local package.')
  sub_parser.add_argument('package',
      help='The package name.')
  sub_parser.set_defaults(handler=process_command_build)

  sub_parser = subparsers.add_parser(
      'install',
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      help='Install binary packages from local to remote package server.')
  sub_parser.add_argument('--make_current', action='store_false',
      help='Make the installed pacakge as current version.')
  sub_parser.add_argument('package',
      help='The package name.')
  sub_parser.set_defaults(handler=process_command_install)

  args = parser.parse_args()
  Log.verbosity = args.verbosity
  return args

def process_command_list(args):
  if not args.remote:
    # list local packages.
    Log.print_info('All local packages:')
    print '[package]: [artifact] [version]'
    for path in glob.glob(get_package_config_file('*')):
      basename = os.path.basename(path)
      package = basename[:-len('.yaml')]
      package_config = get_pacakge_config(package)
      print '%s: %s %s' % (
          package, package_config['artifact'], package_config['version'])
  else:
    # list local packages.
    Log.print_critical('Not implemented yet!')

def process_command_build(args):
  package_file = get_package_config_file(args.package)
  package_config = get_pacakge_config(args.package)
  package_dir = os.path.dirname(package_file)

  package_source = os.path.abspath(
      os.path.join(package_dir, package_config['source']))
  check_directory(package_source)

  subprocess.check_call(
      'cd %s; %s' % (package_source, package_config['build']), shell=True)

def process_command_install(args):
  package_file = get_package_config_file(args.package)
  package_config = get_pacakge_config(args.package)
  package_dir = os.path.dirname(package_file)

  package_source = os.path.abspath(
      os.path.join(package_dir, package_config['source']))
  package_tarball = os.path.abspath(
      os.path.join(package_source, package_config['package']['tarball']))
  # the abspath would remove the trailing slash, so we have to check the
  # original config.
  if package_config['package']['tarball'][-1] == '/':
    package_tarball += '/%s-%s.tar.gz' % (
        package_config['artifact'], package_config['version'])
  check_file(package_tarball)

  Log.print_info("Installing %s to package server" % package_config['artifact'])
  package_info = upload_package(
      package_config['artifact'], package_tarball, package_source)
  if package_info:
    Log.print_success("Install %s to package server success" %
        package_config['artifact'])
    pprint.pprint(package_info)
  else:
    Log.print_critical("Install %s to package server fail" %
        package_config['artifact'])

def main():
  args = parse_command_line()
  return args.handler(args)

if __name__ == '__main__':
  main()
