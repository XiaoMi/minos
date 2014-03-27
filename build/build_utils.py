import ConfigParser
import os
import subprocess
from string import Template

from minos_config import Log

MINOS_ROOT = os.getenv("MINOS_ROOT")
ENV_PIP = os.getenv("ENV_PIP")
BUILD_INFO_FILE = os.getenv("BUILD_INFO_FILE")
BUILD_OFFLINE_REQUIREMENTS_FILE = os.getenv("BUILD_OFFLINE_REQUIREMENTS_FILE")

def execute_command(cmd, log_message="", error_message=""):
  if log_message:
    Log.print_info(log_message)
  try:
    subprocess.check_call(cmd)
  except BaseException, e:
    Log.print_critical('ERROR: %s' % error_message if error_message else str(e))

def check_command_output(cmd, error_message="", skip_error=False):
  try:
    out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
  except BaseException, e:
    if skip_error:
      return 0
    else:
      Log.print_critical('ERROR: %s' % error_message if error_message else str(e))
  return 1

def get_command_variable(cmd):
  child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  out = child.communicate()
  return out[0].strip()

def get_process_running_pid(pid_file):
  try:
    with open(pid_file) as fp:
      return int(fp.readline())
  except ValueError, e:
    Log.print_critical("Error: Pid file %s is null" % pid_file)

def check_process_is_running(pid_file):
  if not os.path.exists(pid_file):
    return False

  process_pid = get_process_running_pid(pid_file)
  try:
    os.kill(process_pid, 0)
    return True
  except OSError:
    return False

def exec_daemon_script(dest_path, script, *extra_para):
  os.chdir(dest_path)
  cmd = ["%s" % script]
  cmd.extend(list(extra_para))
  execute_command(cmd)
  os.chdir(MINOS_ROOT)

def start_daemon_process(process_name, pid_file, dest_path, script, *extra_para):
  if check_process_is_running(pid_file):
    Log.print_warning("%s is running, please check" % process_name)
    return

  Log.print_info("Starting %s" % process_name)
  exec_daemon_script(dest_path, script, *extra_para)
  Log.print_success("Start %s success" % process_name)

def stop_daemon_process(process_name, pid_file, dest_path, script):
  if not check_process_is_running(pid_file):
    Log.print_warning("%s is not running" % process_name)
    return

  Log.print_info("Stopping %s" % process_name)
  exec_daemon_script(dest_path, script, str(get_process_running_pid(pid_file)))
  Log.print_success("Stop %s success" % process_name)

def generate_config_file(template_file, dest_file, config_dict):
  config_template = Template(open(template_file).read())
  config_file = config_template.safe_substitute(config_dict)

  with open(dest_file, 'w') as output:
    output.write(config_file)

def output_build_info(component, info_key, info_val):
  build_info_parser = ConfigParser.SafeConfigParser()
  build_info_parser.read([BUILD_INFO_FILE])

  if not build_info_parser.has_section(component):
    build_info_parser.add_section(component)
  build_info_parser.set(component, info_key, str(info_val))

  with open(BUILD_INFO_FILE, 'wb') as build_info:
    build_info_parser.write(build_info)

def get_build_info_option(component, option):
  build_info_parser = ConfigParser.SafeConfigParser()
  build_info_parser.read([BUILD_INFO_FILE])
  if build_info_parser.has_option(component, option):
    return build_info_parser.get(component, option)
  return None

def check_module_installed(module):
  try:
    __import__(module)
  except ImportError:
    return 0
  return 1

def pip_install_offline(offline_package_dir):
  cmd = [ENV_PIP, "install", "--no-index", "--find-links",
    offline_package_dir, "-r", BUILD_OFFLINE_REQUIREMENTS_FILE]
  execute_command(cmd)

def pip_install(module, module_version):
  log_message = "Installing %s" % module
  cmd = [ENV_PIP, "install", "%s>=%s" % (module, module_version)]
  execute_command(cmd, log_message=log_message)

def check_and_install_modules(modules_list):
  for module_key, module_val, module_version in modules_list:
    if not check_module_installed(module_key):
      pip_install(module_val, module_version)

