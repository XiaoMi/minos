#!/usr/bin/env python
import os
import sys
import ctypes

if __name__ == "__main__":
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "owl.settings")

  root_path = os.path.abspath(
      os.path.dirname(os.path.realpath(__file__))+ '/..')
  owl_path = os.path.join(root_path, 'owl')

  # add libs path for loading module zookeeper
  lib_path = os.path.join(owl_path, "libs")
  sys.path.append(lib_path)
  ctypes.cdll.LoadLibrary(os.path.join(lib_path, 'libzookeeper_mt.so.2'))

  client_path = os.path.join(root_path, 'client')
  sys.path.append(client_path)

  deploy_utils = __import__('deploy_utils')
  conf_path = deploy_utils.get_config_dir()

  owl_conf_path = os.path.join(conf_path, 'owl')
  sys.path.append(owl_conf_path)

  from django.core.management import execute_from_command_line

  execute_from_command_line(sys.argv)
