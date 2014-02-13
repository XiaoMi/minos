import os

import build_utils

from minos_config import CLIENT_PREREQUISITE_PYTHON_LIBS
from minos_config import Log

def build_client():
  # Check and install prerequisite python libraries
  Log.print_info("Check and install prerequisite python libraries")
  build_utils.check_and_install_modules(CLIENT_PREREQUISITE_PYTHON_LIBS)
  Log.print_success("Build Minos client success")

if __name__ == '__main__':
  build_client()
