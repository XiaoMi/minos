import sys

from datetime import datetime

class Log:
  # We have such a agreement on verbosity level:
  # 0: equals to print_info
  # 1: summary of a host level operation (a batch of command)
  # 2: summary of a command
  # 3: details or content of a command
  verbosity = 0

  @staticmethod
  def _print(message):
    print "%s %s" % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), message)

  @staticmethod
  def error_exit(print_stack):
    if not print_stack:
      sys.exit(2)
    else:
      raise RuntimeError("fatal error")

  @staticmethod
  def print_verbose(message, verbosity):
    if verbosity <= Log.verbosity:
      Log.print_info(message)

  @staticmethod
  def print_info(message):
    Log._print(message)

  @staticmethod
  def print_success(message):
    Log._print("\033[0;32m%s\033[0m" % message)

  @staticmethod
  def print_warning(message):
    Log._print("\033[0;33m%s\033[0m" % message)

  @staticmethod
  def print_error(message):
    Log._print("\033[0;31m%s\033[0m" % message)

  @staticmethod
  def print_critical(message):
    Log.print_error(message)
    Log.error_exit(False)
