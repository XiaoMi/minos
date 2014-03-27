#!/bin/bash

source "$(dirname $0)"/build/minos_env.sh || exit 1

if [ $# -lt 1 ]; then
  echo -e "Usage: $0 command [component]\n" \
    "command\n" \
    "'build'      : to build the virtual environment.\n" \
    "'start'      : to start a Minos component.\n" \
    "'stop'       : to stop a Minos component.\n" \
    "component\n" \
    "'tank'       : the package server.\n" \
    "'supervisor' : the processes monitor component.\n" \
    "'owl'        : the metrics management component."
  exit 2
elif [ $1 = "build" ]; then
  $BUILD_VIRTUALENV_ENTRY $@
else
  if ! [ -x $ENV_PYTHON ]; then
    echo "ERROR: please run './build.sh build' to build the virtual environment firstly."
    exit 3
  else
    PYTHONPATH=$CLIENT_ROOT $ENV_PYTHON $BUILD_COMPONENTS_ENTRY $@
  fi
fi
