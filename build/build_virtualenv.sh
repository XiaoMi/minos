#!/bin/bash

#######################
# check python version
#######################

SYS_PYTHON=`which python 2>/dev/null`
if [ ! $SYS_PYTHON ]; then
  echo "Error: No python found!" \
    "Please install Python 2.7 or later from <http://www.python.org> firstly."
  exit 4
fi

####################
# build virtual-env
####################

# Create virtual environment if not exists.
if ! [ -x $ENV_PYTHON ]; then
  echo "Creating virtual environment at $BUILD_ENV_ROOT"
  $SYS_PYTHON $VIRTUAL_BOOTSTRAP_ENTRY --no-site-packages $BUILD_ENV_ROOT 2>&1
  if [ -x $ENV_PYTHON ]; then
    echo "$BUILD_ENV_ROOT ready"
  else
    echo "Creating virtual environment failed"
    exit 5
  fi
fi

###############################################
# Build Minos client, Tank, Supervisor offline
###############################################
if [ $# -gt 1 ]; then
  PYTHONPATH=$CLIENT_ROOT $ENV_PYTHON $BUILD_COMPONENTS_ENTRY $@
fi

############################################################
# build Minos client, install prerequisite python libraries
############################################################
if [ $? -eq 0 ]; then
  PYTHONPATH=$CLIENT_ROOT $ENV_PYTHON $BUILD_CLIENT_ENTRY
fi
