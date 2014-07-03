#!/bin/bash

# '--insecure' is for serving static files in non-debug mode
# $* is for set host:port

source "$(dirname $0)"/../build/minos_env.sh || exit 1
cd $OWL_ROOT

$ENV_PYTHON manage.py runserver --insecure $* > server.log 2>&1

