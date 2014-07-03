#!/bin/bash

source "$(dirname $0)"/../build/minos_env.sh || exit 1
cd $OWL_ROOT

$ENV_PYTHON manage.py collect > collector.log 2>&1
