#!/bin/sh

PYTHONPATH=. $ENV_PYTHON supervisord.py $@
