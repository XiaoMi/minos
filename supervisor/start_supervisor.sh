#!/bin/sh

./stop_supervisor.sh

PYTHONPATH=. ./supervisord.py $@
