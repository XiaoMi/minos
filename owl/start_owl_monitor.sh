#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: $0 port"
  exit 1
fi

source $SCRIPT_UTILS
nohup ./runserver.sh 0.0.0.0:$1 &

child_pid=`get_child_pid $!`
grandchild_pid=`get_child_pid $child_pid`
echo $grandchild_pid > $OWL_MONITOR_PID_FILE
