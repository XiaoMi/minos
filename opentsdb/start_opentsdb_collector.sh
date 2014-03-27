#!/bin/bash

source $SCRIPT_UTILS
nohup ./collector.sh &

child_pid=`get_child_pid $!`
echo $child_pid > $OPENTSDB_COLLECTOR_PID_FILE
