#!/bin/bash

source $SCRIPT_UTILS
nohup ./quota_updater.sh &

child_pid=`get_child_pid $!`
echo $child_pid > $QUOTA_UPDATER_PID_FILE
