#!/bin/bash

job_name=$1
run_dir=$2
package_dir=$run_dir/package
log_dir=$run_dir/log

ln -s $package_dir/public
mkdir logback
ln -s $run_dir/cluster.xml logback

if ! [ -d logs ]; then
  if [ $job_name = "logviewer" ]; then
    ln -s $SUPERVISOR_LOG_DIR logs
  else
    ln -s $log_dir logs
  fi
fi
