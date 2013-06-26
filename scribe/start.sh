#!/bin/bash

run_dir=%run_dir

cd $run_dir

killall -9 hadoop_scribed
sleep 2

ln -sf package/scribed hadoop_scribed

timestamp=`date +%%s`
exec  nohup ./hadoop_scribed -c ./scribe.conf 1>log/scribe_$timestamp.log 2>&1

cd -
