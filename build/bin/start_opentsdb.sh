#!/bin/bash

if [ $# -lt 1 ]; then
  echo "usage: $0 port"
  exit 1
fi

tsdtmp=${TMPDIR-'/tmp'}/tsd
mkdir -p "$tsdtmp"
nohup ./build/tsdb tsd --port=$1 --staticroot=build/staticroot --cachedir="$tsdtmp" 1>opentsdb.out 2>&1 &

echo $! > $OPENTSDB_PID_FILE

