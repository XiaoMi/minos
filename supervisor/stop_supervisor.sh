#!/bin/sh

if [ $# -lt 1 ]; then
  echo "usage: $0 PID"
  exit 1
fi

kill $1 && \
while [ 1 ]; do
  if netstat -nlap 2>/dev/null |grep ":9001" |grep LISTEN; then
    sleep 1
    echo "Wait for supervisor exiting..."
  else
    break
  fi
done
