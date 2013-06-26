#!/bin/sh

pid=`ps aux|grep supervisord.py | grep -v grep | awk '{print $2}'`

kill $pid && \
while [ 1 ]; do
  if netstat -nlap 2>/dev/null |grep 9001 |grep LISTEN; then
    sleep 1
    echo "Wait for supervisor exiting..."
  else
    break
  fi
done
