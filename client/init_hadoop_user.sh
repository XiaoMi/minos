#!/bin/sh

if [ $# -ne 2 ]; then
  echo "Usage: `basename $0` user cluster"
  exit 1
fi

kinit hdfs_admin@XIAOMI.HADOOP || exit 2

user=$1
cluster=$2
HDFS="./deploy.py shell hdfs $cluster dfs"

set -x

if [[ "$user" =~ hbase.* ]]; then
  $HDFS -mkdir -p /hbase
  $HDFS -chown $user /hbase
elif [[ "$user" =~ yarn.* ]]; then
  $HDFS -mkdir -p /tmp/hadoop-yarn/staging/history
  $HDFS -chown $user /tmp/hadoop-yarn
  $HDFS -chmod -R 777 /tmp/hadoop-yarn/staging

  $HDFS -mkdir -p /var/log/hadoop-yarn
  $HDFS -chown $user /var/log/hadoop-yarn
else
  $HDFS -mkdir -p /user/$user
  $HDFS -chown $user /user/$user
fi

kdestroy
