#!/bin/sh

if [ $# -ne 2 ]; then
  echo "usage: $0 ip port"
  exit 1
fi

db_file="sqlite/tank.db"

if ! [ -e $db_file ] || [ -z "`cat $db_file`" ]; then
  $ENV_PYTHON manage.py syncdb
fi

ip=$1
port=$2

nohup $ENV_PYTHON manage.py runserver $ip:$port 1>tank.log 2>&1 &

sleep 1
echo `pgrep -P $!` > $TANK_PID_FILE

