#!/bin/sh

db_file="sqlite/tank.db"

if ! [ -e $db_file ] || [ -z "`cat $db_file`" ]; then
  ./manage.py syncdb
fi

local_ip=`/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
nohup ./manage.py runserver $local_ip:8000 1>tank.log 2>&1 &
