#!/bin/sh

db_file="sqlite/tank.db"

if ! [ -e $db_file ] || [ -z "`cat $db_file`" ]; then
  ./manage.py syncdb
fi

hostname=`hostname --fqdn`
local_ip=`host $hostname | awk '{print $NF}'`
nohup ./manage.py runserver $local_ip:8000 1>tank.log 2>&1 &
