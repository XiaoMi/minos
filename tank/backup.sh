#!/bin/sh

ticket_cache=".tank_ticket"

kinit -R -c $ticket_cache

export KRB5CCNAME=$ticket_cache

./backup.py --hadoop_home="./hadoop-2.0.0-mdh1.0.0-SNAPSHOT" \
            --cluster=lgprc-xiaomi \
            --backup_root=/user/h_tank \
            --tank_home=`pwd` 1>>backup.log 2>&1
