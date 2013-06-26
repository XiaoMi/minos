#!/bin/bash
bin_path=`dirname $0`
cd $bin_path
python2.7 manage.py quota_reportor > quota_reportor.log 2>&1
