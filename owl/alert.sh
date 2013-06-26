#!/bin/bash

# add emails(',' seperated) that need to alert 
python2.7 manage.py alert --to_email="" --period=30 >alert.log 2>&1
