#!/bin/bash
python2.7 manage.py quota_updater --period=3600 > quota_updater.log 2>&1
