#!/bin/bash
$ENV_PYTHON manage.py quota_updater --period=3600 > quota_updater.log 2>&1
