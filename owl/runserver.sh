#!/bin/bash

# '--insecure' is for serving static files in non-debug mode
# $* is for set host:port
python manage.py runserver --insecure $* > server.log 2>&1

