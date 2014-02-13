#!/bin/bash

if [ $# -lt 1 ]; then
  echo "usage: $0 PID"
  exit 1
fi

kill $1
