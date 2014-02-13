#!/bin/bash

function get_child_pid() {
  child_pid=`pgrep -P $1`
  while [ -z $child_pid ]; do
    sleep 1
    child_pid=`pgrep -P $1`
  done
  echo $child_pid
}
