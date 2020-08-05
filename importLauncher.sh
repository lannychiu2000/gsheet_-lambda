#!/bin/bash

timeStamp=$(date +"%T")
launchDir="/root/arm/import"
launchScript="import.py"
PID=`/bin/ps -ef | grep ${launchScript} | grep -v ${timeStamp} | grep -v grep | awk '{print $2}'`

if [[ ${PID} ]]; then
  echo "$(date) INFO: Found existing process running ${PID}. Exiting..."
  exit 0
else
  echo "$(date) INFO: Starting ${launchScript}."
  cmd="${launchDir}/${launchScript}"
  $cmd & 
fi

exit 0