#!/bin/sh

pid=$(jps | grep IKR | awk '{print $1}')
if [ "$pid" = "" ]; then
  echo "No IKR service to stop"
  else
  kill -9 $pid
  echo "IKR stopped"
fi
