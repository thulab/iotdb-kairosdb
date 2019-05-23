#!/bin/sh

if [ -z "${REST_HOME}" ]; then
  export REST_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

$REST_HOME/start-rest-service.sh > /dev/null 2>&1 &