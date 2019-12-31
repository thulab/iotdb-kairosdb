#!/bin/sh

if [ -z "${EXPORT_CSV_HOME}" ]; then
  export EXPORT_CSV_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

#git pull
#rm -rf lib
#mvn clean package -Dmaven.test.skip=true
cd $EXPORT_CSV_HOME/bin
sh ./startup.sh -cf ../conf/config.properties
