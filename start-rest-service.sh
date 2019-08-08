#!/bin/sh

if [ -z "${REST_HOME}" ]; then
  export REST_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

#git pull
#rm -rf lib
#mvn clean package -Dmaven.test.skip=true
cd $REST_HOME/bin
sh ./startup.sh -cf ../conf/config.properties
