#!/bin/sh

if [ -z "${ANA_CONF}" ]; then
  export ANA_CONF="$(cd "`dirname "$0"`"; pwd)"
fi

#git pull
#rm -rf lib
#mvn clean package -Dmaven.test.skip=true
cd $ANA_CONF/bin
sh ./startup.sh -cf ../conf/config.properties
