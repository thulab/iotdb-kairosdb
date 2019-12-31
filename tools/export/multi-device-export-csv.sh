#!/bin/bash

if [ -z "${EXPORT_CSV_HOME}" ]; then
  export EXPORT_CSV_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

#git pull
#rm -rf lib
#mvn clean package -Dmaven.test.skip=true

CHANGE_PARAMETER=MACHINE_ID

DEVICE_ID_LIST=(1701 1702 1703)

for DEVICE in ${DEVICE_ID_LIST[@]}
do
  sed -i "s/^${CHANGE_PARAMETER}.*$/MACHINE_ID=${DEVICE}/g" $EXPORT_CSV_HOME/conf/config.properties
  grep $CHANGE_PARAMETER  $EXPORT_CSV_HOME/conf/config.properties
  cd $EXPORT_CSV_HOME/bin
  sh ./startup.sh -cf ../conf/config.properties
  echo "车号为 ${DEVICE} 的数据导出完毕"
done