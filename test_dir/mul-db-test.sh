#!/bin/bash

USER=fit
WORK_DIR=/home/fit/liurui/distributed_ikr
IOTDB_PATH=$WORK_DIR/iotdb-server-0.9.0
IP_ARRAY=(4 5 11 14 17 19 20 21)
NET_ZONE=192.168.130

### 在每个节点上安装IoTDB
#for i in ${IP_ARRAY[@]}
#do
#  echo "initializing $USER@$NET_ZONE.$i"
#  ssh $USER@$NET_ZONE.$i "rm -rf $WORK_DIR;mkdir -p $WORK_DIR"
#  scp -r iotdb-server-0.9.0/ $USER@$NET_ZONE.$i:$WORK_DIR
#done

for i in ${IP_ARRAY[@]}
do
  ssh $USER@$NET_ZONE.$i "bash $IOTDB_PATH/sbin/stop-server.sh"
  ssh $USER@$NET_ZONE.$i "rm -rf $IOTDB_PATH/data"
  ssh $USER@$NET_ZONE.$i "rm -rf $IOTDB_PATH/logs"
  sleep 2
  ssh $USER@$NET_ZONE.$i "bash $IOTDB_PATH/sbin/start-server.sh > /dev/null 2>&1 &"
  echo "start databases on $USER@$NET_ZONE.$i"
done

echo "All databases started"