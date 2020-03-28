#!/bin/bash

USER=fit
WORK_DIR=/home/fit/liurui/distributed_ikr
IOTDB_PATH=$WORK_DIR/iotdb-server-0.9.0
IP_ARRAY=(4 5 11 14 17 19 20 21)

for i in ${IP_ARRAY[@]}
do
  scp -r iotdb-server-0.9.0/ $USER@192.168.130.$i:$WORK_DIR
done

for i in ${IP_ARRAY[@]}
do
  ssh $USER@192.168.130.$i "sh $IOTDB_PATH/sbin/stop-server.sh"
  ssh $USER@192.168.130.$i "rm -rf $IOTDB_PATH/data"
  ssh $USER@192.168.130.$i "rm -rf $IOTDB_PATH/logs"
  sleep 2
  ssh $USER@192.168.130.$i "sh $IOTDB_PATH/sbin/start-server.sh > /dev/null 2>&1 &"
done

echo "started databases on all nodes"