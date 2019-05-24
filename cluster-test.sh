#!/bin/bash

IOTDB_PATH=/data/liurui/incubator-iotdb/iotdb/iotdb
IKR_PATH=/home/ubuntu/first-rest-test/iotdb-kairosdb
BENCH_PATH=/home/ubuntu/first-rest-test/iotdb-benchmark
MONITOR_PATH=/home/ubuntu/first-rest-test/monitor/iotdb-benchmark

for i in {2..16}
do
  ssh ubuntu@192.168.8.$i "rm -rf /home/ubuntu/first-rest-test"
  scp -r ikr/ ubuntu@192.168.8.$i:/home/ubuntu/first-rest-test
  scp -r iotdb-benchmark/ ubuntu@192.168.8.$i:/home/ubuntu/first-rest-test
  scp -r monitor/ ubuntu@192.168.8.$i:/home/ubuntu/first-rest-test
  ssh ubuntu@192.168.8.$i "sed -i \"s/^DB_URL.*$/DB_URL=http:\/\/192.168.8.${i}:6666/g\" $BENCH_PATH/conf/config.properties"
  ssh ubuntu@192.168.8.$i "$IKR_PATH/stop-rest-service-daemon.sh"
done

###初始化及启动16上的iotdb
echo "start monitoring on 192.168.8.16"
ssh ubuntu@192.168.8.16 "$MONITOR_PATH/ser-benchmark.sh > /dev/null 2>&1 &"
echo "stopping IoTDB server..."
ssh ubuntu@192.168.8.16 "sh $IOTDB_PATH/bin/stop-server.sh"
sleep 2
ssh ubuntu@192.168.8.16 "rm -rf $IOTDB_PATH/data"
sleep 5
ssh ubuntu@192.168.8.16 "$IOTDB_PATH/bin/start-server.sh > /dev/null 2>&1 &"
echo "wait 10 seconds for re-starting IoTDB"
sleep 10

###先启动一个写入数据一段时间，使系统初始化完毕
echo "start local monitoring"
ssh ubuntu@192.168.8.2 "$MONITOR_PATH/ser-benchmark.sh > /dev/null 2>&1 &"
echo "wait 10 seconds for starting local IKR"
ssh ubuntu@192.168.8.2 "$IKR_PATH/start-rest-service-daemon.sh"
sleep 10
ssh ubuntu@192.168.8.2 "$BENCH_PATH/benchmark.sh > /dev/null 2>&1 &"
echo "wait 30 seconds for initializing new metric"
sleep 30

###再启动3～16号机器上的数据写入
for i in {3..15}
do
echo "start monitoring on 192.168.8.${i}"
ssh ubuntu@192.168.8.$i "$MONITOR_PATH/ser-benchmark.sh > /dev/null 2>&1 &"
echo "starting IKR on 192.168.8.${i} ..."
ssh ubuntu@192.168.8.$i "$IKR_PATH/start-rest-service-daemon.sh"
sleep 10
echo "starting ingestion test on 192.168.8.${i} ..."
ssh ubuntu@192.168.8.$i "$BENCH_PATH/benchmark.sh > /dev/null 2>&1 &"
done

echo "starting IKR on 192.168.8.16 ..."
ssh ubuntu@192.168.8.16 "$IKR_PATH/start-rest-service-daemon.sh"
sleep 10
echo "starting ingestion test on 192.168.8.16 ..."
ssh ubuntu@192.168.8.16 "$BENCH_PATH/benchmark.sh > /dev/null 2>&1 &"

echo "ingestion tests on all nodes have started"