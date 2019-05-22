## 测试workload

模拟300个车辆设备，每个车3200个传感器，采集频率200ms

## IKR、IoTDB、iotdb-benchmark部署
1. 在青云15个服务器（192.168.8.2～192.168.8.16）上部署客户端模拟器iotdb-benchmark，（每个iotdb-benchmark设置客户端参数为300/15=20）
2. 在192.168.8.16上部署IoTDB
3. 在青云15个服务器（192.168.8.2～192.168.8.16）上部署iotdb-kairosdb（IKR） （需要配置所有的IKR的HOST=192.168.8.16）

## 部署单节点IoTDB
1. 修改192.168.8.16节点上iotdb的配置文件
```
> nano incubator-iotdb/iotdb/iotdb/conf/iotdb-env.sh
```
修改以下参数：
```
MAX_HEAP_SIZE="55G"
```
```
> nano incubator-iotdb/iotdb/iotdb/conf/iotdb-engine.properties
```
修改以下参数：
```
period_time_for_flush_in_second=3600000
bufferwrite_file_size_threshold=524288000
enable_small_flush=false
```
其他参数为默认配置

2. 在192.168.8.16节点上启动iotdb服务
```
> nohup ./start-server.sh &
```
## 部署IKR
在青云15个服务器（192.168.8.2～192.168.8.16）上部署IKR，详见https://github.com/thulab/iotdb-kairosdb
1. 修改IKR的配置文件 
```
> nano conf/config.properties
```
```
HOST=192.168.8.16
```
2. 后台启动IKR
```
> nohup ./start-rest-service.sh &
```

## 预创建元数据
使用benchmark单客户端写入所有设备的一个数据点，操作步骤如下


2. 配置benchmark参数
```
> nano conf/config.properties
```
```
DB_URL=http://192.168.8.2:6666
FIRST_DEVICE_INDEX=0
LOOP=1
DB_SWITCH=KairosDB
BENCHMARK_WORK_MODE=insertTestWithDefaultPath
CLIENT_NUMBER=1
GROUP_NUMBER=1
DEVICE_NUMBER=1
SENSOR_NUMBER=3200
BATCH_SIZE=1
USE_OPS=false
```
其他参数为默认值

3. 启动benchmark客户端
```
> ./benchmark.sh
```
等待执行完毕后即完成了元数据预创建阶段

## 在每台机器上启动benchmark进行写入
在192.168.8.2～192.168.8.16的每一台机器上启动IKR和benchmark客户端进行数据写入
设当前所在机器的ip是192.168.8.x
1. 配置benchmark参数
```
> nano conf/config.properties
```
```
DB_URL=http://192.168.8.x:6666
FIRST_DEVICE_INDEX= 0 + (x - 2) * 20

LOOP=1000000
DB_SWITCH=KairosDB
BENCHMARK_WORK_MODE=insertTestWithDefaultPath
CLIENT_NUMBER=20
GROUP_NUMBER=1
DEVICE_NUMBER=20
SENSOR_NUMBER=3200
BATCH_SIZE=1
USE_OPS=true
CLIENT_MAX_WRT_RATE=16000
```

2. 后台启动benchmark客户端
```
> nohup ./benchmark.sh &
```

3. 查看输出和日志
```
> tail -f nohup.out
```
```
> tail -f logs/log_info.log
```