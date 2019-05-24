#!/bin/sh

if [ -z "${REST_HOME}" ]; then
  export REST_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

$REST_HOME/start-rest-service.sh > /dev/null 2>&1 &
echo "IKR已在后台启动, 请使用jps命令确认IKR进程是否存在, 或查看logs文件下的日志输出"