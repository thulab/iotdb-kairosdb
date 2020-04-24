#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Applying customized configurations..."
DEPLOYMENT_DESCRIPTOR_FILE="$HOME_DIR/conf/DeploymentDescriptor.json"
CONFIG_FILE="$HOME_DIR/conf/config.properties"
echo $DEPLOYMENT_DESCRIPTOR_FILE
echo $CONFIG_FILE
#
#BROKER_ROLE="SLAVE"
#
#if [ $BROKER_ID = 0 ];then
#    if [ $REPLICATION_MODE = "SYNC" ];then
#      BROKER_ROLE="SYNC_MASTER"
#    else
#      BROKER_ROLE="ASYNC_MASTER"
#    fi
#fi
#
##BROKER_NAME=$(cat /etc/hostname)
#DELETE_WHEN="04"
#FILE_RESERVED_TIME="48"
#FLUSH_DISK_TYPE="ASYNC_FLUSH"
#
function create_config() {
    rm -f $DEPLOYMENT_DESCRIPTOR_FILE
    echo "Creating deployment descriptor JSON:"
    echo "$DEPLOY_DESCRIPTOR_JSON"
    echo "$DEPLOY_DESCRIPTOR_JSON" >> $DEPLOYMENT_DESCRIPTOR_FILE
    echo "Wrote JSON to $DEPLOYMENT_DESCRIPTOR_FILE"
}


create_config
