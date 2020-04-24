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

if [ -z "${REST_HOME}" ]; then
  export REST_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo "REST_HOME: $REST_HOME"

checkVersion()
{
    echo "Version = $1"
	echo $1 |grep -E "^[0-9]+\.[0-9]+\.[0-9]+" > /dev/null
    if [ $? = 0 ]; then
        return 1
    fi

	echo "Version $1 illegal, it should be X.X.X format(e.g. 0.1.0)"
    exit 2
}

if [ $# -lt 1 ]; then
    echo -e "Usage: sh $0 Version"
    exit 2
fi

VERSION=$1
DOCKERHUB_REPO=2019liurui/iotdb-ikr

checkVersion $VERSION


cd ..
rm -rf lib
mvn clean package -Dmaven.test.skip=true
mkdir -p ./image/ikr
cp -r lib ./image/ikr
cp -r bin ./image/ikr
cp -r conf ./image/ikr
cp start-rest-service.sh ./image/ikr
cd ./image
zip -r ikr.zip ikr
rm -rf ikr

docker build --no-cache -t ${DOCKERHUB_REPO}:${VERSION}-alpine --build-arg version=${VERSION} .

docker push ${DOCKERHUB_REPO}:${VERSION}-alpine

rm -rf ikr.zip
