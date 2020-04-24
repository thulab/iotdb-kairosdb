#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM openjdk:8-alpine

RUN apk add --no-cache bash gettext nmap-ncat openssl busybox-extras

ARG user=ikr
ARG group=ikr
ARG uid=2000
ARG gid=2000

# ikr is run with user `ikr`, uid = 2000
# If you bind mount a volume from the host or a data container,
# ensure you use the same uid
RUN addgroup --gid ${gid} ${group} \
    && adduser --uid ${uid} -G ${group} ${user} -s /bin/bash -D

ARG version

# ikr version
ENV VERSION ${version}

# ikr home
ENV HOME_DIR  /home/ikr/ikr-${VERSION}

WORKDIR  ${HOME_DIR}

COPY ikr.zip ${HOME_DIR}/

# Install
RUN unzip ikr.zip; \
	mv ikr/* . ; \
	rmdir ikr ; \
	rm ikr.zip ; \
	apk del .build-deps ; \
    rm -rf /var/cache/apk/* ; \
    rm -rf /tmp/*


RUN chown -R ${uid}:${gid} ${HOME_DIR}

# Expose ikr ports
EXPOSE 6666

## Export Java options
#RUN export JAVA_OPT=" -Duser.home=/opt"
#
## Add ${JAVA_HOME}/lib/ext as java.ext.dirs
#RUN sed -i 's/${JAVA_HOME}\/jre\/lib\/ext/${JAVA_HOME}\/jre\/lib\/ext:${JAVA_HOME}\/lib\/ext/' ${HOME_DIR}/bin/tools.sh

COPY genConfig.sh start.sh ${HOME_DIR}/

RUN chmod a+x ${HOME_DIR}/genConfig.sh \
 && chmod a+x ${HOME_DIR}/start.sh

USER ${user}

CMD ["/bin/bash", "./start.sh"]