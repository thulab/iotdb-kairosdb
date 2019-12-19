#!/bin/sh

if [ -z "${REST_HOME}" ]; then
  export REST_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo $REST_HOME

MAIN_CLASS=cn.edu.tsinghua.iotdb.kairosdb.IKR

CLASSPATH=""
for f in ${REST_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

JMX_PORT="31998"

IOTDB_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "


exec "$JAVA" $IOTDB_JMX_OPTS -Xmx50g -Xms50g -Duser.timezone=GMT+8 -Dlogback.configurationFile=${REST_HOME}/conf/logback.xml -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?