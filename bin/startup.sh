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


exec "$JAVA" -Duser.timezone=GMT+8 -Dlogback.configurationFile=${REST_HOME}/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?