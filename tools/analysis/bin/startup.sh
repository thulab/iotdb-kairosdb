#!/bin/sh

if [ -z "${ANA_CONF}" ]; then
  export ANA_CONF="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo $ANA_CONF

MAIN_CLASS=kairosdb.valid.ana.DataAnalysis

CLASSPATH=""
for f in ${ANA_CONF}/lib/*.jar; do
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


exec "$JAVA" -Xmx10g -Duser.timezone=GMT+8 -Dlogback.configurationFile=${ANA_CONF}/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?