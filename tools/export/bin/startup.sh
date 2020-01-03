#!/bin/sh

if [ -z "${EXPORT_CSV_HOME}" ]; then
  export EXPORT_CSV_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo $EXPORT_CSV_HOME

MAIN_CLASS=kairosdb.export.csv.ExportToCsv

CLASSPATH=""
for f in ${EXPORT_CSV_HOME}/lib/*.jar; do
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


exec "$JAVA" -Xmx6g -Duser.timezone=GMT+8 -Dlogback.configurationFile=${EXPORT_CSV_HOME}/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?