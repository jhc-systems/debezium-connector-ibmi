#!/bin/bash

cd /app

MYIP=$(awk 'END{print $1}' /etc/hosts)

echo "My ip address is $MYIP"

export DEBEZIUM_CONFIG_STORAGE_REPLICATION_FACTOR=${DEBEZIUM_CONFIG_STORAGE_REPLICATION_FACTOR:=$REPLICATION_FACTOR}
export DEBEZIUM_OFFSET_STORAGE_REPLICATION_FACTOR=${DEBEZIUM_OFFSET_STORAGE_REPLICATION_FACTOR:=$REPLICATION_FACTOR}
export DEBEZIUM_STATUS_STORAGE_REPLICATION_FACTOR=${DEBEZIUM_STATUS_STORAGE_REPLICATION_FACTOR:=$REPLICATION_FACTOR}

export DEBEZIUM_CONFIG_STORAGE_PARTITIONS=${DEBEZIUM_CONFIG_STORAGE_PARTITIONS:=$PARTITIONS}
export DEBEZIUM_OFFSET_STORAGE_PARTITIONS=${DEBEZIUM_OFFSET_STORAGE_PARTITIONS:=$PARTITIONS}
export DEBEZIUM_STATUS_STORAGE_PARTITIONS=${DEBEZIUM_STATUS_STORAGE_PARTITIONS:=$PARTITIONS}

export DEBEZIUM_REST_PORT=${DEBEZIUM_REST_PORT:=$REST_PORT}
export DEBEZIUM_REST_ADVERTISED_PORT=${DEBEZIUM_REST_ADVERTISED_PORT:=$REST_PORT}

export DEBEZIUM_KEY_CONVERTER_SCHEMA_REGISTRY_URL=${DEBEZIUM_KEY_CONVERTER_SCHEMA_REGISTRY_URL:=$SCHEMA_REGISTRY_URL}
export DEBEZIUM_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=${DEBEZIUM_VALUE_CONVERTER_SCHEMA_REGISTRY_URL:=$SCHEMA_REGISTRY_URL}

cp $JAVA_HOME/lib/security/cacerts /tmp/cacerts
chmod u+w /tmp/cacerts

if [[ -d "/var/tls" ]] # import certificates if any exist
then
   for i in  /var/tls/*
   do
        test -f "$i" || continue
        test -s "$i" || echo "file $i is empty" || continue
        keytool -import -noprompt -alias "${i##*/}" -storepass changeit -keystore '/tmp/cacerts' -file "$i"
   done
fi

chmod u-w /tmp/cacerts

perl -e 'while (my ($key, $value) = each(%ENV)) {if ($key=~/^DEBEZIUM_/) { $key =~s/^DEBEZIUM_//;  $key =~ s/_/./g; print lc($key)."=$value\n";} }' > /tmp/connect-distributed.properties

jars=$(echo libs/*jar|tr ' ' ':'):classes:resources
jmxjar=/app/libs/jmx_prometheus_javaagent-0.16.1.jar
java -server $JAVA_OPTIONS -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager -Djavax.net.ssl.trustStore=/tmp/cacerts -cp $jars -javaagent:${jmxjar}=7071:/app/debezium-jmx-pometheus.yml -Dlog4j2.configurationFile=/app/resources/log4j2.xml org.apache.kafka.connect.cli.ConnectDistributed /tmp/connect-distributed.properties