# DEPRICATED

This project has been adopted by debezium and can be found at https://github.com/debezium/debezium-connector-ibmi

Many thanks to Jiri Pechanec for taking this on

please use the new project where future development will take place

# Migration notes the newly adopted driver requires a few different options in the json configuration

configuration changes:

```
  "config": {
    "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
    "schema": "SCHEMA",
    "hostname": "ibmiserver",
    "dbname": "database name",
    "user": "xxx",
    "password": "xxx",
    "secure": true
```

will become:

```
  "config": {
    "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
    "database.schema": "SCHEMA",
    "database.hostname": "ibmiserver",
    "database.dbname": "database name",
    "database.user": "xxx",
    "database.password": "xxx",
    "database.driver.secure": true  
```

you will also need to set the topic prefix to the same value as `hostname` in the old config to have the same topic names

`"topic.prefix" = "ibmiserver"`

All jdbc driver parameters are now prefixed `database.driver.` e.g. ccsid mapping or if you want to use secure connections 

i.e.

```
    "database.driver.secure": true 
    "database.driver.from_ccsid": 37,
    "database.driver.to_ccsid": 285,  
```

# OLD information

# Project

Use the IBM I journal as a source of CDC events see https://bitbucket.org/jhc-systems/journal-parsing/ for the journal fetch/decoding

# Configuration

## IBMI Permissions

```
GRTOBJAUT OBJ(<JRNLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<JRNLIB>/*ALL) OBJTYPE(*JRNRCV) USER(<CDC_USER>) AUT(*USE)
GRTOBJAUT OBJ(<JRNLIB>/<JRN>) OBJTYPE(*JRN) USER(<CDC_USER>) AUT(*USE *OBJEXIST)
```
```
GRTOBJAUT OBJ(<PROJECT_LIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<PROJECT_LIB>/*ALL) OBJTYPE(*FILE) USER(<CDC_USER>) AUT(*USE)
```
Where:

* <JRNLIB> is the library where the journal and receivers reside
* <JRN> is the journal name
* <PROJECT_LIB> is the Figaro database library
* <CDC_USER> is the username of the CDC service account

## Required
The following environment variables are mandatory configuration and have no default values

```
DEBEZIUM_BOOTSTRAP_SERVERS
SCHEMA_REGISTRY_URL

DEBEZIUM_REST_ADVERTISED_HOST_NAME
```

## Production

It is highly recommended that the partitions and replicaion_factor is increased for production

```
PARTITIONS=3
REPLICATION_FACTOR=3
```

# Limitations

* TODO integrate with exit program to prevent journal loss https://github.com/jhc-systems/debezium-ibmi-exitpgm
* Limited support for table changes - the journal entries for table changes are not documented so rely on fetching table structure at runtime and refetching when table change detected
* No support for remote journals and fail over
* No support for clobs/xml and similar large text/blobs

# Problems

## running with a self signed certificate

capture your ca cert normally the bottom of the output:
`openssl s_client -showcerts -connect <host>:9471`

between the last pair of `-----BEGIN CERTIFICATE-----` and end and save it to iseries-cert.pem
	

If using docker simply mount your certs at /var/tls

If running natively import the cert
	
	keytool -import -noprompt -alias iseries-cert  -storepass changeit -keystore /usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts -file iseries-cert.pem

## Journals deleted

If the journal is deleted before it is read it will log an error: "Lost journal at position xxx" and reset to the beginning journal

At this point you really need to resync, this does not happen automatically

* Todo needs to support ad-jhoc snapshotting

## Memory

Recommended minimum memory 1GB

This can either be configured with cloud resources or using `JAVA_OPTIONS=-Xmx1g`

## Tracing

```
DEBEZIUM_


```

```
producer.interceptor.classes=brave.kafka.interceptor.TracingProducerInterceptor
consumer.interceptor.classes=brave.kafka.interceptor.TracingConsumerInterceptor

producer.zipkin.sender.type=KAFKA
producer.zipkin.local.service_name=ksql
producer.zipkin.bootstrap.servers=mskafka:9092
```

# Configuring the CDC

## Use the REST interface to post a new connector

https://bitbucket.org/jhc-systems/kafka-kubernetes/src/master/runtime/source/

A new connector configuration `connector-name.json`:

`curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '@./connector-name.json'`

file connector-name.json:

```
{
  "name": "connector-name",
  "config": {
    "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
    "schema": "SCHEMA",
    "sanitize.field.names": "true",
    "tasks.max": "1",
    "hostname": "ibmiserver",
    "dbname": "database name",
    "user": "xxx",
    "password": "xxx",
    "port": "",
    "secure": true
    "poll.interval.ms": "2000",
    "transforms": "unwrap",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "table.include.list": "SCHEMA.TABLE1",
    "snapshot.mode": "initial",

    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",

    "snapshot.max.threads": 4
  }
}
```


Note the `dbname` can be blank and will be used as part of the jdbc connect string : `dbc://hostname/dbname`


Optional:

```
    "socket timeout": "300000",
    "keep alive": "true",
    "thread used": "false"
```
the above help with connections that can be blocked (firewalled) or dropped due to vpn issues

## CCSID

Unusually we have the incorrect CCSID on all our tables and the data is forced into the tables with the wrong encoding

This issue should really be corrected and the data translated but with thousands of tables and many clients all configured incorrectly this is a huge job with significant risk. Instead we have an additional pair of settings from_ccsid which is the ccsid on the table and to_ccsid which will use this ccsid instead - this is for the entire system and all tables.


## The list of connectors

`curl -i -X GET -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/`

```
[connector-name]
```

## Deleting a connector

and deleted with

`curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/connector-name`

## Getting the running configuration of the connector

`curl -i -X GET -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/connector-name`

## Adding tables

The configuration can be updated with

`curl -i -X PUT -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/connector-name/config/ -d "@connector-name-config.json"`

Notes
* the `connector-name` in the url must match the `name` in the json file
* the update file only contains the inner `config`

```
{
    "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
    "schema": "SCHEMA",
    "sanitize.field.names": "true",
    "tasks.max": "1",
    "hostname": "ibmiserver",
    "dbname": "database name",
    "user": "xxx",
    "password": "xxx",
    "port": "",
    "poll.interval.ms": "2000",
    "transforms": "unwrap",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "table.include.list": "SCHEMA.TABLE1",
    "snapshot.mode": "initial",

    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
}
```

Here we've added TABLE2 to the list.

# monitoring

prometheus stats are avaialble on port 7071

sample prometheus stats are in

* metrics/prometheus

sample grafana charts

* metrics/grafana.txt


# Troubleshooting

See upstream project: https://bitbucket.org/jhc-systems/journal-parsing/src/master/

## No journal entries found check journalling is enabled and set to *BOTH

`dspfd MYTABLE`

```
    File is currently journaled . . . . . . . . :            Yes
    Current or last journal . . . . . . . . . . :            MYJRN
      Library . . . . . . . . . . . . . . . . . :            MYLIB
    Journal images  . . . . . . . . . . . . . . : IMAGES     *BOTH

```

# Development

## Class diagram
https://lucid.app/lucidchart/invitations/accept/inv_b0dba11e-fb73-4bfc-9efd-1c14d7ef2642

## Testing/debugging

main class
* `org.apache.kafka.connect.cli.ConnectDistributed`

runtime argument of the configuration e.g. for
* local kafka `src/test/resources/protobuf.properties`
* remote confluent `src/test/resources/confluent.properties`

Logging - vm args `-Dlogback.configurationFile=src/test/resources/logback.xml`

## Running kafka locally
https://bitbucket.org/jhc-systems/kafka-kubernetes/src/master/docker/

## Running debezium locally

Configure the IP addresses in `conf/local.env`, `src/test/resources/protobuf.properties`, and `src/test/resources/confluent.properties` to be your IP address. If running using localhost, you can use `0.0.0.0` for each of these.

### VS Code

To run in VS Code, configure the following launch.json file, and run from the Run and Debug extension.
```
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "type": "java",
            "name": "Launch",
            "request": "launch",
            "mainClass": "org.apache.kafka.connect.cli.ConnectDistributed",
            "projectName": "debezium-connector-ibmi",
            "env": {},
            "args": "src/test/resources/protobuf.properties",
            "logback.configurationFile": "src/test/resources/logback.xml"
        }
    ]
}
```

## Release notes

### 1.10.4

New configuration paramater `secure` this defaults to true

### 1.10.2

Fixes data loss bugs:
* * when receivers reset
* * after receiving a continuation offset the first entry is occasionally lost (when the next request doesn't contain a continuation offset)

### 1.9

additional configuration parameters required for avro in the submitted json

```
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
```

optional parameter

```
"snapshot.max.threads": 4
```

# Build

## build docker image and publish to local docker server

mvn compile jib:dockerBuild
