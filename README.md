# Project 

Use the IBM I journal as a source of CDC events see https://bitbucket.org/jhc-systems/journal-parsing/ for the journal fetch/decoding

# Configuration

## IBMI Permissions

GRTOBJAUT OBJ(<JRNLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<JRNLIB>/*ALL) OBJTYPE(*JRNRCV) USER(<CDC_USER>) AUT(*USE)
GRTOBJAUT OBJ(<JRNLIB>/<JRN>) OBJTYPE(*JRN) USER(<CDC_USER>) AUT(*USE *OBJEXIST)
 
GRTOBJAUT OBJ(<FIGLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<FIGLIB>/*ALL) OBJTYPE(*FILE) USER(<CDC_USER>) AUT(*USE)
 
Where:
 
* <JRNLIB> is the library where the journal and receivers reside
* <JRN> is the journal name
* <FIGLIB> is the Figaro database library
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

# Problems
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

`curl -i -X PUT -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '@./connector-name.json'`

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
    "poll.interval.ms": "2000",
    "transforms": "unwrap",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "table.include.list": "SCHEMA.TABLE1",
    "snapshot.mode": "initial"
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

This issue should really be corrected and the data translated but with thousands of tables and many clients all configured incorrectly this is a huge job with significant risk. Instead we have an additional setting forced_ccsid which will use this ccsid irrespective of the ccsid configured on the table of columns - this is for the entire system and all tables.


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

where the update file only contains the inner config and the connector name is in the url path:

```
{
    "name": "connector-name",
    "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
    "schema": "SCHEMA",
    "sanitize.field.names": "true",
    "tasks.max": "1",
    "hostname": "ibmiserver",
    "dbname": "internaldbname",
    "user": "xxx",
    "password": "xxx",
    "port": "",
    "poll.interval.ms": "2000",
    "transforms": "unwrap",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "table.include.list": "SCHEMA.TABLE1,SCHEMA.TABLE2",
    "snapshot.mode": "initial"
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

# Build

## build docker image and publish to local docker server

mvn compile jib:dockerBuild

