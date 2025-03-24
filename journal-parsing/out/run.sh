#!/usr/bin/bash
export ISERIES_HOST=tracey.servers.jhc.co.uk
export ISERIES_PASSWORD=MSDEV
export ISERIES_SCHEMA=F63HLDDLSH
export ISERIES_USER=MSDEV
export MAX_THREADS=2
export TABLE_PREFIX=SKIPTEST
java -Dlog4j.configurationFile=./log4j2.xml -cp apiguardian-api-1.1.2.jar:byte-buddy-1.9.10.jar:byte-buddy-agent-1.9.10.jar:ibmi-journal-parsing-3.0.4.Final.jar:jt400-jdk9-11.1.jar:jt400-override-ccsid-3.0.4.Final.jar:junit-jupiter-5.9.1.jar:junit-jupiter-api-5.9.1.jar:junit-jupiter-engine-5.9.1.jar:junit-jupiter-params-5.9.1.jar:junit-platform-commons-1.9.1.jar:junit-platform-engine-1.9.1.jar:log4j-1.2-api-2.23.1.jar:log4j-api-2.23.1.jar:log4j-core-2.23.1.jar:log4j-jpl-2.23.1.jar:log4j-jul-2.23.1.jar:log4j-slf4j-impl-2.23.1.jar:mockito-core-3.0.0.jar:mockito-junit-jupiter-4.6.1.jar:objenesis-2.6.jar:opentest4j-1.2.0.jar:slf4j-api-1.7.36.jar io.debezium.ibmi.db2.journal.test.missing.TestEndOffsets
