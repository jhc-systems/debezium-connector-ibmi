/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheHash;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;

public class CcsidDiscovery {
    private static Logger log = LoggerFactory.getLogger(CcsidDiscovery.class);

    static TestConnector connector;
    static Connect<Connection, SQLException> sqlConnect;
    static String database;

    static String table;

    public static void main(String[] args) throws Exception {
        log.info("new");
        connector = new TestConnector();
        sqlConnect = connector.getJdbc();
        table = System.getenv("TABLE");
        database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
        testToDataType();
    }

    public static void testToDataType() throws Exception {
        final JdbcFileDecoder decoder = new JdbcFileDecoder(sqlConnect, database, new SchemaCacheHash(), -1, -1);
        final Optional<TableInfo> info = decoder.getRecordFormat(table, connector.getSchema());

        log.info("table info {}", info.get());
        log.info("primary key {}", info.get().getPrimaryKeys());
    }

}