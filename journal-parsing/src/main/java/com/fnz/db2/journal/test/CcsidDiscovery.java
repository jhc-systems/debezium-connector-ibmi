package com.fnz.db2.journal.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
import com.fnz.db2.journal.retrieve.SchemaCacheHash;
import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;

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
        table =  System.getenv("TABLE");
        database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
        testToDataType();
	}



	public static void testToDataType() throws Exception {
		JdbcFileDecoder decoder = new JdbcFileDecoder(sqlConnect, database, new SchemaCacheHash(), -1);
		Optional<TableInfo> info = decoder.getRecordFormat(table, connector.getSchema());

		log.info("table info {}", info.get());
		log.info("primary key {}", info.get().getPrimaryKeys());
	}
	
}