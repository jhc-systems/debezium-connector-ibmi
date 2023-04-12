package com.fnz.db2.journal.retrieve;




import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.test.TestConnector;

public class CcsidDiscovery {

	static TestConnector connector;
	static Connect<Connection, SQLException> sqlConnect;
	static String database;
	
	static String table;

	public static void main(String[] args) throws Exception {
        connector = new TestConnector();
        sqlConnect = connector.getJdbc();
        table =  System.getenv("TABLE");
        database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
        testToDataType();
	}



	public static void testToDataType() throws Exception {
		JdbcFileDecoder decoder = new JdbcFileDecoder(sqlConnect, database, new SchemaCacheHash(), -1);
		Optional<TableInfo> info = decoder.getRecordFormat(table, connector.getSchema());

		System.out.println(info.get());
		System.out.println(info.get().getPrimaryKeys());
	}
	
}