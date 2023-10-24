package com.fnz.db2.journal.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fnz.db2.journal.retrieve.Connect;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCConnectionForcedCcsid;
import com.ibm.as400.access.AS400JDBCDriverRegistration;

public class TestConnector {
	private static final Logger log = LogManager.getLogger(TestConnector.class);
	private Connect<AS400, IOException> as400Connect;
	private Connect<Connection, SQLException> sqlConnect;
	private final String schema;

	public TestConnector() throws SQLException {
		this(System.getenv("ISERIES_SCHEMA"));
	}

	public TestConnector(String schema) throws SQLException {
		this.schema = schema;
		final String user =  System.getenv("ISERIES_USER");
		final String password =  System.getenv("ISERIES_PASSWORD");
		final String host =  System.getenv("ISERIES_HOST");
		final String fromCcsid =  System.getenv("FROM_CCSID");
		final String toCcsid =  System.getenv("TO_CCSID");


		final String url = String.format("jdbc:as400:%s", host);

		final Properties props = new Properties();
		props.setProperty("user", user);
		props.setProperty("password", password);
		if (fromCcsid != null && toCcsid != null) {
			AS400JDBCDriverRegistration.registerCcsidDriver();
			props.setProperty(AS400JDBCConnectionForcedCcsid.FROM_CCSID, fromCcsid);
			props.setProperty(AS400JDBCConnectionForcedCcsid.TO_CCSID, toCcsid);
			log.info("forced ccsid {}", toCcsid);
		}

		this.as400Connect = new Connect<>() {
			AS400 as400 = new AS400(host, user, password.toCharArray());
			@Override
			public AS400 connection() {
				return as400;
			}
		};

		this.sqlConnect = new Connect<>() {
			Connection con = DriverManager.getConnection(url, props);
			@Override
			public Connection connection() throws SQLException {
				return con;
			}

		};
	}


	public Connect<AS400, IOException> getAs400() {
		return as400Connect;
	}

	public Connect<Connection, SQLException> getJdbc() {
		return sqlConnect;
	}

	public String getSchema() {
		return this.schema;
	}
}
