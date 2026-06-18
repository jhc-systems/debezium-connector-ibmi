package io.debezium.connector.db2as400;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;

class As400ConnectorConfigTest {
	As400ConnectorConfig as400config;

    public static final Field DATABASE_FROM_CCSID = Field.create("driver.from.ccsid", "from ccsid",
            "when the table indicates this from_ccsid translate to the to_ccsid setting", -1);
    public static final Field DATABASE_TO_CCSID = Field.create("driver.to.ccsid", "to ccsid",
            "when the table indicates this from_ccsid translate to the to_ccsid setting", -1);

	
    @Test
	protected void testDatabaseCcsid() throws Exception {
		Configuration config = Configuration.create()
				.with(DATABASE_FROM_CCSID, "from")
				.with(DATABASE_TO_CCSID, "to")
				.build();
		as400config = new As400ConnectorConfig(config); 
		JdbcConfiguration jdbcConfig = as400config.getJdbcConfig();
		
		assertEquals("to", jdbcConfig.getString("to.ccsid"));
		assertEquals("from", jdbcConfig.getString("from.ccsid"));
	}

    @Test
	protected void testRootCcsid() throws Exception {
		Configuration config = Configuration.create()
				.with(As400ConnectorConfig.FROM_CCSID, "123")
				.with(As400ConnectorConfig.TO_CCSID, "321")
				.build();
		as400config = new As400ConnectorConfig(config); 
		JdbcConfiguration jdbcConfig = as400config.getJdbcConfig();
		
		assertEquals("123", jdbcConfig.getString("from.ccsid"));
		assertEquals("321", jdbcConfig.getString("to.ccsid"));
	}
}
