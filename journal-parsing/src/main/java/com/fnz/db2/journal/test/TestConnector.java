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
        String user =  System.getenv("ISERIES_USER");
        String password =  System.getenv("ISERIES_PASSWORD");
        String host =  System.getenv("ISERIES_HOST");
        this.schema =  System.getenv("ISERIES_SCHEMA");
        String ccsid =  System.getenv("FORCED_CCSID");
        
        
        String url = String.format("jdbc:as400:%s", host);
        
        final Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
    	if (ccsid != null) {
    		AS400JDBCDriverRegistration.registerCcsidDriver();
    		props.setProperty(AS400JDBCConnectionForcedCcsid.FORCED_CCSID, ccsid);
    		log.info("forced ccsid " + ccsid);
    	}

        this.as400Connect = new Connect<AS400, IOException>() {
            AS400 as400 = new AS400(host, user, password);
            @Override
            public AS400 connection() {
                return as400;
            }
        };
        
        this.sqlConnect = new Connect<Connection, SQLException>() {
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
