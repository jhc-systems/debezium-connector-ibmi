package com.fnz.db2.journal.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.fnz.db2.journal.retrieve.Connect;
import com.ibm.as400.access.AS400;

public class TestConnector {
    
    private Connect<AS400, IOException> as400Connect;
    private Connect<Connection, SQLException> sqlConnect;
    private final String schema;

    public TestConnector() throws SQLException {
        String user =  System.getenv("ISERIES_USER");
        String password =  System.getenv("ISERIES_PASSWORD");
        String host =  System.getenv("ISERIES_HOST");
        this.schema =  System.getenv("ISERIES_SCHEMA");
        
        
        String url = String.format("jdbc:as400:%s", host);
        
        final Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

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
