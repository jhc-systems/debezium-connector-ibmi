/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.ibm.as400.access;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AS400JDBCDriverRegistration {
    public static void registerCcsidDriver() throws SQLException {
        // remove old driver
        try {
            while (true) {
                final Driver remove = DriverManager.getDriver("jdbc:as400://host");
                DriverManager.deregisterDriver(remove);
            }
        }
        catch (final SQLException e) {
        }
        DriverManager.registerDriver(new AS400JDBCDriverForcedCcsid());
    }

    public static void registerOriginalDriver() throws SQLException {
        // remove old driver
        try {
            while (true) {
                final Driver remove = DriverManager.getDriver("jdbc:as400://host");
                DriverManager.deregisterDriver(remove);
            }
        }
        catch (final SQLException e) {
        }
        DriverManager.registerDriver(new AS400JDBCDriver());
    }
}
