package com.ibm.as400.access;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AS400JDBCDriverRegistration {
	public static void registerCcsidDriver() throws SQLException {
		// remove old driver
		try {
			while (true) {
				Driver remove = DriverManager.getDriver("jdbc:as400://host");
				DriverManager.deregisterDriver(remove);
			}
		} catch (SQLException e) {
		}
		DriverManager.registerDriver(new AS400JDBCDriverForcedCcsid());		
	}
	
	public static void registerOriginalDriver() throws SQLException {
		// remove old driver
		try {
			while (true) {
				Driver remove = DriverManager.getDriver("jdbc:as400://host");
				DriverManager.deregisterDriver(remove);
			}
		} catch (SQLException e) {
		}
		DriverManager.registerDriver(new AS400JDBCDriver());		
	}
}
