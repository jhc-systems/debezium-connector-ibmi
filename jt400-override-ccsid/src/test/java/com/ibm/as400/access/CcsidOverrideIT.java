package com.ibm.as400.access;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CcsidOverrideIT {
	private static final Logger log = Logger.getLogger(CcsidOverrideIT.class.toString());

	private static final int columnCcsid = 37;
	Charset columnCcsidCP = Charset.forName(String.format("CP%03d", columnCcsid));
	private static final String data = "£$[^~?¢¯€123";
	private static final int wrongCcsid = 1146;
	Charset wrongCcsidCP = Charset.forName(String.format("CP%03d", wrongCcsid));

	@Test
	public void ccsidOverrideTest() throws Exception {
		AS400JDBCDriverRegistration.registerCcsidDriver();

		final Properties props = configurePropertiesForDriver(columnCcsid, wrongCcsid);
		final String host = System.getenv("ISERIES_HOST");
		final String schema = System.getenv("ISERIES_SCHEMA");

		try (Connection con = DriverManager.getConnection(String.format("jdbc:as400://%s/", host), props)) {
			assertEquals(com.ibm.as400.access.AS400JDBCConnectionForcedCcsid.class, con.getClass());
			try (PreparedStatement ps = con
					.prepareStatement(String.format("select name from %s.ccsidTest where id = ?", schema))) {
				ps.setInt(1, 1);
				try (ResultSet rs = ps.executeQuery()) {
					assertTrue(rs.next(), "found inserted data");
					final String retrieved = rs.getString(1).trim();

					final byte[] retrievedBytes = rs.getBytes(1); // retrieves unencoded
					final byte[] expectedBytes = data.getBytes(wrongCcsidCP);

					log.log(Level.FINE, "sent     {0}", data);
					log.log(Level.FINE, "received {0}", retrieved);
					log.log(Level.FINE, "expected ebdic  {0}", bytesToHex(expectedBytes));
					log.log(Level.FINE, "recieved ebdic  {0}", bytesToHex(retrievedBytes));

					assertEquals(data, retrieved);
					assertArrayEquals(expectedBytes, retrievedBytes);

				}
			}
		}
	}

	@Test
	public void ccsidOriginalTest() throws Exception {
		AS400JDBCDriverRegistration.registerOriginalDriver();

		final Properties props = configurePropertiesForDriver(columnCcsid, wrongCcsid);
		final String host = System.getenv("ISERIES_HOST");
		final String schema = System.getenv("ISERIES_SCHEMA");

		try (Connection con = DriverManager.getConnection(String.format("jdbc:as400://%s/", host), props)) {
			assertEquals(com.ibm.as400.access.AS400JDBCConnectionImpl.class, con.getClass());
			try (PreparedStatement ps = con
					.prepareStatement(String.format("select name from %s.ccsidTest where id = ?", schema))) {
				ps.setInt(1, 1);
				try (ResultSet rs = ps.executeQuery()) {
					assertTrue(rs.next(), "found inserted data");
					final String retrieved = rs.getString(1).trim();

					final byte[] retrievedBytes = rs.getBytes(1); // retrieves unencoded
					final byte[] expectedBytes = data.getBytes(wrongCcsidCP);
					final String expectedString = new String(expectedBytes, columnCcsidCP);

					log.log(Level.FINE, "sent     {0}", data);
					log.log(Level.FINE, "received {0}", retrieved);
					log.log(Level.FINE, "expected {0}", expectedString);
					log.log(Level.FINE, "expected ebdic  {0}", bytesToHex(expectedBytes));
					log.log(Level.FINE, "recieved ebdic  {0}", bytesToHex(retrievedBytes));

					assertEquals(expectedString, retrieved);
					assertArrayEquals(expectedBytes, retrievedBytes);

				}
			}
		}
	}

	private static void insertData(Connection con, String schema) throws SQLException {
		log.info(con.getClass().toString());
		try (PreparedStatement ps = con
				.prepareStatement(String.format("insert into %s.ccsidTest (id, name) values (?, ?)", schema))) {
			ps.setInt(1, 1);
			ps.setString(2, data);
			final int updated = ps.executeUpdate();
			assertEquals(1, updated, "inserted one row");
		}
		con.commit();
	}

	private static Properties configurePropertiesForDriver(int fromCcsid, int toCcsid) {
		final String user = System.getenv("ISERIES_USER");
		final String password = System.getenv("ISERIES_PASSWORD");
		final String schema = System.getenv("ISERIES_SCHEMA");

		final Properties props = new Properties();
		props.setProperty("from_ccsid", Integer.toString(fromCcsid));
		props.setProperty("to_ccsid", Integer.toString(toCcsid));
		props.setProperty("user", user);
		props.setProperty("password", password);
		props.setProperty("errors", "full");
		props.setProperty("prompt", "false");
		props.setProperty("libraries", schema);
		props.setProperty("naming", "system");
		return props;
	}

	@BeforeAll
	public static void outputUtf8OnWindows() throws UnsupportedEncodingException {
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8"));
	}

	@BeforeAll
	public static void setupTable() throws SQLException {
		AS400JDBCDriverRegistration.registerCcsidDriver();
		final Properties props = configurePropertiesForDriver(columnCcsid, wrongCcsid);
		final String host = System.getenv("ISERIES_HOST");
		final String schema = System.getenv("ISERIES_SCHEMA");

		try (Connection con = DriverManager.getConnection(String.format("jdbc:as400://%s/", host), props)) {
			dropTable(con, host, schema);
			createTable(con, host, schema);
			insertData(con, schema);
		}
	}

	@AfterAll
	public static void cleanUp() throws SQLException {
		AS400JDBCDriverRegistration.registerCcsidDriver();
		final Properties props = configurePropertiesForDriver(columnCcsid, wrongCcsid);
		final String host =  System.getenv("ISERIES_HOST");
		final String schema =  System.getenv("ISERIES_SCHEMA");

		try (Connection con = DriverManager.getConnection(String.format("jdbc:as400://%s/", host), props)) {
			dropTable(con, host, schema);
		}
	}

	private static void createTable(Connection con, String host, String schema) throws SQLException {
		try (PreparedStatement ps = con.prepareStatement(
				String.format("create table %s.ccsidTest (id int, name varchar(200) CCSID %d)", schema, columnCcsid))) {
			ps.execute();
		}
	}

	private static void dropTable(Connection con, String host, String schema) {
		try (PreparedStatement ps = con.prepareStatement(String.format("drop table %s.ccsidTest", schema))) {
			ps.execute();
		} catch (final Exception e) {
			// we can ignore if it doesn't exist - it normally wont on start but it is
			// always good to have it in the right state
		}
	}

	private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

	public static String bytesToHex(byte[] bytes) {
		final byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			final int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars, StandardCharsets.UTF_8);
	}
}
