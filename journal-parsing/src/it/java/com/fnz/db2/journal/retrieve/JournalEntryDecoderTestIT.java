package com.fnz.db2.journal.retrieve;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF.Structure;
import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.test.TestConnector;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Text;


// this file is utf-8

class JournalEntryDecoderTestIT {
    private static final Logger log = LoggerFactory.getLogger(JournalEntryDecoderTestIT.class);

	static Connect<AS400, IOException> as400Connect;
	static Connect<Connection, SQLException> sqlConnect;
	
	static String schema = "MSTYPE1";

	@BeforeAll
	public static void setup() throws Exception {
        TestConnector connector = new TestConnector();
        as400Connect = connector.getAs400();
        sqlConnect = connector.getJdbc();
        
		setupSchema();
	}

	private static void setupSchema() throws SQLException {
		Connection con = sqlConnect.connection(); // debezium doesn't close connections
		try (Statement stmt = con.createStatement()) {
			try (ResultSet rs = stmt
					.executeQuery("select count(1) from qsys2.systables where table_schema='MSTYPE1'")) {
				if (rs.next()) {
					int found = rs.getInt(1);
					if (found < 1) {
						log.debug("create schema");
						stmt.executeUpdate("create schema MSTYPE1");
					}
				}
			}

			log.debug("create schema");
		}
	}

	@Test
	public void testCharacterTypes() throws Exception {
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
		JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema);
		JournalPosition position = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);

		Connection con = sqlConnect.connection(); // debezium doesn't close connections
		try (Statement stmt = con.createStatement()) {
			stmt.executeUpdate("drop TABLE MSTYPE1.CHARTYPES");

			stmt.executeUpdate(
					"CREATE OR replace TABLE MSTYPE1.CHARTYPES (  " + "fchar CHAR(12), vchar VARCHAR(11),    "
//							+ "fgraph GRAPHIC(10) CCSID 13488,  vgraph VARGRAPHIC(10) CCSID 13488,  "
							+ "fbin binary(10),  vbin varbinary(10)  )");

			stmt.executeUpdate("delete from MSTYPE1.chartypes");

			// bin is ebidic a385a2a3 not ascii 74657374
			stmt.executeUpdate("INSERT INTO MSTYPE1.CHARTYPES (fchar, vchar, "
//					+ "fgraph, vgraph, "
					+ "fbin, vbin) "
					+ "VALUES ('fixed', 'var',"
//					+ " 'Paßstraße', 'Maſʒſtab', "
					+ "CAST('abcdefghij' AS BINARY(10)) , CAST('klnmopqrst' AS VARBINARY(10)))");

			// verify we can find the data using sql
			try (ResultSet rs = stmt
					.executeQuery("select fchar, vchar, "
//							+ "fgraph, vgraph, "
							+ "fbin, vbin from MSTYPE1.CHARTYPES")) {
				AS400Text conv = new AS400Text(10, 37);
				if (rs.next()) {
					assertThat(rs.getString("fchar").trim(), equalTo("fixed"));
					assertThat(rs.getString("vchar").trim(), equalTo("var"));
//					assertThat(rs.getString("fgraph").trim(), is(equalTo("Paßstraße")));
//					assertThat(rs.getString("vgraph").trim(), is(equalTo("Maſʒſtab")));
					assertThat(rs.getBytes("fbin"), equalTo(conv.toBytes("abcdefghij")));
					assertThat(rs.getBytes("vbin"), equalTo(conv.toBytes("klnmopqrst")));
				} else {
					fail("should find a result");
				}
			}
		}

		RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal).build();
		RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
		
		
		SchemaCacheHash schemaHash = new SchemaCacheHash();

        String database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
        JdbcFileDecoder fileDecoder = new JdbcFileDecoder(sqlConnect, database, schemaHash);
		boolean foundInsert = false;
		for (int i = 0; i < 10 && !foundInsert; i++) { // journal entries are not available straight away
			boolean success = rj.retrieveJournal(position);
			// verify that the journal entry can be decoded
			while (success && !foundInsert && rj.nextEntry()) {
				EntryHeader eheader = rj.getEntryHeader();
				log.debug(eheader.toString());
                JournalEntryType journalEntryType = eheader.getJournalEntryType();
                if (journalEntryType == null)
                    continue;
				switch (journalEntryType) {
    				case ADD_ROW2, ADD_ROW1:
    					Object[] values = rj.decode(fileDecoder);
    					AS400Text conv = new AS400Text(10, 37);
    					TableInfo ti = schemaHash.retrieve(database, schema, "CHARTYPES");
    					assertNotNull(ti);
    					List<Structure> structures = ti.getStructure();
    					Map<String, Object> map = toValues(structures, values);
    					assertThat(((String)map.get("FCHAR")).trim(), equalTo("fixed"));
    					assertThat(((String)map.get("VCHAR")).trim(), equalTo("var"));
    //						assertThat(((String)map.get("FGRAPH")).trim(), is(equalTo("Paßstraße")));
    //						assertThat(((String)map.get("VGRAPH")).trim(), is(equalTo("Maſʒſtab")));
    					assertThat(((byte[])map.get("FBIN")), equalTo(conv.toBytes("abcdefghij")));
    					assertThat(((byte[])map.get("VBIN")), equalTo(conv.toBytes("klnmopqrst")));
    
    					foundInsert = true;
    					break;
					default:
					    break;
				}
				position.setPosition(rj.getPosition());
			}
		}

		assertThat("insert journal entry found", foundInsert, equalTo(true));
	}
	
	public Map<String, Object> toValues(List<Structure> structures, Object[] values) {
		Map<String, Object> map = new HashMap<>();
		
		for (int i=0; i< structures.size(); i++) {
			map.put(structures.get(i).getName(), values[i]);
		}
		return map;
	}

}
