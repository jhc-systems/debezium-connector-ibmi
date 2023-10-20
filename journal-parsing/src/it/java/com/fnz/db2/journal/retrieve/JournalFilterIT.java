package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.test.TestConnector;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400SecurityException;
import com.ibm.as400.access.CommandCall;
import com.ibm.as400.access.ErrorCompletingRequestException;

class JournalFilterIT {

	// TODO test journal changeover
	// fix duplicate processing

	private static final int MAX_UPDATES = 100;

	private static SchemaCacheHash schemaCache = new SchemaCacheHash();
	static JdbcFileDecoder fileDecoder = null;

	private static final Logger log = LoggerFactory.getLogger(JournalFilterIT.class);

	static TestConnector connector = null;
	static String database;

	static final String SCHEMA = "JRN_TEST4";
	static final String TABLE = "seq1";
	static final String TABLE2 = "ignore";
	int nextValue = 0;

	Set<Integer> expected = new HashSet<>();
	Set<Integer> seen = new HashSet<>();

	@BeforeAll
	public static void setup() throws Exception {
		connector = new TestConnector(SCHEMA);
		final Connection con = connector.getJdbc().connection();
		database = JdbcFileDecoder.getDatabaseName(con);
		setupTables(con);
		fileDecoder = new JdbcFileDecoder(connector.getJdbc(), database, schemaCache, -1);
	}

	@AfterAll
	public static void teardown() throws Exception {
	}

	public JournalFilterIT() {
	}


	@Test
	void filterUpdates() throws Exception {
		System.out.println("update");
		JournalProcessedPosition lastPosition = null;
		final Connect<AS400, IOException> as400Connect = connector.getAs400();
		final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();

		final String schema = connector.getSchema();

		final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
		final List<FileFilter> includes = List.of(new FileFilter(SCHEMA, TABLE));
		final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);
		final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
		JournalProcessedPosition nextPosition = new JournalProcessedPosition(endPosition, Instant.now(), true);

		insertData(sqlConnect.connection());
		final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
				.withServerFiltering(true).withIncludeFiles(includes).withMaxServerSideEntries(7).build();
		final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
			//			log.info("after : {} previous {}", nextPosition, lastPosition);
			if (nextPosition.equals(lastPosition)) {
				log.info("caught up");
				Thread.sleep(1000);
			}
		} while (!nextPosition.equals(lastPosition));
		assertArrayEquals(new ArrayList<>(expected).toArray(), new ArrayList<>(seen).toArray(),
				"found all entries");
		System.out.println(seen);
		assertEquals(MAX_UPDATES, nextValue, "number found");
	}


	@Test
	void filterUpdatesWithNewReceivers() throws Exception {
		final AS400 as400 = connector.getAs400().connection();
		//		as400Command(as400, "sudo/powerup");

		JournalProcessedPosition lastPosition = null;
		final Connect<AS400, IOException> as400Connect = connector.getAs400();
		final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();

		final String schema = connector.getSchema();

		final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
		final List<FileFilter> includes = List.of(new FileFilter(SCHEMA, TABLE));
		final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);
		log.info("journal {}", journal);
		final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
		JournalProcessedPosition nextPosition = new JournalProcessedPosition(endPosition, Instant.now(), true);


		insertDataCreateReceivers(sqlConnect.connection(), connector.getAs400().connection(), 23, journal);
		final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
				.withServerFiltering(true).withIncludeFiles(includes).withMaxServerSideEntries(7).build();
		final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
			if (nextPosition.equals(lastPosition)) {
				log.info("caught up");
			}
		} while (!nextPosition.equals(lastPosition));
		System.out.println(seen);
		assertArrayEquals(new ArrayList<>(expected).toArray(), new ArrayList<>(seen).toArray(),
				"found all entries");
		System.out.println(seen);
		assertEquals(MAX_UPDATES, nextValue, "number found");


		//		as400Command(as400, "sudo/powerdown");
	}

	private boolean as400Command(AS400 as400, String command)
			throws AS400SecurityException, ErrorCompletingRequestException, IOException,
			InterruptedException, PropertyVetoException {
		final CommandCall call = new CommandCall(as400);
		final boolean result = call.run(command);
		log.info("connamd {}", result);
		final AS400Message[] messagelist = call.getMessageList();
		for (int j = 0; j < messagelist.length; ++j) {
			// Show each message.
			log.info("command {} reponse {}", command, messagelist[j].getText());
		}
		return result;
	}

	private JournalProcessedPosition retrieveJorunal(Connect<AS400, IOException> connector, JournalInfo journal,
			RetrieveJournal r, JournalProcessedPosition position) throws Exception {

		final boolean success = r.retrieveJournal(position);
		log.info("success: {} position: {} header {}", success, position, r.getFirstHeader());

		if (success) {
			log.info("more journal data: {}", r.futureDataAvailable());
			while (r.nextEntry()) {
				final EntryHeader eheader = r.getEntryHeader();

				position.setPosition(r.getPosition());
				final JournalEntryType entryType = eheader.getJournalEntryType();

				if (entryType == null) {
					continue;
				}
				final String file = eheader.getFile();
				final String lib = eheader.getLibrary();
				final String member = eheader.getMember();
				assertEquals("SEQ1", file);
				log.info("receiver {}", eheader.getReceiver());

				switch (entryType) {
				case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE:
					if (!"DL".equals(eheader.getEntryType()) && !"DR".equals(eheader.getEntryType())) {
						//				String recordFileName = "/QSYS.LIB/" + lib + ".LIB/" + file + ".FILE/" + member + ".MBR";
						final Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(), eheader.getLibrary());
						tableInfoOpt.ifPresent(tableInfo -> {
							try {
								final Object[] fields = r.decode(fileDecoder);

								assertEquals("VALUE", tableInfo.getStructure().get(1).getName());
								//								assertEquals(nextValue, (Integer) fields[1]);
								final int i = (Integer) fields[1];
								seen.add(i);
								System.out.println(i);
								nextValue++;
							} catch (final Exception e) {
								log.error("failed to decode journal entry {}", nextValue, e);
							}
						});
					}
				break;
				default:
					break;
				}

			}
			position.setPosition(r.getPosition());
		}
		return position;
	}

	void insertDataCreateReceivers(Connection con, AS400 as400, int recevierAfter, JournalInfo journal)
			throws SQLException, AS400SecurityException, ErrorCompletingRequestException, IOException,
			InterruptedException, PropertyVetoException {
		final Statement st = con.createStatement();
		st.executeUpdate("insert into JRN_TEST4.seq1 (id, value) values (1, 1)");
		st.executeUpdate("insert into JRN_TEST4.seq2 (id, value) values (0, 0)");
		final Random r = new Random();

		try (final PreparedStatement ps = con.prepareStatement("update JRN_TEST4.seq1 set value=?")) {
			try (final PreparedStatement ps2 = con.prepareStatement("update JRN_TEST4.seq2 set value=?")) {
				for (int i = 0; i < MAX_UPDATES; i++) {

					final int n1 = r.nextInt(51);
					for (int j = 0; j < n1 && i < MAX_UPDATES; j++) {
						log.info("insert {}", i);
						expected.add(i);
						ps.setInt(1, i);
						ps.addBatch();
						i++;
					}
					ps.executeBatch();
					randomPad(r, ps2, i);
					log.info("add new receiver {}", i);
					as400Command(as400, String.format("CHGJRN JRN(%s/%s) JRNRCV(*GEN) SEQOPT(*RESET)",
							journal.journalLibrary(), journal.journalName()));
					randomPad(r, ps2, i);
					//					if (i % recevierAfter == 0 && i != 0) {
					log.info("add new receiver {}", i);
					as400Command(as400, String.format("CHGJRN JRN(%s/%s) JRNRCV(*GEN) SEQOPT(*RESET)",
							journal.journalLibrary(),
							journal.journalName()));
					//					}
				}
			}
		}
	}

	private void randomPad(Random r, final PreparedStatement ps2, int i) throws SQLException {
		final int n = r.nextInt(51);
		log.info("padding with ignored updates {}", n);
		for (int j = 0; j <= n; j++) {
			ps2.setInt(1, -i - j);
			ps2.addBatch();
		}
		ps2.executeBatch();
	}

	void insertData(Connection con) throws SQLException {
		final Statement st = con.createStatement();
		st.executeUpdate("insert into JRN_TEST4.seq1 (id, value) values (1, 1)");
		st.executeUpdate("insert into JRN_TEST4.seq2 (id, value) values (0, 0)");
		final Random r = new Random();

		try (final PreparedStatement ps = con
				.prepareStatement("update JRN_TEST4.seq1 set value=?")) {
			try (final PreparedStatement ps2 = con
					.prepareStatement("update JRN_TEST4.seq2 set value=?")) {
				for (int i = 0; i < MAX_UPDATES; i++) {
					expected.add(i);

					ps.setInt(1, i);
					ps.executeUpdate();
					final int n = r.nextInt(51);
					log.info("padding with ignored updates {}", n);
					for (int j = 0; j <= n; j++) {
						ps2.setInt(1, -i - j);
						ps2.executeUpdate();
					}
				}
			}
		}
	}

	private static void setupTables(Connection con) throws SQLException {
		boolean exists = false;
		try (final PreparedStatement ps = con
				.prepareStatement("select table_name, SYSTEM_TABLE from qsys2.systables where table_schema=?")) {
			ps.setString(1, SCHEMA);
			try (ResultSet rs = ps.executeQuery()) {
				try (final Statement stable = con.createStatement()) {
					while (rs.next()) {
						final String table = rs.getString(1);
						final String system = rs.getString(2);
						if ("N".equals(system)) {
							stable.executeUpdate(String.format("drop table %s.%s", SCHEMA, table));
						}
					}
				}
			}
		}
		try (final PreparedStatement ps = con
				.prepareStatement("SELECT * FROM TABLE (QSYS2.OBJECT_STATISTICS(?,'LIB')) AS A")) {
			ps.setString(1, SCHEMA);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					exists = true;
				}
			}
		}
		try (final Statement st = con.createStatement()) {
			if (!exists) {
				st.executeUpdate(String.format("create schema %s", SCHEMA));
			}
			st.executeUpdate("CREATE OR replace TABLE JRN_TEST4.seq1 (id int, value int, primary key (id))");
			st.executeUpdate("CREATE OR replace TABLE JRN_TEST4.seq2 (id int, value int, primary key (id))");
		}
	}

	private static void dropSchema(Connection con) {
		try (final Statement st = con.createStatement()) {
			st.executeUpdate(String.format("drop schema %s RESTRICT", SCHEMA));
		} catch (final SQLException e) {
			log.debug("failed to drop schema {}", SCHEMA, e);
		}
	}
}
