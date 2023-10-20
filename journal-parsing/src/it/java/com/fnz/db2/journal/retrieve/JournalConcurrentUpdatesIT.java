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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

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

class JournalConcurrentUpdatesIT {

	// TODO test journal changeover

	private static final int MAX_UPDATES = 100;

	private static SchemaCacheHash schemaCache = new SchemaCacheHash();
	static JdbcFileDecoder fileDecoder = null;

	private static final Logger log = LoggerFactory.getLogger(JournalConcurrentUpdatesIT.class);

	static TestConnector connector = null;
	static String database;

	static final String SCHEMA = "JRN_TEST4"; // note upper case
	static final String TABLE = "SEQ1";
	static final String IGNORE_TABLE = "IGNORE";
	int nextValue = 0;

	List<Integer> expected = new LinkedList<>();
	List<Integer> seen = new LinkedList<>();

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

	public JournalConcurrentUpdatesIT() {
	}

	//	@Test
	//	void findMissing() throws Exception {
	//		final AS400 as400 = connector.getAs400().connection();
	//		// as400Command(as400, "sudo/powerup");
	//
	//		JournalProcessedPosition lastPosition = null;
	//		final Connect<AS400, IOException> as400Connect = connector.getAs400();
	//		final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
	//
	//		final String schema = connector.getSchema();
	//
	//		final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
	//		final List<FileFilter> includes = List.of(new FileFilter(SCHEMA, TABLE));
	//		final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);
	//		log.info("journal {}", journal);
	//		//		final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
	//
	//		// 76, receiver=JournalReceiver[name=QSQJRN0288
	//		final JournalPosition sp = new JournalPosition(BigInteger.valueOf(3l),
	//				new JournalReceiver("QSQJRN0288", SCHEMA));
	//		JournalProcessedPosition nextPosition = new JournalProcessedPosition(sp, Instant.now(), false);
	//
	//		//		final JournalPosition endPosition = new JournalPosition(BigInteger.valueOf(3l),
	//		//				new JournalReceiver("QSQJRN0289", SCHEMA));
	//
	//		//		final PositionRange range = new PositionRange(false, nextPosition, endPosition);
	//
	//		final AtomicBoolean active = new AtomicBoolean(true);
	//
	//		final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
	//				.withServerFiltering(true).withIncludeFiles(includes).withMaxServerSideEntries(7).build();
	//		final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
	//		final long start = System.currentTimeMillis();
	//		do {
	//			lastPosition = new JournalProcessedPosition(nextPosition);
	//			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
	//			if (nextPosition.equals(lastPosition)) {
	//				log.info("caught up");
	//			}
	//		} while (!nextPosition.equals(lastPosition));
	//		active.set(false);
	//	}

	@Test
	void countUpdates() throws Exception {
		final AS400 as400 = connector.getAs400().connection();
		// as400Command(as400, "sudo/powerup");

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

		final AtomicBoolean active = new AtomicBoolean(true);

		final Thread t = new Thread(() -> insertingRealData(connector, 51, journal, active));
		t.start();

		final Thread dt = new Thread(() -> insertingDummyData(connector, 51, journal, active));
		dt.start();

		final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
				.withServerFiltering(true).withIncludeFiles(includes).withMaxServerSideEntries(7).build();
		final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
		final long start = System.currentTimeMillis();
		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
			if (nextPosition.equals(lastPosition)) {
				log.info("caught up");
			}
		} while (System.currentTimeMillis() - start < 60000l);

		active.set(false);
		Thread.sleep(100);

		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
			if (nextPosition.equals(lastPosition)) {
				log.info("caught up");
			}
		} while (!nextPosition.equals(lastPosition));
		assertArrayEquals(new ArrayList<>(expected).toArray(), new ArrayList<>(seen).toArray(), "found all entries");
		// as400Command(as400, "sudo/powerdown");
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
			while (r.nextEntry()) {
				final EntryHeader eheader = r.getEntryHeader();

				position.setPosition(r.getPosition());
				final JournalEntryType entryType = eheader.getJournalEntryType();
				//				log.info("{}.{}", eheader.getJournalCode(), eheader.getEntryType());

				if (entryType == null) {
					continue;
				}
				final String file = eheader.getFile();
				final String lib = eheader.getLibrary();
				final String member = eheader.getMember();

				assertEquals(TABLE, file);
				//				log.info("receiver {}", eheader.getReceiver());

				switch (entryType) {
				case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE:
					if (!"DL".equals(eheader.getEntryType()) && !"DR".equals(eheader.getEntryType())) {
						//				String recordFileName = "/QSYS.LIB/" + lib + ".LIB/" + file + ".FILE/" + member + ".MBR";
						final Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(), eheader.getLibrary());
						tableInfoOpt.ifPresent(tableInfo -> {
							try {
								final Object[] fields = r.decode(fileDecoder);

								assertEquals("VALUE", tableInfo.getStructure().get(1).getName());
								assertEquals(nextValue, (Integer) fields[1]);
								final int i = (Integer) fields[1];
								seen.add(i);
								//								log.info("found {} expecting {} {} {}", i, nextValue, member, r.getPosition());
								assertEquals(nextValue, i);
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

	void insertingRealData(TestConnector connector, int maxBatchSize, JournalInfo journal, AtomicBoolean active) {
		try {
			final Connection con = connector.getJdbc().connection();
			final AS400 as400 = connector.getAs400().connection();
			final Random r = new Random();

			try (final PreparedStatement ps = con.prepareStatement("update JRN_TEST4.SEQ1 set value=?")) {
				for (int i = 0; active.get();) {

					final int n1 = r.nextInt(maxBatchSize);
					for (int j = 0; j < n1; j++) {
						expected.add(i);
						ps.setInt(1, i);
						ps.addBatch();
						i++;
					}
					ps.executeBatch();
					//					log.info("add new receiver {}", i);
					as400Command(as400, String.format("CHGJRN JRN(%s/%s) JRNRCV(*GEN) SEQOPT(*RESET)",
							journal.journalLibrary(), journal.journalName()));
					//					final int d = r.nextInt(50);
					//					Thread.sleep(d);
				}
			}
		} catch (final Exception e) {
			log.error("inserting failed", e);
		}
	}

	void insertingDummyData(TestConnector connector, int maxBatchSize, JournalInfo journal, AtomicBoolean active) {
		try {
			final Connection con = connector.getJdbc().connection();
			final AS400 as400 = connector.getAs400().connection();
			final Random r = new Random();

			try (final PreparedStatement ps = con.prepareStatement("update JRN_TEST4.IGNORE set value=?")) {
				for (int i = 0; active.get();) {

					final int n1 = r.nextInt(maxBatchSize);
					for (int j = 0; j < n1; j++) {
						ps.setInt(1, -i);
						ps.addBatch();
						i++;
					}
					ps.executeBatch();
					//					final int d = r.nextInt(50);
					//					Thread.sleep(d);
				}
			}
		} catch (final Exception e) {
			log.error("inserting failed", e);
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
							stable.executeUpdate(String.format("truncate table %s.%s", SCHEMA, table));
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
			st.executeUpdate("CREATE OR replace TABLE JRN_TEST4.SEQ1 (id int, value int, primary key (id))");
			st.executeUpdate("CREATE OR replace TABLE JRN_TEST4.IGNORE (id int, value int, primary key (id))");
			st.executeUpdate("insert into JRN_TEST4.SEQ1 (id, value) values (1, 1)");
			st.executeUpdate("insert into JRN_TEST4.IGNORE (id, value) values (0, 0)");
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
