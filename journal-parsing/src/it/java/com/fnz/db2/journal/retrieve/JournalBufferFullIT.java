package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.retrieve.rjne0200.OffsetStatus;
import com.fnz.db2.journal.test.TestConnector;
import com.ibm.as400.access.AS400;

class JournalBufferFullIT {
	private static final Logger log = LoggerFactory.getLogger(JournalBufferFullIT.class);

	private static final int MAX_BUFFER_SIZE = 400;
	private static final int MAX_UPDATES = 50;

	private static SchemaCacheHash schemaCache = new SchemaCacheHash();
	static JdbcFileDecoder fileDecoder = null;


	static TestConnector connector = null;
	static String database;
	static JournalInfo journal;

	static final String SCHEMA = "JRN_TEST4"; // note upper case
	static final String TABLE = "SEQ1";
	int nextValue = 0;
	boolean continuationSet = false;
	final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
	final static List<FileFilter> includes = List.of(new FileFilter(SCHEMA, TABLE));

	List<Integer> expected = new LinkedList<>();
	List<Integer> seen = new LinkedList<>();

	@BeforeAll
	public static void setup() throws Exception {
		log.info("setting up");
		connector = new TestConnector(SCHEMA);
		journal = JournalInfoRetrieval.getJournal(connector.getAs400().connection(), SCHEMA,
				includes);
		final Connection con = connector.getJdbc().connection();
		database = JdbcFileDecoder.getDatabaseName(con);
		setupTables(con);
		fileDecoder = new JdbcFileDecoder(connector.getJdbc(), database, schemaCache, -1);
		log.info("set up");
	}

	//
	@AfterAll
	public static void teardown() throws Exception {
		log.info("cleaning up");
		cleanup(connector.getJdbc().connection(), connector.getAs400().connection(), journal);
		log.info("finished");

	}

	@Test
	void test() {
		System.out.println("");
	}

	public JournalBufferFullIT() {
	}

	@Test
	void findAllWithContinuationOffset() throws Exception {
		JournalProcessedPosition lastPosition = null;
		final Connect<AS400, IOException> as400Connect = connector.getAs400();

		log.info("journal {}", journal);
		final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
		JournalProcessedPosition nextPosition = new JournalProcessedPosition(endPosition, Instant.now(), true);

		final AtomicBoolean active = new AtomicBoolean(true);

		log.info("insert data");
		insertRealData(connector, 51, MAX_UPDATES, journal, active);

		log.info("process entries");

		final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
				.withJournalBufferSize(MAX_BUFFER_SIZE).withServerFiltering(true).withIncludeFiles(includes)
				.withMaxServerSideEntries(1000).build();
		final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
		} while (!nextPosition.equals(lastPosition));
		log.info("verify");
		assertEquals(true, continuationSet);
		assertEquals(MAX_UPDATES, nextValue);
	}

	private JournalProcessedPosition retrieveJorunal(Connect<AS400, IOException> connector, JournalInfo journal,
			RetrieveJournal r, JournalProcessedPosition position) throws Exception {

		final boolean success = r.retrieveJournal(position);
		log.info("success: {} position: {} header {}", success, position, r.getFirstHeader());

		if (success) {
			continuationSet |= r.getFirstHeader().status() == OffsetStatus.MORE_DATA_NEW_OFFSET;
			while (r.nextEntry()) {
				final EntryHeader eheader = r.getEntryHeader();

				position.setPosition(r.getPosition());
				final JournalEntryType entryType = eheader.getJournalEntryType();
				// log.info("{}.{}", eheader.getJournalCode(), eheader.getEntryType());

				if (entryType == null) {
					continue;
				}
				final String file = eheader.getFile();
				final String lib = eheader.getLibrary();
				final String member = eheader.getMember();

				assertEquals(TABLE, file);
				// log.info("receiver {}", eheader.getReceiver());

				switch (entryType) {
				case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE:
					if (!"DL".equals(eheader.getEntryType()) && !"DR".equals(eheader.getEntryType())) {
						// String recordFileName = "/QSYS.LIB/" + lib + ".LIB/" + file + ".FILE/" +
						// member + ".MBR";
						final Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(),
								eheader.getLibrary());
						tableInfoOpt.ifPresent(tableInfo -> {
							try {
								final Object[] fields = r.decode(fileDecoder);

								assertEquals("VALUE", tableInfo.getStructure().get(1).getName());
								assertEquals(nextValue, (Integer) fields[1]);
								final int i = (Integer) fields[1];
								seen.add(i);
								// log.info("found {} expecting {} {} {}", i, nextValue, member,
								// r.getPosition());
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

	void insertRealData(TestConnector connector, int batchSize, int size, JournalInfo journal, AtomicBoolean active) {
		try {
			final Connection con = connector.getJdbc().connection();
			try (final PreparedStatement ps = con.prepareStatement("update JRN_TEST4.SEQ1 set value=?")) {
				for (int i = 0; i < size;) {
					for (int j = 0; j < batchSize; j++) {
						expected.add(i);
						ps.setInt(1, i);
						ps.addBatch();
						i++;
					}
					ps.executeBatch();
				}
			}
		} catch (final Exception e) {
			log.error("inserting failed", e);
		}
	}

	private static void setupTables(Connection con) throws SQLException {
		ITUtilities.truncateAllTables(con, SCHEMA); // if tables still exist
		ITUtilities.createSchema(con, SCHEMA);

		try (final Statement st = con.createStatement()) {
			st.executeUpdate("CREATE OR replace TABLE JRN_TEST4.SEQ1 (id int, value int, primary key (id))");
			st.executeUpdate("insert into JRN_TEST4.SEQ1 (id, value) values (1, 1)");
		}
	}


	static void cleanup(Connection con, AS400 as400, JournalInfo journal) throws Exception {
		ITUtilities.deleteReceiversDropSchema(con, as400, journal);
	}
}
