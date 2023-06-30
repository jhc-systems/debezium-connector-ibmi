package com.fnz.db2.journal.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.FileFilter;
import com.fnz.db2.journal.retrieve.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.JournalProcessedPosition;
import com.fnz.db2.journal.retrieve.RetrieveConfig;
import com.fnz.db2.journal.retrieve.RetrieveConfigBuilder;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.SchemaCacheHash;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.ibm.as400.access.AS400;

public class JournalFilterTimeout {
	private static final Logger log = LoggerFactory.getLogger(JournalFilterTimeout.class);

	private static SchemaCacheHash schemaCache = new SchemaCacheHash();

	public static void main(String[] args) throws Exception {
		final TestConnector connector = new TestConnector();
		final Connect<AS400, IOException> as400Connect = connector.getAs400();
		final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
		final String schema = connector.getSchema();
		final List<FileFilter> includes = new ArrayList<>();
		final String includesEnv = System.getenv("ISERIES_INCLUDES");
		if (includesEnv != null) {
			for (final String i : Arrays.asList(includesEnv.split(","))) {
				includes.add(new FileFilter(schema, i));
			}
		}
		final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);

		final String offset = System.getenv("ISERIES_OFFSET");
		final String receiver = System.getenv("ISERIES_RECEIVER");


		final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
		final List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(as400Connect.connection(),
				journal);
		final DetailedJournalReceiver first = receivers.stream().min((x, y) -> x.start().compareTo(y.start())).get();
		final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(),
				journal);
		log.info("start {} end {}", first, endPosition);

		try (PrintWriter pw = new PrintWriter(new File("exceptions.txt"))) {
			log.info("journal: {}", journal);
			final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
					.withDumpFolder("./bad-journal").withServerFiltering(true).withIncludeFiles(includes).build();
			final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);

			final JournalProcessedPosition p = new JournalProcessedPosition(first.start(), first.info().name(), first.info().library(),
					Instant.ofEpochSecond(0), false);
			final long start = System.currentTimeMillis();
			final boolean success = rj.retrieveJournal(p);
			final long end = System.currentTimeMillis();

			log.info("success: {} position: {} time: {} ", success, rj.getPosition(), (end - start) / 1000.0);

		}
	}

}
