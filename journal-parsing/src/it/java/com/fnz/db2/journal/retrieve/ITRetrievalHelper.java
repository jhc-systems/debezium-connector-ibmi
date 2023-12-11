package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.test.TestConnector;
import com.ibm.as400.access.AS400;

public class ITRetrievalHelper {
	private static final Logger log = LoggerFactory.getLogger(ITRetrievalHelper.class);
	TestConnector connector;
	JournalInfo journal;
	final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
	List<FileFilter> includes;
	Set<String> tables;
	int bufferSize;
	int maxEntries;
	TriFunction<Object[], TableInfo, String, Void> journalEntryFunction;
	Function<RetrieveJournal, Void> fetchedEntries;
	JdbcFileDecoder fileDecoder;
	SchemaCacheHash schemaCache = new SchemaCacheHash();
	JournalProcessedPosition nextPosition;
	RetrieveJournal rj;

	public ITRetrievalHelper(TestConnector connector, JournalInfo journal, List<FileFilter> includes, int bufferSize,
			int maxEntries, TriFunction<Object[], TableInfo, String, Void> journalEntryFunction,
			Function<RetrieveJournal, Void> fetchedEntries) {
		this.connector = connector;
		this.journal = journal;
		this.includes = includes;
		this.bufferSize = bufferSize;
		this.maxEntries = maxEntries;
		this.journalEntryFunction = journalEntryFunction;
		tables = includes.stream().map(FileFilter::table).collect(Collectors.toSet());
		fileDecoder = new JdbcFileDecoder(connector.getJdbc(), "databasename", schemaCache, -1, -1);
		this.fetchedEntries = fetchedEntries;
		final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(connector.getAs400())
				.withJournalInfo(journal).withJournalBufferSize(bufferSize).withServerFiltering(true)
				.withIncludeFiles(includes).withMaxServerSideEntries(maxEntries).build();
		rj = new RetrieveJournal(config, journalInfoRetrieval);
	}

	public void setStartPositionToNow() throws Exception {
		final Connect<AS400, IOException> as400Connect = connector.getAs400();
		final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
		nextPosition = new JournalProcessedPosition(endPosition, Instant.now(), true);
	}

	public void retrieveJournalEntries(Function<Boolean, Boolean> finished) throws Exception {
		JournalProcessedPosition lastPosition = null;

		log.debug("process entries");
		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJournal(rj, nextPosition);
		} while (finished.apply(!nextPosition.equals(lastPosition)));
	}

	JournalProcessedPosition retrieveJournal(RetrieveJournal r, JournalProcessedPosition position) throws Exception {

		final boolean success = r.retrieveJournal(position);
		log.debug("success: {} position: {} header {}", success, position, r.getFirstHeader());

		if (success) {
			fetchedEntries.apply(r);
			while (r.nextEntry()) {
				final EntryHeader eheader = r.getEntryHeader();

				position.setPosition(r.getPosition());
				final JournalEntryType entryType = eheader.getJournalEntryType();

				if (entryType == null) {
					continue;
				}
				final String file = eheader.getFile();
				final String lib = eheader.getLibrary();

				log.info("code {}.{} table {}.{} sequence {}", eheader.getJournalCode(), eheader.getEntryType(), lib,
						file, eheader.getSequenceNumber());

				assertTrue(tables.contains(file), "filtering working");

				switch (entryType) {
				case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE: {
					final Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(),
							eheader.getLibrary());
					tableInfoOpt.ifPresentOrElse(tableInfo -> {
						try {
							log.info("found data structure");
							final Object[] fields = r.decode(fileDecoder);
							journalEntryFunction.apply(fields, tableInfo, file);
						} catch (final Exception e) {
							log.error("failed to decode journal entry", e);
							fail("faild to decode journal entry");
						}
					}, () -> {
						log.error("unexpected empty table info");
					});
				}
				case BEFORE_IMAGE:
					break;
				default:
					log.error("unexpected journal entry type {}", entryType);
					break;
				}

			}
			position.setPosition(r.getPosition());
		}
		return position;
	}
}
