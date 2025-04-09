/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.test;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.PositionRange;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/** Test stub **/
public class ListSequences {

    private static final Logger log = LoggerFactory.getLogger(ListSequences.class);

    static FileWriter orderids;

    public static void main(String[] args) throws Exception {
        final TestConnector connector = new TestConnector();
        final Connect<AS400, IOException> as400Connect = connector.getAs400();
        final String schema = connector.getSchema();

        final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();

        final List<FileFilter> includes = new ArrayList<>();
        final String includesEnv = System.getenv("ISERIES_INCLUDES");
        if (includesEnv != null) {
            for (final String i : Arrays.asList(includesEnv.split(","))) {
                includes.add(new FileFilter(schema, i));
            }
        }

        final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);

        final String startOffset = System.getenv("ISERIES_START_OFFSET");
        final String startReceiver = System.getenv("ISERIES_START_RECEIVER");
        JournalProcessedPosition startPosition = new JournalProcessedPosition(startOffset, startReceiver, journal.journalLibrary(),
                Instant.ofEpochSecond(0), false);

        final String endOffset = System.getenv("ISERIES_END_OFFSET");
        final String endReceiver = System.getenv("ISERIES_END_RECEIVER");
        JournalPosition endPosition = new JournalPosition(new BigInteger(endOffset), new JournalReceiver(endReceiver, journal.journalLibrary()));

        log.info("journal: {} from {} to {}", journal, startPosition, endPosition);

        final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
                .withServerFiltering(true).withIncludeFiles(includes)
                .withMaxServerSideEntries(10000).build();
        final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);

        PositionRange range = new PositionRange(startPosition, endPosition);

        final boolean success = rj.retrieveJournal(startPosition, range);
        log.info("success: {} position: {} header {}", success, startPosition, rj.getFirstHeader());

        if (success) {
            log.info("more journal data: {}", rj.futureDataAvailable());
            while (rj.nextEntry()) {
                final EntryHeader eheader = rj.getEntryHeader();
                log.info("Entry: {}", eheader);

                final JournalEntryType entryType = eheader.getJournalEntryType();

                if (entryType == null) {
                    continue;
                }

                final String file = eheader.getFile();
                final String lib = eheader.getLibrary();
                final String member = eheader.getMember();

                switch (entryType) {
                    case DELETE_ROW, ROLLBACK_DELETE_ROW:
                        log.debug("deleted row lib: {} file: {} member: {}", lib, file, member);
                        break;
                    case ADD_ROW2, ADD_ROW1:
                        log.debug("add row lib: {} file: {} member: {}", lib, file, member);
                        break;
                    case BEFORE_IMAGE, ROLLBACK_BEFORE_IMAGE:
                        log.debug("update row old values lib: {} file: {} member: {}", lib, file, member);
                        break;
                    case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE:
                        log.debug("update row new values lib: {} file: {} member: {}", lib, file, member);
                        break;
                    default:
                        break;
                }

            }

            log.info("next offset == {}", rj.getPosition());
        }
        else {
            log.info("failed");
            System.exit(-1);
        }
    }

}
