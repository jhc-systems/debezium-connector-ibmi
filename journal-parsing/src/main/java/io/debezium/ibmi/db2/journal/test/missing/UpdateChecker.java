package io.debezium.ibmi.db2.journal.test.missing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheHash;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.test.TestConnector;

public class UpdateChecker {
    final String table;
    final TestConnector connector;

    private static JdbcFileDecoder fileDecoder;
    private static SchemaCacheHash schemaCache = new SchemaCacheHash();
    private static final Logger log = LoggerFactory.getLogger(UpdateChecker.class);
    RetrieveJournal rj;
    JournalProcessedPosition nextPosition;
    Random random = new Random();

    public UpdateChecker(TestConnector connector, String table) {
        this.table = table;
        this.connector = connector;
    }

    public void startChecker() throws Exception {
        UpdateTables updateTables = new UpdateTables(connector.getJdbc().connection(), connector.getSchema(), table, 0, 100);
        updateTables.initaliseTable();

        Thread checker = new Thread(() -> check());
        init();
        checker.start();
        updateTables.startUpdateThread();
    }

    private void init() throws Exception {
        final Connect<AS400, IOException> as400Connect = connector.getAs400();
        final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
        final String schema = connector.getSchema();

        Connection sqlConnection = sqlConnect.connection();

        final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();

        final List<FileFilter> includes = new ArrayList<>();
        includes.add(new FileFilter(schema, table));

        final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);

        final String database = JdbcFileDecoder.getDatabaseName(sqlConnection);

        fileDecoder = new JdbcFileDecoder(sqlConnect, database, schemaCache, -1, -1);

        log.info("journal: {}", journal);
        final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal)
                .withServerFiltering(true).withIncludeFiles(includes)
                .withMaxServerSideEntries(10000).build();
        rj = new RetrieveJournal(config, journalInfoRetrieval);

        final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
        nextPosition = new JournalProcessedPosition(endPosition, Instant.ofEpochSecond(0), false);
    }

    long last;

    private void check() {
        last = System.currentTimeMillis();
        try {
            do {
                final boolean success = rj.retrieveJournal(nextPosition);
                if (success) {
                    while (rj.nextEntry()) {
                        EntryHeader eheader = rj.getEntryHeader();
                        nextPosition.setPosition(rj.getPosition());
                        final JournalEntryType entryType = eheader.getJournalEntryType();
                        if (entryType == null) {
                            log.info("entry type null {}", eheader);
                            continue;
                        }
                        switch (entryType) {
                            case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE: {
                                final Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(),
                                        eheader.getLibrary());
                                tableInfoOpt.ifPresentOrElse(tableInfo -> {
                                    try {
                                        final Object[] fields = rj.decode(fileDecoder);
                                        final String file = eheader.getFile();
                                        processUpdate(fields, tableInfo, file, rj);
                                    }
                                    catch (final Exception e) {
                                        log.error("failed to decode journal entry", e);
                                        System.exit(1);
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

                        // log.info("file {} sequence {} ", eheader.getFile(), eheader.getSequenceNumber());
                    }
                    nextPosition.setPosition(rj.getPosition());
                }
                else {
                    log.info("success: {} header {}", success, rj.getFirstHeader());
                    log.info("********************** no data found *******************");

                }

                Thread.sleep(random.nextInt(20000));
            } while (true);
        }
        catch (Exception e) {
            log.error("checker thread failed", e);
            System.exit(1);
        }
    }

    int nextValue = 0;

    private Void processUpdate(final Object[] fields, TableInfo tableInfo, String tableName, RetrieveJournal rj) {
        int journalValue = (Integer) fields[1];
        if (nextValue != journalValue) {
            log.error("missing entry {} found {} expected {} parameters {}", nextValue, tableName, journalValue, rj.parameters());
            nextValue = journalValue + 1;
        }
        nextValue++;
        long now = System.currentTimeMillis();
        if (last + 60000 < now) {
            last = now;
            log.info("all match so far table {} count {} this requested journal sequence difference {}", tableName, nextValue, rj.offsetDifference());
        }

        return null;
    }
}
