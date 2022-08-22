/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
import com.fnz.db2.journal.retrieve.JournalEntryType;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.SchemaCacheIF;
import com.fnz.db2.journal.retrieve.exception.FatalException;
import com.fnz.db2.journal.retrieve.exception.InvalidPositionException;

import io.debezium.DebeziumException;
import io.debezium.connector.db2as400.As400RpcConnection.BlockingRecieverConsumer;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * <p>
 * A {@link StreamingChangeEventSource} A main loop polls using a RPC call for
 * new journal entries and turns them into change events.
 * </p>
 */
public class As400StreamingChangeEventSource implements StreamingChangeEventSource<As400Partition, As400OffsetContext> {
    private static final int MAX_STUCK_THREAD = 60000;
    private static final String NO_TRANSACTION_ID = "00000000000000000000";
    private JdbcFileDecoder fileDecoder;
    private long connectionTime = -1;
    private long MIN_DISCONNECT_TIME_MS = 30000;

    private static final Logger log = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);

    private HashMap<String, Object[]> beforeMap = new HashMap<>();
    private static Set<Character> alwaysProcess = Stream.of('J', 'C')
            .collect(Collectors.toCollection(HashSet::new));

    /**
     * Connection used for reading CDC tables.
     */
    private final As400RpcConnection dataConnection;
    private As400JdbcConnection jdbcConnection;

    /**
     * A separate connection for retrieving timestamps; without it, adaptive
     * buffering will not work.
     */
    private final EventDispatcher<As400Partition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final As400DatabaseSchema schema;
    private final Duration pollInterval;
    private final As400ConnectorConfig connectorConfig;
    private final Map<String, TransactionContext> txMap = new HashMap<>();
    private final String database;

    public As400StreamingChangeEventSource(As400ConnectorConfig connectorConfig, As400RpcConnection dataConnection, As400JdbcConnection jdbcConnection,
                                           EventDispatcher<As400Partition, TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, As400DatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
        this.database = jdbcConnection.getRealDatabaseName();
        this.fileDecoder = new JdbcFileDecoder(jdbcConnection, database, (SchemaCacheIF) schema);
    }

    private void cacheBefore(TableId tableId, Timestamp date, Object[] dataBefore) {
        String key = String.format("%s-%s", tableId.schema(), tableId.table());
        beforeMap.put(key, dataBefore);
    }

    private Object[] getBefore(TableId tableId, Timestamp date) {
        String key = String.format("%s-%s", tableId.schema(), tableId.table());
        Object[] dataBefore = beforeMap.remove(key);
        if (dataBefore == null) {
            log.debug("before image not found for {}", key);
        }
        else {
            log.debug("found before image for {}", key);
        }
        return dataBefore;
    }

    @Override
    public void execute(ChangeEventSourceContext context, As400Partition partition, As400OffsetContext offsetContext) throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        int retries = 0;
        WatchDog watchDog = new WatchDog(Thread.currentThread(), MAX_STUCK_THREAD);
        watchDog.start();
        try {
	        while (context.isRunning()) {
	            try {
	                try {
	                    JournalPosition before = new JournalPosition(offsetContext.getPosition());
	                    if (!dataConnection.getJournalEntries(offsetContext, processJournalEntries(partition, offsetContext), watchDog)) {
	                        log.debug("sleep");
	                        metronome.pause();
	                    }
	                    if (!offsetContext.getPosition().equals(before)) {
	                        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
	                    }
	                    retries = 0;
	                }
	                catch (FatalException e) {
	                	log.error("Unable to process offset {}", offsetContext.getPosition(), e);
	                	throw new DebeziumException("Unable to process offset " + offsetContext.getPosition(), e);
	                }
	                catch (InvalidPositionException e) {
	                    log.error("Invalid position resetting offsets to beginning", e);
	                    offsetContext.setPosition(new JournalPosition());
	                }
	                catch (InterruptedException e) {
	                    log.error("Interrupted processing offset {} retry {}", offsetContext.getPosition().toString(), retries);
	                    closeAndReconnect();
	                    retries++;
	                    metronome.pause();
	                }
	                catch (IOException | SQLNonTransientConnectionException e) { // SQLNonTransientConnectionException thrown by jt400 jdbc driver when connection errors
	                    log.error("Connection failed offset {} retry {}", offsetContext.getPosition().toString(), retries, e);
	                    closeAndReconnect();
	
	                    retries++;
	                    metronome.pause(); // throws interruptedException
	                }
	                catch (Exception e) {
	                    log.error("Failed to process offset {} retry {}", offsetContext.getPosition().toString(), retries, e);
	
	                    retries++;
	                    metronome.pause();
	                }
	            }
	            catch (InterruptedException e) { // handle InterruptedException during the exception handling
	                log.debug("Interrupted", e);
	            }
	        }
        } finally {
        	watchDog.stop();
        }
    }

    public void rateLimittedClose() {
        if (System.currentTimeMillis() - connectionTime > MIN_DISCONNECT_TIME_MS) {
            closeAndReconnect();
        }
        else {
            log.debug("Only connected since {} ignoring disconnect", new Date(connectionTime));
        }
    }

    public void closeAndReconnect() {
        try {
            dataConnection.close();
            dataConnection.connection();
        }
        catch (Exception e) {
            log.error("Failure reconnecting command", e);
        }
        try {
            jdbcConnection.close();
            jdbcConnection.connect();
        }
        catch (Exception e) {
            log.error("Failure reconnecting sql", e);
        }
        connectionTime = System.currentTimeMillis();
    }

    private BlockingRecieverConsumer processJournalEntries(As400Partition partition, As400OffsetContext offsetContext)
            throws IOException, SQLNonTransientConnectionException {
        return (nextOffset, r, eheader) -> {
            try {
                JournalEntryType journalEntryType = eheader.getJournalEntryType();

                if (journalEntryType == null || ignore(journalEntryType)) {
                    log.debug("excluding table {} entry type {}", eheader.getFile(), eheader.getEntryType());
                    return;
                }

                String longName = eheader.getFile();
                try {
                    longName = jdbcConnection.getLongName(eheader.getLibrary(), eheader.getFile());
                }
                catch (IllegalStateException e) {
                    log.error("failed to look up long name", e);
                }
                TableId tableId = new TableId(database, eheader.getLibrary(), longName);

                boolean includeTable = connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId);

                if (!alwaysProcess.contains(eheader.getJournalCode()) && !includeTable) { // always process journal J and transaction C messages
                    log.debug("excluding table {} journal code {}", tableId, eheader.getJournalCode());
                    return;
                }

                log.debug("next event: {} type: {} table: {}", eheader.getSequenceNumber(), eheader.getEntryType(), tableId.table());
                switch (journalEntryType) {
                    case START_COMMIT: {
                        // start commit
                        String txId = eheader.getCommitCycle().toString();
                        log.debug("begin transaction: {}", txId);
                        TransactionContext txc = new TransactionContext();
                        txc.beginTransaction(txId);
                        txMap.put(txId, txc);
                        log.debug("start transaction id {} tx {} table {}", nextOffset, txId, tableId);
                        dispatcher.dispatchTransactionStartedEvent(partition, txId, offsetContext);
                    }
                        break;
                    case END_COMMIT: {
                        // end commit
                        // TOOD transaction must be provided by the OffsetContext
                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.remove(txId);
                        log.debug("commit transaction id {} tx {} table {}", nextOffset, txId, tableId);
                        if (txc != null) {
                            txc.endTransaction();
                            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
                        }
                    }
                        break;
                    case FILE_CHANGE:
                    case FILE_CREATED: {
                        // table has changed - reload schema
                        fileDecoder.clearCache(tableId.table(), tableId.schema());
                        fileDecoder.getRecordFormat(tableId.table(), tableId.schema());
                    }
                        break;
                    case BEFORE_IMAGE: {
                        // before image
                        tableId.schema();
                        Object[] dataBefore = r.decode(fileDecoder);

                        cacheBefore(tableId, eheader.getTimestamp(), dataBefore);
                    }
                        break;
                    case AFTER_IMAGE: {
                        // after image
                        // before image is meant to have been immediately before
                        Object[] dataBefore = getBefore(tableId, eheader.getTimestamp());
                        Object[] dataNext = r.decode(fileDecoder);

                        offsetContext.setSourceTime(eheader.getTimestamp());

                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);

                        log.debug("update event id {} tx {} table {}", nextOffset, txId, tableId);

                        dispatcher.dispatchDataChangeEvent(partition, tableId,
                                new As400ChangeRecordEmitter(partition, offsetContext, Operation.UPDATE, dataBefore, dataNext, clock));
                    }
                        break;
                    case ADD_ROW1:
                    case ADD_ROW2: {
                        // record added
                        Object[] dataNext = r.decode(fileDecoder);
                        offsetContext.setSourceTime(eheader.getTimestamp());

                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);
                        if (txc != null) {
                            txc.event(tableId);
                        }

                        log.debug("insert event id {} tx {} table {}", offsetContext.getPosition().toString(), txId, tableId);
                        dispatcher.dispatchDataChangeEvent(partition, tableId,
                                new As400ChangeRecordEmitter(partition, offsetContext, Operation.CREATE, null, dataNext, clock));
                    }
                        break;
                    case DELETE_ROW1:
                    case DELETE_ROW2: {
                        // record deleted
                        Object[] dataBefore = r.decode(fileDecoder);

                        offsetContext.setSourceTime(eheader.getTimestamp());

                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);
                        if (txc != null) {
                            txc.event(tableId);
                        }

                        log.debug("delete event id {} tx {} table {}", offsetContext.getPosition().toString(), txId, tableId);
                        dispatcher.dispatchDataChangeEvent(partition, tableId,
                                new As400ChangeRecordEmitter(partition, offsetContext, Operation.DELETE, dataBefore, null, clock));
                    }
                        break;
                    default:
                        break;
                }
            }
            catch (IOException | SQLNonTransientConnectionException e) {
                throw e;
            }
            catch (Exception e) {
                log.error("Failed to process record", e);
            }
        };
    }

    private boolean ignore(JournalEntryType journalCode) {
        return journalCode == JournalEntryType.OPEN || journalCode == JournalEntryType.CLOSE;
    }

}
