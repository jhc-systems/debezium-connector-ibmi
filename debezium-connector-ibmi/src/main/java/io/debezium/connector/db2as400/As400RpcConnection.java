/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLNonTransientConnectionException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.FileFilter;
import com.fnz.db2.journal.retrieve.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.JournalProcessedPosition;
import com.fnz.db2.journal.retrieve.JournalReceiver;
import com.fnz.db2.journal.retrieve.RetrieveConfig;
import com.fnz.db2.journal.retrieve.RetrieveConfigBuilder;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.logging.structured.StructuredMessage;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.SocketProperties;

import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;


public class As400RpcConnection implements AutoCloseable, Connect<AS400, IOException> {
    private static Logger log = LogManager.getLogger(As400RpcConnection.class);
    private final As400StreamingChangeEventSourceMetrics streamingMetrics;

    private As400ConnectorConfig config;
    private JournalInfo journalInfo;
    private RetrieveJournal retrieveJournal;
    private AS400 as400;
    private static SocketProperties socketProperties = new SocketProperties();
    private final LogLimmiting periodic = new LogLimmiting(5 * 60 * 1000l);
    private final LogLimmiting infrequent = new LogLimmiting(60 * 60 * 1000l);
    private JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();


    public As400RpcConnection(As400ConnectorConfig config, As400StreamingChangeEventSourceMetrics streamingMetrics, List<FileFilter> includes) {
        super();
        this.config = config;
        this.streamingMetrics = streamingMetrics;
        try {
        	System.setProperty("com.ibm.as400.access.AS400.guiAvailable", "False");
        	if (includes.isEmpty()) {
        		// TODO add in parameters so this is configurable
    			journalInfo = JournalInfoRetrieval.getJournal(connection(), config.getSchema());
    		} else {
    			journalInfo = JournalInfoRetrieval.getJournal(connection(), config.getSchema(), includes);
    		}
			RetrieveConfig rconfig = new RetrieveConfigBuilder().withAs400(this)
					.withJournalBufferSize(config.getJournalBufferSize())
					.withJournalInfo(journalInfo)
					.withMaxServerSideEntries(config.getMaxServerSideEntries())
					.withServerFiltering(true)
					.withIncludeFiles(includes).build();
			retrieveJournal = new RetrieveJournal(rconfig, journalInfoRetrieval);
        }
        catch (IOException e) {
            log.error("Failed to fetch library", e);
        }
    }

    @Override
    public void close() {
        try {
            if (as400 != null) {
                log.info("Disconnecting");
                this.as400.disconnectAllServices();
            }
        }
        catch (Exception e) {
            log.debug("Problem closing connection", e);
        }

        this.as400 = null;
    }

    public boolean isValid() {
        return (as400 != null && as400.isConnectionAlive(AS400.COMMAND));

    }

    public AS400 connection() throws IOException {
        if (as400 == null || !as400.isConnectionAlive(AS400.COMMAND)) {
            log.info("create new as400 connection");
            try {
                // need to both create a new object and connect
                close();
                this.as400 = new AS400(config.getHostName(), config.getUser(), config.getPassword().toCharArray());
                socketProperties.setSoTimeout(config.getSocketTimeout());
                as400.setSocketProperties(socketProperties);
                as400.connectService(AS400.COMMAND);
            }
            catch (Exception e) {
                log.error("Failed to reconnect", e);
                throw new IOException("Failed to reconnect", e);
            }
        }
        return as400;
    }

    public JournalPosition getCurrentPosition() throws RpcException {
        try {
            JournalPosition position = journalInfoRetrieval.getCurrentPosition(connection(), journalInfo);

            return new JournalPosition(position);
        }
        catch (Exception e) {
            throw new RpcException("Failed to find offset", e);
        }
    }

    public boolean getJournalEntries(ChangeEventSourceContext context, As400OffsetContext offsetCtx, BlockingReceiverConsumer consumer, WatchDog watchDog)
            throws Exception {
        boolean success = false;
        JournalProcessedPosition position = offsetCtx.getPosition();
        success = retrieveJournal.retrieveJournal(position);

        logOffsets(position, success);
        logAllReceivers();

        watchDog.alive();

        if (success) {
            if (!retrieveJournal.hasData()) {
                noDataDiagnostics(position);
            }
            while (retrieveJournal.nextEntry() && context.isRunning()) {
                watchDog.alive();
                EntryHeader eheader = retrieveJournal.getEntryHeader();
                BigInteger processingOffset = eheader.getSequenceNumber();

                consumer.accept(processingOffset, retrieveJournal, eheader);
                // while processing journal entries getPosistion is the current position
                position.setPosition(retrieveJournal.getPosition());
            }

            // note that getPosition returns the current position or the next continuation offset after the current block
            offsetCtx.setPosition(retrieveJournal.getPosition());

        }
        else {
            // this is bad, we've probably lost data
            List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(connection(), journalInfo);
            log.error(new StructuredMessage("Failed to fetch journal entries, resetting journal to blank",
                    Map.of("position", position, 
                            "receivers", receivers)));
            offsetCtx.setPosition(new JournalProcessedPosition());
        }

        return success && retrieveJournal.futureDataAvailable();
    }
    
    private void logAllReceivers() throws IOException, Exception {
        if (infrequent.shouldLogRateLimted("all-receivers")) {
            List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(connection(), journalInfo);
            log.info(new StructuredMessage("all receivers", 
                    Map.of("receivers", receivers)));
        }
    }

    private void logOffsets(JournalProcessedPosition position, boolean success) throws IOException, Exception {
        if (periodic.shouldLogRateLimted("offsets")) {
            JournalPosition currentReceiver = getCurrentPosition();
            BigInteger behind = currentReceiver.getOffset().subtract(position.getOffset());
            streamingMetrics.setJournalOffset(currentReceiver.getOffset());
            streamingMetrics.setJournalBehind(behind);
            log.info(new StructuredMessage("current position diagnostics", 
                    Map.of("header", retrieveJournal.headerAsString(),
                            "behind", behind,
                            "position", position,
                            "currentReceiver", currentReceiver,
                            "success", success)));

        }
    }

    private void noDataDiagnostics(JournalProcessedPosition position) throws Exception, IOException {
        JournalReceiver currentReceiver = JournalInfoRetrieval.getReceiver(connection(), journalInfo);
        if (!isLatestJournal(position, currentReceiver)) {
            List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(connection(), journalInfo);
            int size = receivers.size();
            if (size > 10) {
            	receivers = receivers.subList(size-10, size);
            }
            log.warn(new StructuredMessage("Detected newer receiver but no data received", 
                    Map.of("header", retrieveJournal.headerAsString(),
                            "position", position,
                            "currentReceiver", currentReceiver,
                            "lastTenReceivers", receivers)));
        }
        else {
            if (periodic.shouldLogRateLimted("no-data")) {
                List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(connection(), journalInfo);
                log.info(new StructuredMessage("We didn't get any data", Map.of("header", retrieveJournal.headerAsString(), "position", position, "receivers", receivers)));
            }
        }
    }

    private boolean isLatestJournal(JournalProcessedPosition position, JournalReceiver journalNow)
            throws Exception, IOException {
        if (position.getReceiver() == null
                || journalNow.equals(position.getReceiver())) {
            return true;
        }
        return false;
    }

    public static interface BlockingReceiverConsumer {
        void accept(BigInteger offset, RetrieveJournal r, EntryHeader eheader) throws RpcException, InterruptedException, IOException, SQLNonTransientConnectionException;
    }

    public static interface BlockingNoDataConsumer {
        void accept() throws InterruptedException;
    }

    public static class RpcException extends Exception {
        public RpcException(String message, Throwable cause) {
            super(message, cause);
        }

        public RpcException(String message) {
            super(message);
        }
    }


    private static class LogLimmiting {
        private final Map<String, Long> lastLogged = new HashMap<>();
        private final long rate;
        
        public LogLimmiting(long rate) {
            this.rate = rate;
        }
        
        public boolean shouldLogRateLimted(String type) {
            if (lastLogged.containsKey(type)) {
                if (System.currentTimeMillis() > rate + lastLogged.get(type)) {
                    lastLogged.put(type, System.currentTimeMillis());
                    return true;
                }
            }
            else {
                lastLogged.put(type, System.currentTimeMillis());
                return true;
            }
            return false;
        }
    }
}
