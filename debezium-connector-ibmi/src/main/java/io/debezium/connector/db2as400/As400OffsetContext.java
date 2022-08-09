/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.JournalPosition;

import io.debezium.config.Field;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

public class As400OffsetContext implements OffsetContext {
    Logger log = LoggerFactory.getLogger(As400OffsetContext.class);
    // TODO note believe there is a per journal offset
    private static final String SERVER_PARTITION_KEY = "server";
    public static final String EVENT_SEQUENCE = "offset.event_sequence";
    public static final String RECEIVER_LIBRARY = "offset.receiver_library";
    public static final String PROCESSED = "offset.processed";
    public static final String RECEIVER = "offset.receiver";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";    

    public static final Field EVENT_SEQUENCE_FIELD = Field.create(EVENT_SEQUENCE);
    public static final Field RECEIVER_LIBRARY_FIELD = Field.create(RECEIVER_LIBRARY);
    public static final Field RECEIVER_FIELD = Field.create(RECEIVER);
    public static final Field PROCESSED_FIELD = Field.create(PROCESSED);

    private final Map<String, String> partition;
    private TransactionContext transactionContext;

    private As400ConnectorConfig connectorConfig;
    private SourceInfo sourceInfo;
    private JournalPosition position;
    private String inclueTables;
    private boolean hasNewTables = false;
    private boolean snapshotComplete = false;

    public As400OffsetContext(As400ConnectorConfig connectorConfig) {
        super();
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = connectorConfig.getOffset();
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        inclueTables = connectorConfig.tableIncludeList();
    }

    public As400OffsetContext(As400ConnectorConfig connectorConfig, JournalPosition position) {
        super();
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = position;
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        inclueTables = connectorConfig.tableIncludeList();
    }

    public As400OffsetContext(As400ConnectorConfig connectorConfig, JournalPosition position, String includeTables, boolean snapshotComplete) {
        super();
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = position;
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        this.inclueTables = includeTables;
        this.snapshotComplete = snapshotComplete;
    }

    public void setPosition(JournalPosition newPosition) {
        this.position.setPosition(newPosition);
    }
    
    public boolean isSnapshotCompplete() {
        return this.snapshotComplete;
    }

    public JournalPosition getPosition() {
        return position;
    }

    public void setTransaction(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public void endTransaction() {
        transactionContext = null;
    }
    
    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            log.debug("new snapshot offset {}", position);
        }
        else {
            log.debug("new offset {}", position);
                 
        }
        BigInteger offset = position.getOffset();
        String offsetStr = "null";
        if (null != offset) {
            offsetStr = offset.toString();
        }
        return Collect.hashMapOf(
                As400OffsetContext.EVENT_SEQUENCE, offsetStr,
                As400OffsetContext.RECEIVER, position.getReciever(),
                As400OffsetContext.PROCESSED, Boolean.toString(position.processed()),
                As400OffsetContext.RECEIVER_LIBRARY, position.getReceiverLibrary(),
                RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name(), inclueTables,
                As400OffsetContext.SNAPSHOT_COMPLETED_KEY, Boolean.toString(snapshotComplete));   
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    public void setSourceTime(Timestamp time) {
        sourceInfo.setSourceTime(time.toInstant());
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot();
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void preSnapshotStart() {
        snapshotComplete = false;
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotComplete = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
//        sourceInfo.tableEvent((TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public String getIncludeTables() {
        return inclueTables;
    }

    public static class Loader implements OffsetContext.Loader<As400OffsetContext> {

        private final As400ConnectorConfig connectorConfig;

        public Loader(As400ConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public As400OffsetContext load(Map<String, ?> map) {
            String offsetStr = (String)map.get(As400OffsetContext.EVENT_SEQUENCE);
            String complete = (String)map.get(As400OffsetContext.SNAPSHOT_COMPLETED_KEY);
            boolean snapshotComplete = false;
            if (null != complete) {
                snapshotComplete = Boolean.valueOf((String)complete);
            }
            String receiver = (String) map.get(As400OffsetContext.RECEIVER);
            boolean processed = Boolean.valueOf((String) map.get(As400OffsetContext.PROCESSED));
            String schema = (String) map.get(As400OffsetContext.RECEIVER_LIBRARY);
            String inclueTables = (String) map.get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
            JournalPosition position = new JournalPosition();
            if (!"null".equals(offsetStr)) {
            	BigInteger offset = new BigInteger(offsetStr);
                position = new JournalPosition(offset, receiver, schema, processed);
            }
            return new As400OffsetContext(connectorConfig, position, inclueTables, snapshotComplete);
        }
    }

    public boolean hasNewTables() {
        return hasNewTables;
    }

    public void hasNewTables(boolean hasNewTables) {
        this.hasNewTables = hasNewTables;
    }

    @Override
    public String toString() {
        return "As400OffsetContext [position=" + position + "]";
    }

}
