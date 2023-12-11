/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class As400ChangeEventSourceFactory implements ChangeEventSourceFactory<As400Partition, As400OffsetContext> {

    private final As400ConnectorConfig configuration;
    private final As400ConnectorConfig snapshotConfig;
    private final As400RpcConnection rpcConnection;
    private final MainConnectionProvidingConnectionFactory<As400JdbcConnection> jdbcConnectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<As400Partition, TableId> dispatcher;
    private final Clock clock;
    private final As400DatabaseSchema schema;

    public As400ChangeEventSourceFactory(As400ConnectorConfig configuration, As400ConnectorConfig snapshotConfig,
            As400RpcConnection rpcConnection,
            MainConnectionProvidingConnectionFactory<As400JdbcConnection> jdbcConnectionFactory,
            ErrorHandler errorHandler, EventDispatcher<As400Partition, TableId> dispatcher, Clock clock,
            As400DatabaseSchema schema) {
        this.configuration = configuration;
        this.rpcConnection = rpcConnection;
        this.jdbcConnectionFactory = jdbcConnectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.snapshotConfig = snapshotConfig;
    }

    @Override
    public SnapshotChangeEventSource<As400Partition, As400OffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<As400Partition> snapshotProgressListener,
            NotificationService<As400Partition, As400OffsetContext> notificationService) {
        return new As400SnapshotChangeEventSource(snapshotConfig, rpcConnection, jdbcConnectionFactory, schema,
                dispatcher, clock, snapshotProgressListener, notificationService);
    }

    @Override
    public StreamingChangeEventSource<As400Partition, As400OffsetContext> getStreamingChangeEventSource() {
        return new As400StreamingChangeEventSource(configuration, rpcConnection, jdbcConnectionFactory.mainConnection(),
                dispatcher, errorHandler, clock, schema);
    }
}
