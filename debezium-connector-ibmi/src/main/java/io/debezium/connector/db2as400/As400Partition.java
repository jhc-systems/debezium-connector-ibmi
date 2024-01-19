/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class As400Partition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public As400Partition(String serverName) {
        this.serverName = serverName;
    }

    public As400Partition(As400Partition orig) {
        this(orig.serverName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final As400Partition other = (As400Partition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    static class Provider implements Partition.Provider<As400Partition> {
        private final As400ConnectorConfig connectorConfig;

        Provider(As400ConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<As400Partition> getPartitions() {
            return Collections.singleton(new As400Partition(connectorConfig.getLogicalName()));
        }
    }
}
