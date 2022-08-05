/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.connector.common.CdcSourceTaskContext;

public class As400TaskContext extends CdcSourceTaskContext {
    private final As400ConnectorConfig config;

    public As400TaskContext(As400ConnectorConfig config, As400DatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
        this.config = config;
    }

    public As400ConnectorConfig getConfig() {
        return config;
    }
}
