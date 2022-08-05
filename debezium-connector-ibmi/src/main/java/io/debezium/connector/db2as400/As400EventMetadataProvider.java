/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

public class As400EventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        Long timestamp = value.getInt64(SourceInfo.TIMESTAMP_KEY);

        if (timestamp == null) {
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        }

        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key,
                                                      Struct value) {
        Map<String, ?> map = offset.getOffset();
        return (Map<String, String>) map;
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // TODO Auto-generated method stub
        if (offset.getTransactionContext() == null) {
            return null;
        }
        return offset.getTransactionContext().getTransactionId();
    }

}
