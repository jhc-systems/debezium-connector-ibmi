/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.metrics;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class As400StreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<As400Partition> implements As400ChangeEventSourceMetricsMXBean {
    private final AtomicLong journalBehind = new AtomicLong();
    private final AtomicLong journalOffset = new AtomicLong();

    public <T extends CdcSourceTaskContext> As400StreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
            EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public long getJournalBehind() {
        return journalBehind.get();
    }

    public void setJournalBehind(long behind) {
        this.journalBehind.lazySet(behind);
    }
    
    @Override
    public long getJournalOffset() {
        return journalOffset.get();
    }

    public void setJournalOffset(long offset) {
        this.journalOffset.lazySet(offset);
    }
}
