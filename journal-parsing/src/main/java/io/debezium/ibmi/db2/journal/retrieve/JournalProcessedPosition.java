/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Objects;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

/** class for just the position */
public class JournalProcessedPosition {
    // position should be last processed record as requesting the next record will error and be indistinguishable from losing the journal
    private BigInteger offset; // sequence number up to 18 446 644 000 000 000 000
    private JournalReceiver receiver;
    private Instant timeOfLastProcessed = Instant.EPOCH;
    private boolean processed = false;
    private static String[] empty = new String[]{};

    public JournalProcessedPosition(JournalProcessedPosition position) {
        this.offset = position.offset;
        this.receiver = position.receiver;
        this.processed = position.processed;
        this.timeOfLastProcessed = position.timeOfLastProcessed;
    }

    public JournalProcessedPosition() {
    }

    public JournalProcessedPosition(JournalPosition p, Instant time, boolean processed) {
        this(p.getOffset(), p.getReceiver(), time, processed);
    }

    public JournalPosition asJournalPosition() {
        return new JournalPosition(this.offset, this.receiver);
    }

    public boolean processed() {
        return processed;
    }

    public JournalProcessedPosition(String offsetStr, String receiver, String receiverLibrary, Instant time, boolean processed) {
        if (offsetStr == null || offsetStr.isBlank()) {
            this.offset = null;
        }
        else {
            this.offset = new BigInteger(offsetStr);
        }
        this.receiver = new JournalReceiver(StringHelpers.safeTrim(receiver), StringHelpers.safeTrim(receiverLibrary));
        this.timeOfLastProcessed = time;
        this.processed = processed;
    }

    public JournalProcessedPosition(BigInteger offset, JournalReceiver receiver, Instant time, boolean processed) {
        this.offset = offset;
        this.receiver = receiver;
        this.timeOfLastProcessed = time;
        this.processed = processed;
    }

    public BigInteger getOffset() {
        if (null == offset) {
            return BigInteger.ZERO;
        }
        return offset;
    }

    public Instant getTimeOfLastProcessed() {
        return this.timeOfLastProcessed;
    }

    public boolean isOffsetSet() {
        return (null != offset);
    }

    public JournalReceiver getReceiver() {
        return receiver;
    }

    public String[] getJournal() {
        if (receiver.name() != null && receiver.library() != null) {
            return new String[]{ receiver.name(), receiver.library(), receiver.name(), receiver.library() };
        }
        else {
            return empty;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, processed, receiver);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JournalProcessedPosition other = (JournalProcessedPosition) obj;
        return Objects.equals(offset, other.offset) && processed == other.processed
                && Objects.equals(receiver, other.receiver);
    }

    @Override
    public String toString() {
        return String.format("JournalProcessedPosition [offset=%s, receiver=%s, timeOfLastProcessed=%s, processed=%s]", offset,
                receiver, timeOfLastProcessed, processed);
    }

    // TODO remove all setters and convert to record
    public JournalProcessedPosition setOffset(BigInteger offset, Instant time, boolean processed) {
        this.offset = offset;
        this.processed = processed;
        this.timeOfLastProcessed = time;
        return this;
    }

    public JournalProcessedPosition setProcessed(boolean processed) {
        this.processed = processed;
        return this;
    }

    public void setJournalReceiver(BigInteger offset, String journalReceiver, String schema, Instant time, boolean processed) {
        this.offset = offset;
        this.receiver = new JournalReceiver(StringHelpers.safeTrim(journalReceiver), StringHelpers.safeTrim(schema));
        this.processed = processed;
        this.timeOfLastProcessed = time;
    }

    public void setPosition(JournalProcessedPosition newPosition) {
        this.offset = newPosition.offset;
        this.receiver = newPosition.receiver;
        if (!newPosition.timeOfLastProcessed.equals(Instant.EPOCH)) {
            this.timeOfLastProcessed = newPosition.timeOfLastProcessed;
        }
        this.processed = newPosition.processed;
    }

    public void setPosition(JournalPosition newPosition, boolean processed) {
        this.offset = newPosition.getOffset();
        this.receiver = newPosition.receiver();
        this.processed = processed;
    }

    public JournalProcessedPosition withOffset(BigInteger offset, boolean processed) {
        return new JournalProcessedPosition(offset, this.receiver, this.timeOfLastProcessed, processed);
    }

    public boolean isSameReceiver(DetailedJournalReceiver other) {
        if (receiver == null || other.info() == null) {
            return false;
        }
        return receiver.equals(other.info().receiver());
    }
}