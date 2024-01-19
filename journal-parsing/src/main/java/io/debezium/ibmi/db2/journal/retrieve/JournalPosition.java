/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

// sequence number up to 18 446644 000 000 000 000
public record JournalPosition(BigInteger offset, JournalReceiver receiver) {

    public JournalPosition(JournalPosition position) {
        this(position.offset, position.receiver);
    }

    public static JournalPosition endPosition(DetailedJournalReceiver details) {
        return new JournalPosition(details.end(), details.info().receiver());
    }

    public static JournalPosition startPosition(DetailedJournalReceiver details) {
        return new JournalPosition(details.start(), details.info().receiver());
    }

    public BigInteger getOffset() {
        if (null == offset) {
            return BigInteger.ZERO;
        }
        return offset;
    }

    public JournalReceiver getReceiver() {
        return receiver();
    }
}