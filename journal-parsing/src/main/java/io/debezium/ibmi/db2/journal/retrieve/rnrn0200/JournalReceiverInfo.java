/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import java.util.Date;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

public record JournalReceiverInfo(JournalReceiver receiver, Date attachTime, JournalStatus status,
        Optional<Integer> chain) {
    public JournalReceiverInfo withStatus(JournalStatus newStatus) {
        return new JournalReceiverInfo(receiver, attachTime, newStatus, chain);
    }
}