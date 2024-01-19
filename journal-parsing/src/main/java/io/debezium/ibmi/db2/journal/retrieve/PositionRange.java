/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

public record PositionRange(boolean fromBeginning, JournalProcessedPosition start, JournalPosition end) {

    boolean startEqualsEnd() {
        final JournalPosition startPos = new JournalPosition(start.getOffset(), start.getReceiver());
        return startPos.equals(end) && start.processed();
    }
}
