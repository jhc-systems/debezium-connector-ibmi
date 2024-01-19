/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

public interface JournalEntryDeocder<T> {
    int ENTRY_SPECIFIC_DATA_OFFSET = 16; // 5 length + 11 reserved

    T decode(EntryHeader entryHeader, byte[] data, int offset) throws Exception;
}
