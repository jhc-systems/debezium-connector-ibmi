package com.fnz.db2.journal.retrieve;

import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;

public interface JournalEntryDeocder<T> {
	static int ENTRY_SPECIFIC_DATA_OFFSET = 16; // 5 length + 11 reserved

	T decode(EntryHeader entryHeader, byte[] data, int offset) throws Exception;
}
