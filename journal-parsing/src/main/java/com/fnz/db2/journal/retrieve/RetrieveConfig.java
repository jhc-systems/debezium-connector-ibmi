package com.fnz.db2.journal.retrieve;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.ibm.as400.access.AS400;

public record RetrieveConfig(Connect<AS400, IOException> as400, 
		JournalInfo journalInfo,
		int journalBufferSize,
		boolean filtering,
		JournalCode[] filterCodes,
		List<FileFilter> includeFiles,
		int maxServerSideEntries,
		File dumpFolder) {

	public static final int DEFAULT_MAX_SERVER_SIDE_ENTRIES = 1000000;
}
