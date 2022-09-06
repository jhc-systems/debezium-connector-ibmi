package com.fnz.db2.journal.retrieve;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.fnz.db2.journal.retrieve.RetrieveJournal.ParameterListBuilder;
import com.ibm.as400.access.AS400;
import java.util.Collections;

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
