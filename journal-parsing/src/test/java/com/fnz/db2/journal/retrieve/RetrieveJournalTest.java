package com.fnz.db2.journal.retrieve;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.ibm.as400.access.AS400;

public class RetrieveJournalTest {
	JournalInfo ji = new JournalInfo("journalName", "journalLibrary");
	Connect<AS400, IOException> as400;
	JournalCode[] codes = new JournalCode[] {};
	List<FileFilter> fileFilter = List.of(new FileFilter("schema", "TABE1"), new FileFilter("schema", "TABLE2"));
	RetrieveConfig config = new RetrieveConfig(as400, ji, 1024*32, true, codes, fileFilter , 10000, null);
	JournalInfoRetrieval journalRetrieval = new JournalInfoRetrieval();
	RetrieveJournal retrieve = new RetrieveJournal(config, journalRetrieval);
	@BeforeEach
	protected void setUp() throws Exception {
	}

	@Test
	void testRetrieveJournal() throws Exception {
		retrieve.retrieveJournal(new JournalProcessedPosition(BigInteger.valueOf(0), "receiver", "lib", Instant.ofEpochSecond(0), true));
	}

}
