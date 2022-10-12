package com.fnz.db2.journal.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.FileFilter;
import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
import com.fnz.db2.journal.retrieve.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.RetrieveConfig;
import com.fnz.db2.journal.retrieve.RetrieveConfigBuilder;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.SchemaCacheHash;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.ibm.as400.access.AS400;

public class JournalFilterTimeout {
	private static final Logger log = LoggerFactory.getLogger(CommitLogProcessor.class);
	
	private static SchemaCacheHash schemaCache = new SchemaCacheHash();

	public static void main(String[] args) throws Exception {
	    TestConnector connector = new TestConnector();
	    Connect<AS400, IOException> as400Connect = connector.getAs400();
	    Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
	    String schema = connector.getSchema();
	    
        JournalInfo journalLib = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema);

        String offset =  System.getenv("ISERIES_OFFSET");
        String receiver =  System.getenv("ISERIES_RECEIVER");
        List<FileFilter> includes = new ArrayList<FileFilter>();
        String includesEnv = System.getenv("ISERIES_INCLUDES");
        if (includesEnv != null) {
			for (String i : Arrays.asList(includesEnv.split(","))) {
				includes.add(new FileFilter(schema, i));
			}
		}

		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
		List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(as400Connect.connection(), journalLib);
		DetailedJournalReceiver first = receivers.stream().min((x, y) -> x.start().compareTo(y.start())).get();
        JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journalLib);
		log.info("start {} end {}", first, endPosition);
		
		
		
		try (PrintWriter pw = new PrintWriter(new File("exceptions.txt"))) {
			JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema);
			log.info("journal: " + journal);
			RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal).withDumpFolder("./bad-journal").withServerFiltering(true).withIncludeFiles(includes).build();
			RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);

			JournalPosition p = new JournalPosition(first.start(), first.info().name(), first.info().library(), false); 
			long start = System.currentTimeMillis();
			boolean success = rj.retrieveJournal(p);
			long end = System.currentTimeMillis();
//			while (r.nextEntry()) {
//				;
//			}
			log.info("success: {} position: {} time: {} ", success, rj.getPosition(), (end-start)/1000.0);

		}
	}

}
