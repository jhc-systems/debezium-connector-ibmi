package com.fnz.db2.journal.test;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.ibm.as400.access.AS400;

public class JournalInfoMain {
    private static final Logger log = LoggerFactory.getLogger(JournalInfoMain.class);

	public static void main(String[] args) throws Exception {
        TestConnector connector = new TestConnector();
        Connect<AS400, IOException> as400Connect = connector.getAs400();
        String schema = connector.getSchema();

        JournalInfo ji = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, "PERSON");
        
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();

        List<DetailedJournalReceiver> jri = journalInfoRetrieval.getReceivers(as400Connect.connection(), ji);
        log.info("all {}", jri);

        log.info(DetailedJournalReceiver.lastJoined(jri).toString());
        
//        for (DetailedJournalReceiver j : jri) {
//            log.info("receiver {}", j);
//        }
        JournalReceiver jr = JournalInfoRetrieval.getReceiver(as400Connect.connection(), ji);
        log.info("Journal info {}", jr);
        
        log.info("current position: {}", journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), ji));
        log.info("size {}", jri.size());
	}
}
