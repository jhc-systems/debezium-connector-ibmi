package com.fnz.db2.journal.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.data.types.Diagnostics;
import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
import com.fnz.db2.journal.retrieve.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.JournalReceiver;
import com.fnz.db2.journal.retrieve.JournalRecordDecoder;
import com.fnz.db2.journal.retrieve.NullIndicatorDecoder;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.SchemaCacheHash;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.retrieve.rjne0200.FirstHeader;
import com.fnz.db2.journal.retrieve.rjne0200.OffsetStatus;
import com.ibm.as400.access.AS400;

public class DebugJournal {
    private static final Logger log = LoggerFactory.getLogger(DebugJournal.class);


	public static void main(String[] args) throws Exception {
        TestConnector connector = new TestConnector();
        Connect<AS400, IOException> as400Connect = connector.getAs400();
        Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
        String schema = connector.getSchema();
        
        String database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
        JdbcFileDecoder fileDecoder = new JdbcFileDecoder(sqlConnect, database, new SchemaCacheHash());

        byte[] data = Files.readAllBytes(Paths.get("C:\\dev\\kafka\\journal-parsing\\good-journal\\201218-0616-0"));
        JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema);
        RetrieveJournal rnj = new RetrieveJournal(as400Connect, journal,  null);
        rnj.setOutputData(data, new FirstHeader(data.length, 0, data.length, OffsetStatus.NO_MORE_DATA, Optional.empty()), new JournalPosition());
        if (rnj.nextEntry() ) {
        	EntryHeader entry = rnj.getEntryHeader();
        	String code = String.format("%s %s", entry.getJournalCode(), entry.getEntryType());
            log.info("code: {}", code);
        	switch (code) {
        		case "J NR":
        			JournalReceiver receiver = rnj.decode(new JournalRecordDecoder());
        			log.info("receiver {}", receiver);
        			break;
        		
        	}
        	log.info("header {}", rnj.getEntryHeader().toString());
        	rnj.dumpEntry();
    		log.info("dump from entry start");
    		log.info(Diagnostics.binAsHex(data, rnj.getOffset()  + rnj.getEntryHeader().getEntrySpecificDataOffset(), rnj.getEntryHeader().getLength()));
    		log.info(Diagnostics.binAsEbcdic(data, rnj.getOffset() + rnj.getEntryHeader().getEntrySpecificDataOffset() , rnj.getEntryHeader().getLength()));

    		rnj.dumpEntry();
        }
	}

}
