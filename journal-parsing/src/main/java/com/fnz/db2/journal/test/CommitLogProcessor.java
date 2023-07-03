package com.fnz.db2.journal.test;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.FileFilter;
import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
import com.fnz.db2.journal.retrieve.JournalEntryType;
import com.fnz.db2.journal.retrieve.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.JournalProcessedPosition;
import com.fnz.db2.journal.retrieve.JournalReceiver;
import com.fnz.db2.journal.retrieve.RetrieveConfig;
import com.fnz.db2.journal.retrieve.RetrieveConfigBuilder;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.SchemaCacheHash;
import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.ibm.as400.access.AS400;

/** Test stub **/
public class CommitLogProcessor {
	private static final Logger log = LoggerFactory.getLogger(CommitLogProcessor.class);
	
	static FileWriter orderids;
	private static JdbcFileDecoder fileDecoder;
	private static SchemaCacheHash schemaCache = new SchemaCacheHash();


	public static void main(String[] args) throws Exception {
	    TestConnector connector = new TestConnector();
	    Connect<AS400, IOException> as400Connect = connector.getAs400();
	    Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
	    String schema = connector.getSchema();
	    
		JournalProcessedPosition nextPosition = new JournalProcessedPosition();
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();

		List<FileFilter> includes = new ArrayList<FileFilter>();
        String includesEnv = System.getenv("ISERIES_INCLUDES");
        if (includesEnv != null) {
			for (String i : Arrays.asList(includesEnv.split(","))) {
				includes.add(new FileFilter(schema, i));
			}
		}
        
        JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);

        String offset =  System.getenv("ISERIES_OFFSET");
        String receiver =  System.getenv("ISERIES_RECEIVER");
        if (offset != null && receiver != null)
            nextPosition = new JournalProcessedPosition(offset, receiver, journal.journalLibrary(), Instant.ofEpochSecond(0), false);
        
		String database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
		fileDecoder = new JdbcFileDecoder(sqlConnect, database, schemaCache, -1);
		JournalProcessedPosition lastPosition = null;
		
        JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
		log.info("end position is: {}", endPosition);
		
		long startTime = System.currentTimeMillis();
		
		
		log.info("journal: {}", journal);
		RetrieveConfig config = new RetrieveConfigBuilder()
				.withAs400(as400Connect)
				.withJournalInfo(journal)
				.withDumpFolder("./bad-journal")
				.withServerFiltering(true)
				.withIncludeFiles(includes)
				.withMaxServerSideEntries(10000)
				.build();
		RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);

		do {
			lastPosition = new JournalProcessedPosition(nextPosition);
			nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
			log.info("after : {} previous {}", nextPosition, lastPosition);
			if (nextPosition.equals(lastPosition)) {
				log.info("caught up");
				Thread.sleep(1000);
			}
		} while (!nextPosition.equals(lastPosition));
			
		long end = System.currentTimeMillis();
		
		log.info("time taken {}", (end-startTime)/1000.0);
		
	}

	private static JournalProcessedPosition retrieveJorunal(Connect<AS400, IOException> connector, JournalInfo journal, RetrieveJournal r, JournalProcessedPosition position)
			throws Exception {

		boolean success = r.retrieveJournal(position);
		log.info("success: {} position: {}", success, position);

		if (success) {
			log.info("more journal data: {}", r.futureDataAvailable());
			while (r.nextEntry()) {
				EntryHeader eheader = r.getEntryHeader();

				position.setPosition(r.getPosition());
				JournalEntryType entryType = eheader.getJournalEntryType();
				
				if (entryType == null)
				    continue;
				
                String file = eheader.getFile();
                String lib = eheader.getLibrary();
                String member = eheader.getMember();

//                log.debug("time {} receiver {} offset {} lib: {} file: {} member: {}", eheader.getTime(), eheader.getReceiver(), eheader.getEndOffset(), lib, file, member);
                
				switch (entryType) {
					case DELETE_ROW1, DELETE_ROW2:
//    			        log.debug("deleted lib: {} file: {} member: {}", lib, file, member);
						break;
					case ADD_ROW2,ADD_ROW1:
//                        log.debug("add row lib: {} file: {} member: {}", lib, file, member);
//                        dumpTable(eheader, r, file, lib, member);
                        break;
                    case BEFORE_IMAGE:
//                        log.debug("update row old values lib: {} file: {} member: {}", lib, file, member);
//                        dumpTable(eheader, r, file, lib, member);
                        break;
					case AFTER_IMAGE:
//					    log.debug("update row new values lib: {} file: {} member: {}", lib, file, member);
//				        dumpTable(eheader, r, file, lib, member);
						break;
					default:
						break;
				}
				
			}

            EntryHeader eh = r.getEntryHeader();
			
//            if (eh != null) {
//                log.info("last offset was {}.{}.{}", eh.getSequenceNumber(), eh.getReceiver(), eh.getReceiverLibrary());
//            }
//            
//            r.getFirstHeader().nextPosition().map(jp -> {
//				log.info("next offset is {}", jp.toString());
//				return null;
//			});
			log.info("next offset == {}", r.getPosition());
			
			
			position.setPosition(r.getPosition());

		} else {
			log.info("finished?");
			JournalReceiver journalNow = JournalInfoRetrieval.getReceiver(connector.connection(), journal);
            JournalProcessedPosition lastOffset = position;
            if (lastOffset.getReceiver() != null
                    && !journalNow.equals(lastOffset.getReceiver())) {
                log.warn("journal receiver doesn't match at position {} we have journal {} and latest is {} ",
                        position, lastOffset.getReceiver(), journalNow);
            }
            log.error(
                    "Lost journal at position {}. Restarting with blank journal and offset ( current journal is {} )",
                    position, journalNow);
            position.setPosition(new JournalProcessedPosition());
            System.exit(-1);
		}
			
		r = null;
		log.info("position {}", position);
		return position;
	}
	



	private static void dumpTable(EntryHeader eheader, RetrieveJournal rnje, String file, String lib, String member) {
		log.info("lib: {} file: {} memper: {}", lib, file, member);
        // DL entries are empty don't try and decode them
		if (!"DL".equals(eheader.getEntryType()) && !"DR".equals(eheader.getEntryType())) {
//				String recordFileName = "/QSYS.LIB/" + lib + ".LIB/" + file + ".FILE/" + member + ".MBR";
				Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(), eheader.getLibrary());
				tableInfoOpt.ifPresent(tableInfo -> {
					try {
					log.info("tableInfo: {}", tableInfo);
	
					Object[] fields = rnje.decode(fileDecoder);
					if (fields != null) {
						log.info("number of fields {}", fields.length);
						int i=0;
						for(Object f: fields) {
							String value = "No value found";
							if (f == null) {
								value = "null";
							} else if (f instanceof byte[] data) {
								StringBuilder sb = new StringBuilder(data.length * 2);
								for(byte b: data)
								      sb.append(String.format("%02x", b));
								value = sb.toString();
							} else {
								value = f.toString();
							}
							log.info("\t{} = {} type {}", tableInfo.getStructure().get(i).getName(), value, tableInfo.getStructure().get(i).getType());
							i++;
						}
					}
					} catch (Exception e) {
						log.error("Failed to dump table", e);
					}
				});
		}
	}

	static long count = 0;


}