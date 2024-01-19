/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.test;

/** Test stub **/
public class CommitLogProcessor {
    /*
     * private static final Logger log = LoggerFactory.getLogger(CommitLogProcessor.class);
     *
     * static FileWriter orderids;
     * private static JdbcFileDecoder fileDecoder;
     * private static SchemaCacheHash schemaCache = new SchemaCacheHash();
     *
     * public static void main(String[] args) throws Exception {
     * final TestConnector connector = new TestConnector();
     * final Connect<AS400, IOException> as400Connect = connector.getAs400();
     * final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
     * final String schema = connector.getSchema();
     *
     * JournalProcessedPosition nextPosition = new JournalProcessedPosition();
     * final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
     *
     * final List<FileFilter> includes = new ArrayList<>();
     * final String includesEnv = System.getenv("ISERIES_INCLUDES");
     * if (includesEnv != null) {
     * for (final String i : Arrays.asList(includesEnv.split(","))) {
     * includes.add(new FileFilter(schema, i));
     * }
     * }
     *
     * final JournalInfo journal = JournalInfoRetrieval.getJournal(as400Connect.connection(), schema, includes);
     *
     * final String offset = System.getenv("ISERIES_OFFSET");
     * final String receiver = System.getenv("ISERIES_RECEIVER");
     * if (offset != null && receiver != null) {
     * nextPosition = new JournalProcessedPosition(offset, receiver, journal.journalLibrary(), Instant.ofEpochSecond(0), false);
     * }
     *
     * final String database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
     * fileDecoder = new JdbcFileDecoder(sqlConnect, database, schemaCache, -1, -1);
     * JournalProcessedPosition lastPosition = null;
     *
     * final JournalPosition endPosition = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
     * log.info("end position is: {}", endPosition);
     *
     * final long startTime = System.currentTimeMillis();
     *
     * log.info("journal: {}", journal);
     * final RetrieveConfig config = new RetrieveConfigBuilder()
     * .withAs400(as400Connect)
     * .withJournalInfo(journal)
     * .withDumpFolder("./bad-journal")
     * .withServerFiltering(true)
     * .withIncludeFiles(includes)
     * .withMaxServerSideEntries(10000)
     * .build();
     * final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);
     *
     * do {
     * lastPosition = new JournalProcessedPosition(nextPosition);
     * nextPosition = retrieveJorunal(as400Connect, journal, rj, nextPosition);
     * log.info("after : {} previous {}", nextPosition, lastPosition);
     * if (nextPosition.equals(lastPosition)) {
     * log.info("caught up");
     * Thread.sleep(1000);
     * }
     * } while (!nextPosition.equals(lastPosition));
     *
     * final long end = System.currentTimeMillis();
     *
     * log.info("time taken {}", (end - startTime) / 1000.0);
     *
     * }
     *
     * private static JournalProcessedPosition retrieveJorunal(Connect<AS400, IOException> connector, JournalInfo journal, RetrieveJournal r,
     * JournalProcessedPosition position)
     * throws Exception {
     *
     * final boolean success = r.retrieveJournal(position);
     * log.info("success: {} position: {} header {}", success, position, r.getFirstHeader());
     *
     * if (success) {
     * log.info("more journal data: {}", r.futureDataAvailable());
     * while (r.nextEntry()) {
     * final EntryHeader eheader = r.getEntryHeader();
     *
     * position.setPosition(r.getPosition());
     * final JournalEntryType entryType = eheader.getJournalEntryType();
     *
     * if (entryType == null) {
     * continue;
     * }
     *
     * final String file = eheader.getFile();
     * final String lib = eheader.getLibrary();
     * final String member = eheader.getMember();
     *
     * // log.debug("time {} receiver {} offset {} lib: {} file: {} member: {}", eheader.getTime(), eheader.getReceiver(), eheader.getEndOffset(), lib, file, member);
     *
     * switch (entryType) {
     * case DELETE_ROW1, DELETE_ROW2:
     * // log.debug("deleted lib: {} file: {} member: {}", lib, file, member);
     * break;
     * case ADD_ROW2, ADD_ROW1:
     * // log.debug("add row lib: {} file: {} member: {}", lib, file, member);
     * // dumpTable(eheader, r, file, lib, member);
     * break;
     * case BEFORE_IMAGE, ROLLBACK_BEFORE_IMAGE:
     * // log.debug("update row old values lib: {} file: {} member: {}", lib, file, member);
     * // dumpTable(eheader, r, file, lib, member);
     * break;
     * case AFTER_IMAGE, ROLLBACK_AFTER_IMAGE:
     * // log.debug("update row new values lib: {} file: {} member: {}", lib, file, member);
     * dumpTable(eheader, r, file, lib, member);
     * break;
     * default:
     * break;
     * }
     *
     * }
     *
     * final EntryHeader eh = r.getEntryHeader();
     *
     * // if (eh != null) {
     * // log.info("last offset was {}.{}.{}", eh.getSequenceNumber(), eh.getReceiver(), eh.getReceiverLibrary());
     * // }
     * //
     * // r.getFirstHeader().nextPosition().map(jp -> {
     * // log.info("next offset is {}", jp.toString());
     * // return null;
     * // });
     * log.info("next offset == {}", r.getPosition());
     *
     * position.setPosition(r.getPosition());
     *
     * }
     * else {
     * log.info("finished?");
     * final JournalReceiver journalNow = JournalInfoRetrieval.getReceiver(connector.connection(), journal);
     * final JournalProcessedPosition lastOffset = position;
     * if (lastOffset.getReceiver() != null
     * && !journalNow.equals(lastOffset.getReceiver())) {
     * log.warn("journal receiver doesn't match at position {} we have journal {} and latest is {} ",
     * position, lastOffset.getReceiver(), journalNow);
     * }
     * log.error(
     * "Lost journal at position {}. Restarting with blank journal and offset ( current journal is {} )",
     * position, journalNow);
     * position.setPosition(new JournalProcessedPosition());
     * System.exit(-1);
     * }
     *
     * r = null;
     * log.info("position {}", position);
     * return position;
     * }
     *
     * private static void dumpTable(EntryHeader eheader, RetrieveJournal rnje, String file, String lib, String member) {
     * log.info("lib: {} file: {} memper: {}", lib, file, member);
     * // DL entries are empty don't try and decode them
     * if (!"DL".equals(eheader.getEntryType()) && !"DR".equals(eheader.getEntryType())) {
     * // String recordFileName = "/QSYS.LIB/" + lib + ".LIB/" + file + ".FILE/" + member + ".MBR";
     * final Optional<TableInfo> tableInfoOpt = fileDecoder.getRecordFormat(eheader.getFile(), eheader.getLibrary());
     * tableInfoOpt.ifPresent(tableInfo -> {
     * try {
     * log.info("tableInfo: {}", tableInfo);
     *
     * final Object[] fields = rnje.decode(fileDecoder);
     * if (fields != null) {
     * log.info("number of fields {}", fields.length);
     * int i = 0;
     * for (final Object f : fields) {
     * String value = "No value found";
     * if (f == null) {
     * value = "null";
     * }
     * else if (f instanceof final byte[] data) {
     * final StringBuilder sb = new StringBuilder(data.length * 2);
     * for (final byte b : data) {
     * sb.append(String.format("%02x", b));
     * }
     * value = sb.toString();
     * }
     * else {
     * value = f.toString();
     * }
     * log.info("\t{} = {} type {}", tableInfo.getStructure().get(i).getName(), value, tableInfo.getStructure().get(i).getType());
     * i++;
     * }
     * }
     * }
     * catch (final Exception e) {
     * log.error("Failed to dump table", e);
     * }
     * });
     * }
     * }
     *
     * static long count = 0;
     */
}
