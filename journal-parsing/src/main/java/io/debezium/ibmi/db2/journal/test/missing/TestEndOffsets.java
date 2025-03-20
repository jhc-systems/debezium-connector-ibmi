package io.debezium.ibmi.db2.journal.test.missing;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.test.TestConnector;

public class TestEndOffsets {

    private static final Logger log = LoggerFactory.getLogger(TestEndOffsets.class);

    public static void main(String args[]) throws Exception {
        final String threadsParam = System.getenv("MAX_THREADS");
        String tablePrefix = System.getenv("TABLE_PREFIX");
        int threads = 10;
        if (threadsParam != null) {
            threads = Integer.valueOf(threadsParam);
        }
        tablePrefix = (tablePrefix == null || tablePrefix.isBlank()) ? "TESTSKIP" : tablePrefix;

        List<String> al = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            al.add(tablePrefix + i);
        }

        for (String table : al) {
            final TestConnector connector = new TestConnector();
            log.info("starting checker for {}", table);
            UpdateSingleRowChecker checker = new UpdateSingleRowChecker(connector, table);
            checker.startChecker();
        }

        {
            String table = tablePrefix + (threads + 1);
            final TestConnector connector = new TestConnector();
            UpdateAllRows updateRows = new UpdateAllRows(connector.getJdbc().connection(), connector.getSchema(), table, 0, 100);
            updateRows.initaliseTable();

            updateRows.startUpdateThread();
        }
    }

}