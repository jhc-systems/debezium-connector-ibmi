package io.debezium.ibmi.db2.journal.test.missing;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.test.TestConnector;

public class TestEndOffsets {

    private static final Logger log = LoggerFactory.getLogger(TestEndOffsets.class);

    public static void main(String args[]) throws Exception {
        final TestConnector connector = new TestConnector();

        List<String> al = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            al.add("TESTSKIP" + i);
        }
        for (String table : al) {
            UpdateChecker checker = new UpdateChecker(connector, table);
            checker.startChecker();
        }
    }

}