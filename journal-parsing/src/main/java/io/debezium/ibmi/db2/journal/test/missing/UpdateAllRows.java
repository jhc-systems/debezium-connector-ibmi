package io.debezium.ibmi.db2.journal.test.missing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateAllRows {
    private static final Logger log = LoggerFactory.getLogger(UpdateAllRows.class);
    String schema;
    String table;
    int start;
    int end;
    Connection con;
    Random random = new Random();

    public UpdateAllRows(Connection con, String schema, String table, int start, int end) {
        this.schema = schema;
        this.table = table;
        this.start = start;
        this.end = end;
        this.con = con;
    }

    public void initaliseTable() throws Exception {
        initDb(con);
    }

    public void startUpdateThread() throws Exception {
        Thread update = new Thread(() -> updateData(con));
        update.start();
    }

    private void initDb(Connection con) throws Exception {
        try (final Statement st = con.createStatement()) {
            st.executeUpdate(String.format(
                    "drop TABLE %s.%s",
                    schema, table));
            con.commit();
        }
        catch (Exception e) {
            // ignore if doesn't exist
        }

        try (final Statement st = con.createStatement()) {
            st.executeUpdate(String.format(
                    "CREATE OR replace TABLE %s.%s (id int, v1 integer, primary key (id))",
                    schema, table));
            con.commit();
        }

        String sql = String.format("insert into %s.%s (id, v1) values (?, ?)", schema, table);
        for (int i = start; i < end; i++) {
            try (final PreparedStatement st = con.prepareStatement(sql)) {
                st.setInt(1, i);
                st.setInt(2, -1);
                st.executeUpdate();
                con.commit();
            }
            catch (SQLException e) {
                log.error("sql failed {} {}", sql, i, e);
            }
        }

    }

    private void updateData(Connection con) {
        String updateQuery = String.format("update %s.%s set v1=?", schema, table);
        log.info("starting updates");
        int counter = 0;

        try {
            try (final PreparedStatement ps = con.prepareStatement(updateQuery)) {
                while (true) {
                    ps.setInt(1, counter++);
                    final int ir = ps.executeUpdate();
                    con.commit();
                    Thread.sleep(random.nextInt(100));
                }
            }
        }
        catch (Exception e) {
            log.error("update failed on table {}", table, e);
            System.exit(1);
        }
    }
}
