/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;

import com.fnz.db2.journal.retrieve.JournalPosition;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

//TODO  can we deliver HistorizedRelationalDatabaseConnectorConfig or should it be RelationalDatabaseConnectorConfig 
public class As400ConnectorConfig extends RelationalDatabaseConnectorConfig {
    private static TableIdToStringMapper tableToString = x -> {
        StringBuilder sb = new StringBuilder(x.schema());
        sb.append(".").append(x.table());
        return sb.toString();
    };

    private final SnapshotMode snapshotMode;
    private final Configuration config;
    private String incrementalTables = "";

    // used by the snapshot to limit the additional tables for a change in configuration
    private RelationalTableFilters tableFilters;

    /**
     * A field for the user to connect to the AS400. This field has no default
     * value.
     */
    public static final Field USER = Field.create("user", "Name of the user to be used when connecting to the as400");
    /**
     * A field for the password to connect to the AS400. This field has no default
     * value.
     */
    public static final Field PASSWORD = Field.create("password", "Password to be used when connecting to the as400");

    /**
     * A field for the password to connect to the AS400. This field has no default
     * value.
     */
    public static final Field SCHEMA = Field.create("schema", "schema holding tables to capture");

    /**
     * A field for the size of buffer for fetching journal entries default 65535 (should not be smaller)
     */
    public static final Field BUFFER_SIZE = Field.create("buffer_size", "journal buffer size",
            "size of buffer for fetching journal entries default 131072 (should not be smaller)", "131072");

    /**
     * keep alive flag, should the driver send keep alive packets default true
     */
    public static final Field KEEP_ALIVE = Field.create("keep alive", "keep alive",
            "keep alive", true);

    /**
     * threads should be used in communication with the host servers - timeouts might not work as expected when true - default false
     */
    public static final Field THREAD_USED = Field.create("thread used", "thread used - timeouts might not work as expected when true",
            "thread used", false);

    /**
     * The timeout to use for sockets
     */
    public static final Field SOCKET_TIMEOUT = Field.create("socket timeout", "socket timeout in milliseconds", "socket timeout", 0);



    public As400ConnectorConfig(Configuration config) {
        super(config, config.getString(JdbcConfiguration.HOSTNAME), new SystemTablesPredicate(),
                tableToString, 1, ColumnFilterMode.SCHEMA);
        this.config = config;
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.tableFilters = new As400NormalRelationalTableFilters(config, new SystemTablesPredicate(), tableToString);
    }

    // used by the snapshot to limit the additional tables for a change in configuration
    public As400ConnectorConfig(Configuration config, String incrementalTableFilters) {
        this(config);
        this.incrementalTables = incrementalTableFilters;
        this.tableFilters = new As400AdditionalRelationalTableFilters(
                config, new SystemTablesPredicate(), tableToString, incrementalTableFilters);
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public String getHostName() {
        return config.getString(JdbcConfiguration.HOSTNAME);
    }

    public String getUser() {
        return config.getString(USER);
    }

    public String getPassword() {
        return config.getString(PASSWORD);
    }

    public String getSchema() {
        return config.getString(SCHEMA).toUpperCase();
    }

    public Integer getJournalBufferSize() {
        return config.getInteger(BUFFER_SIZE);
    }

    public Integer getSocketTimeout() {
        Integer i = config.getInteger(SOCKET_TIMEOUT);
        return i;
    }

    public Integer getKeepAlive() {
        return config.getInteger(KEEP_ALIVE);
    }

    public JournalPosition getOffset() {
        String receiver = config.getString(As400OffsetContext.RECEIVER);
        String lib = config.getString(As400OffsetContext.RECEIVER_LIBRARY);
        String offset = config.getString(As400OffsetContext.EVENT_SEQUENCE);
        Boolean processed = config.getBoolean(As400OffsetContext.PROCESSED);
    	return new JournalPosition(offset, receiver, lib, (processed == null) ? false : processed);
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !(t.schema().toLowerCase().equals("QSYS2") || t.schema().toLowerCase().equals("QSYS")); // TODO
        }
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new As400SourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    public static Field.Set ALL_FIELDS = Field.setOf(JdbcConfiguration.HOSTNAME, USER, PASSWORD, SCHEMA, BUFFER_SIZE,
            RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, KEEP_ALIVE, THREAD_USED, SOCKET_TIMEOUT);

    public static ConfigDef configDef() {
        ConfigDef c = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
                .name("ibmi")
                .type(
                        JdbcConfiguration.HOSTNAME, USER, PASSWORD, SCHEMA, BUFFER_SIZE,
                        KEEP_ALIVE, THREAD_USED, SOCKET_TIMEOUT)
                .connector()
                .events(
                        As400OffsetContext.EVENT_SEQUENCE_FIELD,
                        As400OffsetContext.RECEIVER_FIELD,
                        As400OffsetContext.RECEIVER_LIBRARY_FIELD,
                        As400OffsetContext.PROCESSED_FIELD)
                .create().configDef();
        return c;
    }

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode").withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL).withWidth(Width.SHORT).withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. " + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed", true),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin
         * reading the binlog. This should be used with care, but it is very useful when
         * the change event consumers need only the changes from the point in time the
         * snapshot is made (and doesn't care about any state or changes prior to this
         * point).
         */
        SCHEMA_ONLY("schema_only", false),

        /**
         * Never perform a snapshot and only read the binlog. This assumes the binlog
         * contains all the history of those databases and tables that will be captured.
         */
        NEVER("never", false);

        private final String value;
        private final boolean includeData;

        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null
         *         default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    @Override
    // used by the snapshot to limit the additional tables for a change in configuration
    public RelationalTableFilters getTableFilters() {
        if (tableFilters == null) {
            return super.getTableFilters();
        }
        return tableFilters;
    }

    public JdbcConfiguration getJdbcConfiguration() {
        return JdbcConfiguration.adapt(config);
    }
}
