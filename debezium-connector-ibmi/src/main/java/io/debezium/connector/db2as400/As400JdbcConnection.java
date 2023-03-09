/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.FileFilter;
import com.ibm.as400.access.AS400JDBCDriverForcedCcsid;
import com.ibm.as400.access.AS400JDBCDriverRegistration;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

public class As400JdbcConnection extends JdbcConnection implements Connect<Connection, SQLException> {
    private static final Logger log = LoggerFactory.getLogger(As400JdbcConnection.class);
    private static final String URL_PATTERN = "jdbc:as400://${" + JdbcConfiguration.HOSTNAME + "}/${"
            + JdbcConfiguration.DATABASE + "}";
    private final JdbcConfiguration config;
    private final int forcedCcsid;
    private boolean registered = false;

    private static final String GET_DATABASE_NAME = "values ( CURRENT_SERVER )";
    private static final String GET_SYSTEM_TABLE_NAME = "select trim(system_table_name) from qsys2.systables where system_table_schema=? AND table_name=?";
    private static final String GET_ALL_SYSTEM_TABLE_NAME = "select trim(system_table_name), trim(table_name) from qsys2.systables where system_table_schema=?";

    private static final String GET_TABLE_NAME = "select trim(table_name) from qsys2.systables where system_table_schema=? AND system_table_name=?";
    private static final String GET_INDEXES = """
    		SELECT c.column_name FROM qsys.QADBKATR k 
            INNER JOIN qsys2.SYSCOLUMNS c on c.table_schema=k.dbklib and c.system_table_name=k.dbkfil AND c.system_column_name=k.DBKFLD 
            WHERE k.dbklib=? AND k.dbkfil=? ORDER BY k.DBKPOS ASC 
           """;
    
    private static final String GET_LONG_COLUMN_NAMES = "select trim(system_column_name), trim(column_name) from qsys2.syscolumns where system_table_schema=? AND system_table_name=?";
    private final Map<String, String> systemToLongTableName = new HashMap<>();
    private final Map<String, Optional<String>> longToSystemTableName = new HashMap<>();
    private final Map<String, String> systemToLongColumnName = new HashMap<>();
    private final Map<String, String> longToSystemColumnName = new HashMap<>();

    private final String realDatabaseName;
    
    private static Field[] fields = new Field[] {
    		// setting, display name, description, default
    		Field.create("thread used", "thread used", "use threads for fetching/receiving data setting this to 'true' typically doesn't end well", false),
    		Field.create("date format", "date format", "default date format is 2 digit date 1940->2039 set this to 'iso' or make sure you only have dates in this range, performance is ambysmal if you don't not to mention lots of missing data", "iso"),
    		Field.create("keep alive", "keep alive", "send heart beat to keep the connection alive", true),
    		Field.create("prompt", "prompt", "do you want a GUI prompt for the password if the password is wrong", false),
    		Field.create("errors", "full error reporting", "jdbc level of detail to include options are: 'basic', or 'full'", "full"),
    		Field.create("forced_ccsid", "force_ccsid",  "ignore all the information on your system and tables and use this encoding anyway", -1)
	};

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            AS400JDBCDriverForcedCcsid.class.getName(), As400JdbcConnection.class.getClassLoader(), fields);

    public As400JdbcConnection(JdbcConfiguration config) {
        super(config, FACTORY, "'", "'");
        this.forcedCcsid = config.getInteger(As400ConnectorConfig.FORCE_CCSID);
        this.config = config;
        realDatabaseName = retrieveRealDatabaseName();
        log.debug("connection:" + this.connectionString(URL_PATTERN));
    }

    public List<FileFilter> shortIncludes(String schema, String includes) {
        if (includes == null || includes.isBlank()) {
            return Collections.<FileFilter>emptyList();
        }
        String[] incs = includes.split(",");
        List<FileFilter> r = new ArrayList<>();
        for (String tableName: incs) {
            String schemaName = "";
            int o = tableName.lastIndexOf('.');
            if (o > 0) {
                schemaName = tableName.substring(0, o);
                tableName = tableName.substring(o + 1);
            }

            // This handles the case where the table name has been specified as "database.schema.table"
            if (!"".equals(schemaName)) {
                o = schemaName.lastIndexOf('.');
                if (o> 0 ) {
                    schemaName = schemaName.substring(o + 1);
                }
            }

            if ("".equals(schemaName)) {
                schemaName = schema;
            }

            final String tableSchema = schemaName;
            getSystemName(tableSchema, tableName).map(x -> r.add(new FileFilter(tableSchema, x)));
        }
        return r;
    }
    
    public static As400JdbcConnection forTestDatabase(String databaseName) {
        return new As400JdbcConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDatabase(databaseName).build());
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1).trim(), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    protected List<String> readPrimaryKeyOrUniqueIndexNames(DatabaseMetaData metadata, TableId id) throws SQLException {
        List<String> pkColumnNames = readPrimaryKeyNames(metadata, id);
        if (pkColumnNames.isEmpty()) {
            pkColumnNames = readAs400PrimaryKeys(id);
        }
        if (pkColumnNames.isEmpty()) {
            pkColumnNames = readTableUniqueIndices(metadata, id);
        }
        return pkColumnNames;
    }

    protected List<String> readAs400PrimaryKeys(TableId id) throws SQLException {
        List<String> columns = prepareQueryAndMap(GET_INDEXES,
                call -> {
                    call.setString(1, id.schema());
                    call.setString(2, id.table());
                },
                rs -> {
                    final List<String> indexColumns = new ArrayList<>();
                    while (rs.next()) {
                        indexColumns.add(rs.getString(1).trim());
                    }
                    return indexColumns;
                });
        return columns;
    }

    protected Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog, String schemaNamePattern,
                                                           String tableName, TableFilter tableFilter, ColumnNameFilter columnFilter, DatabaseMetaData metadata,
                                                           final Set<TableId> viewIds)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableName, null)) {
            while (columnMetadata.next()) {
                String catalogName = columnMetadata.getString(1);
                String schemaName = columnMetadata.getString(2);
                String metaTableName = columnMetadata.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, metaTableName);

                // exclude views and non-captured tables
                if (viewIds.contains(tableId) || (tableFilter != null && !tableFilter.isIncluded(tableId))) {
                    continue;
                }

                // add all included columns
                readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                    columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>()).add(column.create());
                });
            }
        }
        return columnsByTable;
    }

    public void getAllSystemNames(String schemaName) throws SQLException, InterruptedException {
        prepareQueryWithBlockingConsumer(GET_ALL_SYSTEM_TABLE_NAME, call -> {
            call.setString(1, schemaName);
        },
                (rs) -> {
                    while (rs.next()) {
                        String systemName = rs.getString(1);
                        String tableName = rs.getString(2);
                        String longKey = String.format("%s.%s", schemaName, tableName);
                        longToSystemTableName.put(longKey, Optional.of(systemName));
                        String shortKey = String.format("%s.%s", schemaName, systemName);
                        systemToLongTableName.put(shortKey, tableName);

                    }
                });
        log.info("fetched {} long names", longToSystemTableName.size());
    }

    public Optional<String> getSystemName(String schemaName, String longTableName) {
        String longKey = String.format("%s.%s", schemaName, longTableName);
        if (longToSystemTableName.containsKey(longKey)) {
            return longToSystemTableName.get(longKey);
        }
        else {
            try {
                String systemName = prepareQueryAndMap(GET_SYSTEM_TABLE_NAME,
                        call -> {
                            call.setString(1, schemaName);
                            call.setString(2, longTableName);
                        },
                        singleResultMapper(rs -> rs.getString(1).trim(), "Could not retrieve system table name"));
                if (systemName == null) {
                    systemName = longTableName;
                }
                String systemKey = String.format("%s.%s", schemaName, systemName);
                Optional<String> systemNameOpt = Optional.of(systemName);
                longToSystemTableName.put(longKey, systemNameOpt);
                systemToLongTableName.put(systemKey, longTableName);
                return systemNameOpt;
            }
            catch (IllegalStateException | SQLException e) {
                log.error("failed lookup for system name {}.{}", schemaName, longTableName, e);
                if (longTableName.length() > 10) {
                	return Optional.empty();
                }
                String systemName = longTableName;
                String systemKey = String.format("%s.%s", schemaName, systemName);
                longToSystemTableName.put(longKey, Optional.of(systemName));
                systemToLongTableName.put(systemKey, longTableName);
                return Optional.of(systemName);
            }
        }
    }

    public String getLongName(String schemaName, String systemName) {
        String systemKey = String.format("%s.%s", schemaName, systemName);
        if (schemaName.isEmpty() || systemName.isEmpty()) {
            return "";
        }
        if (systemToLongTableName.containsKey(systemKey)) {
            return systemToLongTableName.get(systemKey);
        }
        else {
            log.info("missed cache for {} {}", schemaName, systemName);
            String longTableName;
            try {
                longTableName = prepareQueryAndMap(GET_TABLE_NAME,
                        call -> {
                            call.setString(1, schemaName);
                            call.setString(2, systemName);
                        },
                        singleResultMapper(rs -> rs.getString(1).trim(), String.format("Could not retrieve long table name %s in %s", systemName, schemaName)));
                if (longTableName == null) {
                    log.warn("Failed to lookup table name {} in schema {}", systemName, schemaName);
                    longTableName = systemName;
                }
                String longKey = String.format("%s.%s", schemaName, longTableName);
                longToSystemTableName.put(longKey, Optional.of(systemName));
                systemToLongTableName.put(systemKey, longTableName);
                return longTableName;
            }
            catch (IllegalStateException | SQLException e) {
                log.warn("failed lookup for long name {}.{}", schemaName, systemName, e);
                String longKey = String.format("%s.%s", schemaName, systemName);
                longToSystemTableName.put(longKey, Optional.of(systemName));
                systemToLongTableName.put(systemKey, systemName);
                return systemName;
            }
        }
    }

    @Override
    public synchronized Connection connection() throws SQLException {
        if (forcedCcsid > 0 && !registered) {
        	AS400JDBCDriverRegistration.registerCcsidDriver();
			registered = true;
        }

        Connection conn = super.connection(true);
        if (!conn.isValid(3)) {
            log.info("connection dead closing");
            try {
                conn.close();
            }
            catch (Exception e) {
                // we can ignore this we just need a new connection
            }
            conn = super.connection(true);
        }
        else {
            log.debug("validated connection OK");
        }
        return conn;
    }
    
    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter) throws SQLException {

        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }
            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            column.nativeType(resolveNativeType(column.typeName()));
            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));

            // Allow implementation to make column changes if required before being added to table
            column = overrideColumn(column);

            // don't fetch defaults or we will send the default value when not set even though the database value is not set
            return Optional.of(column);
        }

        return Optional.empty();
    }
    
    @Override
    public Optional<Timestamp> getCurrentTimestamp() throws SQLException {
        return queryAndMap("values ( CURRENT TIMESTAMP )",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1)) : Optional.empty());
    }
}
