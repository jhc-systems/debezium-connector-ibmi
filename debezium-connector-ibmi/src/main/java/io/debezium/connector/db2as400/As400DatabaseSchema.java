/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF;

import io.debezium.connector.db2as400.conversion.As400DefaultValueConverter;
import io.debezium.connector.db2as400.conversion.SchemaInfoConversion;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.SchemaNameAdjuster;

public class As400DatabaseSchema extends RelationalDatabaseSchema implements SchemaCacheIF {

    private static final Logger log = LoggerFactory.getLogger(As400DatabaseSchema.class);
    private final As400ConnectorConfig config;
    private final Map<String, TableInfo> map = new HashMap<>();
    private final As400JdbcConnection jdbcConnection;

    public As400DatabaseSchema(As400ConnectorConfig config, 
    		As400JdbcConnection jdbcConnection,
			TopicNamingStrategy<TableId> topicSelector,
            SchemaNameAdjuster schemaNameAdjuster) {
        super(config,
                topicSelector,
                config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        new As400ValueConverters(),
                        new As400DefaultValueConverter(),
                        schemaNameAdjuster,
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getSanitizeFieldNames(),
                        false),
                false,
                config.getKeyMapper());

        this.config = config;
        this.jdbcConnection = jdbcConnection;
    }

    public void addSchema(Table table) {
        TableInfo tableInfo = SchemaInfoConversion.table2TableInfo(table);
        TableId id = table.id();
        // store in the short name
        // used directly by the decoder
        final Optional<String> systemTableNameOpt = jdbcConnection.getSystemName(id.schema(), id.table());
        systemTableNameOpt.map(systemTableName -> map.put(id.catalog() + id.schema() + systemTableName, tableInfo));

        // and store the long name
        // use for serialisation
        map.put(id.catalog() + id.schema() + id.table(), tableInfo);
        tables().overwriteTable(table);
        this.buildAndRegisterSchema(table);
    }

    public String getSchemaName() {
        return config.getSchema();
    }

    @Override
    public void store(String database, String schema, String tableName, TableInfo tableInfo) {
        map.put(database + schema + tableName, tableInfo);

        Table table = SchemaInfoConversion.tableInfo2Table(database, schema, tableName, tableInfo);
        addSchema(table);
    }

    @Override
    public TableInfo retrieve(String database, String schema, String tableName) {
        return map.get(database + schema + tableName);
    }

    @Override
    public void clearCache(String database, String schema, String tableName) {
        map.remove(database + schema + tableName);
    }

}
