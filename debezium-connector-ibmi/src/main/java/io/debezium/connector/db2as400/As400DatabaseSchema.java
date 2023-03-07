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

import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
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
    private final SchemaInfoConversion schemaInfoConversion;
	private final JdbcFileDecoder fileDecoder;
    
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
		fileDecoder = new JdbcFileDecoder(jdbcConnection, jdbcConnection.getRealDatabaseName(), this, config.getForcedCcsid());

        schemaInfoConversion = new SchemaInfoConversion(fileDecoder);
    }
    
    public JdbcFileDecoder getFileDecoder() {
    	return fileDecoder;
    }

	public void clearCache(String systemTableName, String schema) {
		fileDecoder.clearCache(systemTableName, schema);
	}
	
	public Optional<TableInfo> getRecordFormat(String systemTableName, String schema) {
		Optional<TableInfo> oti = fileDecoder.getRecordFormat(systemTableName, schema);
		oti.stream().forEach(ti -> {
			store(jdbcConnection.getRealDatabaseName(), schema, systemTableName, ti);
		});
		return oti;
	}
	
    // via the create table journal entry (short names)
    public void addSystemSchema(Table table) {
        TableId id = table.id();
        final Optional<String> systemTableNameOpt = jdbcConnection.getSystemName(id.schema(), id.table());
        // save for decoding
        systemTableNameOpt.map(systemTableName -> {
            TableInfo tableInfo = schemaInfoConversion.table2TableInfo(table);
    		map.put(id.catalog() + id.schema() + systemTableName, tableInfo);
    		return map.put(id.catalog() + id.schema() + id.table(), tableInfo);
		});

        Table longTable = jdbcConnection.shortToLongTable(table);
        // save for serialisation
        tables().overwriteTable(longTable);
        this.buildAndRegisterSchema(longTable);
    }
    
    public void addLongSchema(Table table) {
        TableId id = table.id();
        // save for decoding
        final Optional<String> systemTableNameOpt = jdbcConnection.getSystemName(id.schema(), id.table());
        systemTableNameOpt.flatMap(systemTableName -> {
        	Optional<Table> shortTableOpt = jdbcConnection.longToShortTable(table);
        	return shortTableOpt.map(shortTable -> {
	            TableInfo tableInfo = schemaInfoConversion.table2TableInfo(shortTable);
	    		map.put(id.catalog() + id.schema() + systemTableName, tableInfo);
	    		return map.put(id.catalog() + id.schema() + id.table(), tableInfo);
    		});
		});

        // save for serialisation
        tables().overwriteTable(table);
        this.buildAndRegisterSchema(table);
    }

    public String getSchemaName() {
        return config.getSchema();
    }

    @Override
    // implements SchemaCacheIF.store - system name tables/column names
    public void store(String database, String schema, String tableName, TableInfo tableInfo) {
        map.put(database + schema + tableName, tableInfo);

        Table table = SchemaInfoConversion.tableInfo2Table(database, schema, tableName, tableInfo);
        addSystemSchema(table);
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
