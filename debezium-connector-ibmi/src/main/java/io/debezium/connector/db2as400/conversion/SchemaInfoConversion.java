/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.conversion;

import java.util.ArrayList;
import java.util.List;

import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;

import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.Structure;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * TableInfo and Table serve essentially the same purpose but the journal parsing library doesn't depend on debezium
 * @author sillencem
 *
 */
public class SchemaInfoConversion {
    private final JdbcFileDecoder fileDecoder;

    public SchemaInfoConversion(JdbcFileDecoder fileDecoder) {
        this.fileDecoder = fileDecoder;
    }

    public TableInfo table2TableInfo(Table table) {
        List<Structure> structures = table2Structure(table);
        AS400Structure as400Structure = table2As400Structure(table);
        List<String> primaryKeys = new ArrayList<>(table.primaryKeyColumnNames());

        return new TableInfo(structures, primaryKeys, as400Structure);
    }

    public static List<Structure> table2Structure(Table table) {
        List<Structure> structures = new ArrayList<>();
        if (table != null && table.columns() != null) {
            for (Column c : table.columns()) {
                Structure structure = new Structure(c.name(), c.typeName(), c.jdbcType(), c.length(), c.scale().orElse(0), c.isOptional(), c.position(),
                        c.isAutoIncremented());
                structures.add(structure);
            }
        }
        return structures;
    }

    public AS400Structure table2As400Structure(Table table) {
        TableId id = table.id();
        List<AS400DataType> as400structure = new ArrayList<>();
        if (table != null && table.columns() != null) {
            for (Column c : table.columns()) {
                AS400DataType as400dt = fileDecoder.toDataType(id.schema(), id.table(), c.name(), c.typeName(), c.length(), c.scale().orElse(0));
                as400structure.add(as400dt);
            }
        }

        AS400Structure as400Structure = new AS400Structure(as400structure.toArray(new AS400DataType[as400structure.size()]));
        return as400Structure;
    }

    public static Table tableInfo2Table(String database, String schema, String tableName, TableInfo tableInfo) {
        TableEditor editor = Table.editor();
        TableId id = new TableId(database, schema, tableName);
        editor.tableId(id);
        if (tableInfo != null) {
            List<Structure> structure = tableInfo.getStructure();
            if (structure != null) {
                for (Structure col : structure) {
                    ColumnEditor ceditor = Column.editor();
                    ceditor.jdbcType(col.getJdcbType());
                    ceditor.type(col.getType());
                    ceditor.length(col.getLength());
                    ceditor.scale(col.getPrecision());
                    ceditor.name(col.getName());
                    ceditor.autoIncremented(col.isAutoinc());
                    ceditor.optional(col.isOptional());
                    ceditor.position(col.getPosition());

                    editor.addColumn(ceditor.create());
                }
            }
            editor.setPrimaryKeyNames(tableInfo.getPrimaryKeys());
        }

        return editor.create();
    }
}
