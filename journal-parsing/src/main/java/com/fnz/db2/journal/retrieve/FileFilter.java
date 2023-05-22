package com.fnz.db2.journal.retrieve;

public class FileFilter {
    private final String schema;
    private final String tableName;
    
    public FileFilter(String schema, String tableName) {
        this.schema = schema;
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }
    public String getTableName() {
        return tableName;
    }

	@Override
	public String toString() {
		return String.format("FileFilter [schema=%s, tableName=%s]", schema, tableName);
	}
}
