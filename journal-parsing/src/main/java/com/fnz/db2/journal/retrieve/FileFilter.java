package com.fnz.db2.journal.retrieve;

public class FileFilter {
    private final String schema;
    private final String table;
    
    public FileFilter(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }
    public String getTable() {
        return table;
    }

	@Override
	public String toString() {
		return String.format("FileFilter [schema=%s, table=%s]", schema, table);
	}
}
