package com.fnz.db2.journal.retrieve;

public record FileFilter (String schema, String table) {

	@Override
	public String toString() {
		return String.format("FileFilter [schema=%s, table=%s]", schema, table);
	}
}
