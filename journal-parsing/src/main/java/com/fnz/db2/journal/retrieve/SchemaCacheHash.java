package com.fnz.db2.journal.retrieve;

import java.util.HashMap;

public class SchemaCacheHash implements SchemaCacheIF {
	private final HashMap<String, TableInfo> map = new HashMap<>();

	@Override
	public void store(String database, String schema, String table, TableInfo entry) {
		map.put(database + schema + table,  entry);
	}

	@Override
	public TableInfo retrieve(String database, String schema, String table) {
		return map.get(database + schema + table);
	}

	@Override
	public void clearCache(String database, String schema, String table) {
		map.remove(database + schema + table);
	}

}
