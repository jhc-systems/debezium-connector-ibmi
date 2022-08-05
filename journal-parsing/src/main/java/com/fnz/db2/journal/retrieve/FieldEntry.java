package com.fnz.db2.journal.retrieve;

public class FieldEntry {
	private final String name;
	private final Object value;
	
	public FieldEntry(String name, Object value) {
		super();
		this.name = name;
		this.value = value;
	}
	
	public String getName() {
		return name;
	}
	public Object getValue() {
		return value;
	}

	
}
