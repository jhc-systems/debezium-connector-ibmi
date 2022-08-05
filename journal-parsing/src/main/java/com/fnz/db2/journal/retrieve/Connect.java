package com.fnz.db2.journal.retrieve;

public interface Connect<T, E extends Exception> {
	T connection() throws E;
}
