/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

public record FileFilter(String schema, String table) {

    @Override
    public String toString() {
        return String.format("FileFilter [%s.%s]", schema, table);
    }

    public String toShortString() {
        return String.format("[%s.%s]", schema, table);
    }
}
