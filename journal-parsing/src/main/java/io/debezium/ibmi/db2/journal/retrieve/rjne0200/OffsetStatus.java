/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rjne0200;

public enum OffsetStatus {
    NOT_CALLED,
    NO_DATA,
    DATA,
    MORE_DATA_NEW_OFFSET
}
