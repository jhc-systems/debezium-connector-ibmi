/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.exception;

public class InvalidJournalFilterException extends FatalException {

    public InvalidJournalFilterException(String message) {
        super(message);
    }

    public InvalidJournalFilterException(Throwable cause) {
        super(cause);
    }

    public InvalidJournalFilterException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidJournalFilterException(String message, Throwable cause, boolean enableSuppression,
                                         boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
