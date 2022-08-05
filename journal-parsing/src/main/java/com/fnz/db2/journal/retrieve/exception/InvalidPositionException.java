package com.fnz.db2.journal.retrieve.exception;

public class InvalidPositionException extends Exception {

    public InvalidPositionException(String message) {
        super(message);
    }

    public InvalidPositionException(Throwable cause) {
        super(cause);
    }

    public InvalidPositionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidPositionException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
