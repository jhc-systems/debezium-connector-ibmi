package com.fnz.db2.journal.retrieve.exception;

public class FatalException extends Exception {

	public FatalException() {
	}

	public FatalException(String message) {
		super(message);
	}

	public FatalException(Throwable cause) {
		super(cause);
	}

	public FatalException(String message, Throwable cause) {
		super(message, cause);
	}

	public FatalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
