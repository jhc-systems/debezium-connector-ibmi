/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;

public class JournalPosition {
	// position should be last processed record as requesting the next record will error and be indistinguishable from losing the journal 
    private BigInteger offset; // sequence number up to 18 446 644 000 000 000 000
    private String receiver;
    private String receiverLibrary;
    private boolean processed = false;
    private static String[] empty = new String[]{};

    public JournalPosition(JournalPosition position) {
        this.offset = position.offset;
        this.receiver = position.receiver;
        this.receiverLibrary = position.receiverLibrary;
		this.processed = position.processed;
    }

    public JournalPosition() {
    }
    
    public boolean processed() {
    	return processed;
    }
    
    public JournalPosition(String offsetStr, String receiver, String receiverLibrary, boolean processed) {
        if (offsetStr == null || offsetStr.isBlank()) {
        	this.offset = null;
        } else {
        	this.offset = new BigInteger(offsetStr);
        }
        this.receiver = StringHelpers.safeTrim(receiver);
        this.receiverLibrary = StringHelpers.safeTrim(receiverLibrary);
		this.processed = processed;
    }

    public JournalPosition(BigInteger offset, String receiver, String receiverLibrary, boolean processed) {
        this.offset = offset;
        this.receiver = StringHelpers.safeTrim(receiver);
        this.receiverLibrary = StringHelpers.safeTrim(receiverLibrary);
		this.processed = processed;
    }

    public BigInteger getOffset() {
        if (null == offset) {
            return BigInteger.ZERO;
        }
        return offset;
    }
    
    public boolean isOffsetSet() {
        return (null != offset);
    }

    public String getReciever() {
        return receiver;
    }

    public String getReceiverLibrary() {
        return receiverLibrary;
    }

    public JournalPosition setOffset(BigInteger offset, boolean processed) {
        this.offset = offset;
    	this.processed = processed;
    	return this;
    }
    
    public JournalPosition setProcessed(boolean processed) {
    	this.processed = processed;
    	return this;
    }
    
    public void setJournalReciever(BigInteger offset, String journalReciever, String schema, boolean processed) {
        this.offset = offset;
        this.receiver = StringHelpers.safeTrim(journalReciever);
        this.receiverLibrary = StringHelpers.safeTrim(schema);
    	this.processed = processed;
    }

    public String[] getJournal() {
        if (receiver != null && receiverLibrary != null) {
            return new String[]{ receiver, receiverLibrary, receiver, receiverLibrary };
        }
        else {
            return empty;
        }
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((receiver == null) ? 0 : receiver.hashCode());
		result = prime * result + ((offset == null) ? 0 : offset.hashCode());
		result = prime * result + (processed ? 1231 : 1237);
		result = prime * result + ((receiverLibrary == null) ? 0 : receiverLibrary.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JournalPosition other = (JournalPosition) obj;
		if (receiver == null) {
			if (other.receiver != null)
				return false;
		} else if (!receiver.equals(other.receiver))
			return false;
		if (offset == null) {
			if (other.offset != null)
				return false;
		} else if (!offset.equals(other.offset))
			return false;
		if (processed != other.processed)
			return false;
		if (receiverLibrary == null) {
			if (other.receiverLibrary != null)
				return false;
		} else if (!receiverLibrary.equals(other.receiverLibrary))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "JournalPosition [offset=" + offset + ", receiver=" + receiver + ", receiverLibrary=" + receiverLibrary
				+ ", processed=" + processed + "]";
	}

	public void setPosition(JournalPosition newPosition) {
	    this.offset = newPosition.offset;
    	this.receiver = newPosition.receiver;
    	this.receiverLibrary = newPosition.receiverLibrary;
	    this.processed = newPosition.processed;
	}
}
