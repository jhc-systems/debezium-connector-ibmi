/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Objects;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

/** class for just the position */
public class JournalProcessedPosition {
	// position should be last processed record as requesting the next record will error and be indistinguishable from losing the journal 
    private BigInteger offset; // sequence number up to 18 446 644 000 000 000 000
    private String receiver;
    private String receiverLibrary;
    private Instant time = Instant.ofEpochSecond(0);
    private boolean processed = false;
    private static String[] empty = new String[]{};

    public JournalProcessedPosition(JournalProcessedPosition position) {
        this.offset = position.offset;
        this.receiver = position.receiver;
        this.receiverLibrary = position.receiverLibrary;
		this.processed = position.processed;
		this.time = position.time;
    }
    
    public JournalProcessedPosition() {
    }
    
    public JournalProcessedPosition(JournalPosition p, Instant time, boolean processed) {
    	this(p.getOffset(), p.getReciever(), p.getReceiverLibrary(), time, processed);
    }
    
    public JournalPosition asJournalPosition() {
    	return new JournalPosition(this.offset, this.receiver, this.receiverLibrary);
    }
    
    public boolean processed() {
    	return processed;
    }
    
    public JournalProcessedPosition(String offsetStr, String receiver, String receiverLibrary, Instant time, boolean processed) {
        if (offsetStr == null || offsetStr.isBlank()) {
        	this.offset = null;
        } else {
        	this.offset = new BigInteger(offsetStr);
        }
        this.receiver = StringHelpers.safeTrim(receiver);
        this.receiverLibrary = StringHelpers.safeTrim(receiverLibrary);
    	this.time = time;
		this.processed = processed;
    }

    public JournalProcessedPosition(BigInteger offset, String receiver, String receiverLibrary, Instant time, boolean processed) {
        this.offset = offset;
        this.receiver = StringHelpers.safeTrim(receiver);
        this.receiverLibrary = StringHelpers.safeTrim(receiverLibrary);
    	this.time = time;
		this.processed = processed;
    }

    public BigInteger getOffset() {
        if (null == offset) {
            return BigInteger.ZERO;
        }
        return offset;
    }
    
    public Instant getTime() {
    	return this.time;
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
		return Objects.hash(offset, processed, receiver, receiverLibrary);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JournalProcessedPosition other = (JournalProcessedPosition) obj;
		return Objects.equals(offset, other.offset) && processed == other.processed
				&& Objects.equals(receiver, other.receiver) && Objects.equals(receiverLibrary, other.receiverLibrary);
	}
	
	@Override
	public String toString() {
		return String.format(
				"JournalProcessedPosition [offset=%s, receiver=%s, receiverLibrary=%s, time=%s, processed=%s]", offset,
				receiver, receiverLibrary, time, processed);
	}
	
	// TODO remove all setters and convert to record
	public JournalProcessedPosition setOffset(BigInteger offset, Instant time, boolean processed) {
	  this.offset = offset;
		this.processed = processed;
		this.time = time;
		return this;
	}
	
	public JournalProcessedPosition setProcessed(boolean processed) {
		this.processed = processed;
		return this;
	}

	public void setJournalReciever(BigInteger offset, String journalReciever, String schema, Instant time, boolean processed) {
	  this.offset = offset;
	  this.receiver = StringHelpers.safeTrim(journalReciever);
	  this.receiverLibrary = StringHelpers.safeTrim(schema);
		this.processed = processed;
		this.time = time;
	}
	
	public void setPosition(JournalProcessedPosition newPosition) {
	    this.offset = newPosition.offset;
    	this.receiver = newPosition.receiver;
    	this.receiverLibrary = newPosition.receiverLibrary;
    	this.time = newPosition.time;
	    this.processed = newPosition.processed;
	}

	public void setPosition(JournalPosition newPosition, boolean processed) {
	    this.offset = newPosition.getOffset();
    	this.receiver = newPosition.getReciever();
    	this.receiverLibrary = newPosition.getReceiverLibrary();
	    this.processed = processed;
	}
	
	public boolean isSameReceiver(DetailedJournalReceiver other) {
		if (receiver == null ||receiverLibrary == null || other.info() == null)
			return false;
		return receiver.equals(other.info().name()) && receiverLibrary.equals(other.info().library());
	}
}