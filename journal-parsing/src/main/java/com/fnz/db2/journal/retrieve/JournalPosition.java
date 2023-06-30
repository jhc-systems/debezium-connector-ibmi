/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;

public record JournalPosition( 
		BigInteger offset, // sequence number up to 18 446 644 000 000 000 000
	    String receiver,
	    String receiverLibrary) {

    public JournalPosition(JournalPosition position) {
        this(position.offset, position.receiver, position.receiverLibrary);
    }
    
    public JournalPosition(String offsetStr, String receiver, String receiverLibrary) {
    	this(
			(offsetStr == null || offsetStr.isBlank()) ? null : new BigInteger(offsetStr),
	    	StringHelpers.safeTrim(receiver),
	    	StringHelpers.safeTrim(receiverLibrary));
    }
    
    public BigInteger getOffset() {
        if (null == offset) {
            return BigInteger.ZERO;
        }
        return offset;
    }

    public String getReciever() {
        return receiver;
    }

    public String getReceiverLibrary() {
        return receiverLibrary;
    }

	@Override
	public String toString() {
		return String.format("JournalPosition [offset=%s, receiver=%s, receiverLibrary=%s]", offset, receiver,
				receiverLibrary);
	}
}