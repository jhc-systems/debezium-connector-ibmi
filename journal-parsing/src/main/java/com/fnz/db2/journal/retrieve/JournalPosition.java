/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;

public record JournalPosition( 
		BigInteger offset, // sequence number up to 18 446 644 000 000 000 000
		JournalReceiver receiver) {

    public JournalPosition(JournalPosition position) {
        this(position.offset, position.receiver);
    }
    
	public static JournalPosition endPosition(DetailedJournalReceiver details) {
		return new JournalPosition(details.end(), details.info().receiver());
	}
    
//    public JournalPosition(String offsetStr, String receiver, String receiverLibrary) {
//    	this(
//			(offsetStr == null || offsetStr.isBlank()) ? null : new BigInteger(offsetStr),
//	    	new JournalReceiver(StringHelpers.safeTrim(receiver), StringHelpers.safeTrim(receiverLibrary)));
//    }
//    
//    public JournalPosition(BigInteger offset, String receiver, String library) {
//		this(offset, new JournalReceiver(receiver, library));
//	}

	public BigInteger getOffset() {
        if (null == offset) {
            return BigInteger.ZERO;
        }
        return offset;
    }

    public JournalReceiver getReciever() {
        return receiver();
    }
}