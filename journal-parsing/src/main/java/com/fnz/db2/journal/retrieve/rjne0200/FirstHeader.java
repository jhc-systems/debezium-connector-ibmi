package com.fnz.db2.journal.retrieve.rjne0200;

import java.util.Optional;

import com.fnz.db2.journal.retrieve.JournalPosition;

public record FirstHeader(
        int totalBytes, 
        int offset, 
        int size, 
        OffsetStatus status,
        Optional<JournalPosition> nextPosition) { 
	
	public boolean hasData() {
	    return status == OffsetStatus.MORE_DATA_NEW_OFFSET || offset > 0;
	}

	@Override
    public String toString() {
        return "FirstHeader [totalBytes=" + totalBytes + ", offset=" + offset + ", size=" + size + ", status=" + status
                + ", nextPosition=" + nextPosition + "]";
    }

    public FirstHeader withCurrentJournalPosition(JournalPosition pos) {
	    return new FirstHeader(totalBytes, offset, size, OffsetStatus.NO_MORE_DATA_NEW_OFFSET, Optional.of(pos));
	}
    
    public boolean hasFutureDataAvailable() {
        return status == OffsetStatus.MORE_DATA_NEW_OFFSET;
    }
}
