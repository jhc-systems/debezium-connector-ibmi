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
		return String.format("FirstHeader [totalBytes=%s, offset=%s, size=%s, status=%s, nextPosition=%s]", totalBytes,
				offset, size, status, nextPosition);
	}

	public FirstHeader withNextJournalPosition(JournalPosition pos) {
	    return new FirstHeader(totalBytes, offset, size, OffsetStatus.NO_MORE_DATA_NEW_OFFSET, Optional.of(pos));
	}
    
    public boolean hasFutureDataAvailable() {
        return status == OffsetStatus.MORE_DATA_NEW_OFFSET;
    }
}
