package com.fnz.db2.journal.retrieve.rjne0200;

import com.fnz.db2.journal.retrieve.JournalProcessedPosition;

public record FirstHeader(
		int totalBytes,
		int offset,
		int size,
		OffsetStatus status,
		JournalProcessedPosition nextPosition) {

	public boolean hasData() {
		return status == OffsetStatus.MORE_DATA_NEW_OFFSET || offset > 0;
	}

	@Override
	public String toString() {
		return String.format("FirstHeader [totalBytes=%s, offset=%s, size=%s, status=%s, nextPosition=%s]", totalBytes,
				offset, size, status, nextPosition);
	}

	public boolean hasFutureDataAvailable() {
		return status == OffsetStatus.MORE_DATA_NEW_OFFSET;
	}
}
