package com.fnz.db2.journal.retrieve;

public record PositionRange(JournalProcessedPosition start, JournalPosition end) {
	boolean startEqualsEnd() {
		if (start.processed())
			return false;
		JournalPosition startPos = new JournalPosition(start.getOffset(), start.getReciever());
		return (start.processed() && startPos.equals(end));
	}
}
