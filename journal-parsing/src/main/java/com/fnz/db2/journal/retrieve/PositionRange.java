package com.fnz.db2.journal.retrieve;

public record PositionRange(boolean fromBeginning, JournalProcessedPosition start, JournalPosition end) {
	boolean startEqualsEnd() {
		JournalPosition startPos = new JournalPosition(start.getOffset(), start.getReceiver());
		return (startPos.equals(end));
	}
}
