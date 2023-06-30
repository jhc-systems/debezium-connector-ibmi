package com.fnz.db2.journal.retrieve;

import java.util.Optional;

public record PositionRange(JournalProcessedPosition start, JournalPosition end) {
}
