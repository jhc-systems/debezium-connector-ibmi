package com.fnz.db2.journal.retrieve.rnrn0200;

import java.util.Date;
import java.util.Optional;

import com.fnz.db2.journal.retrieve.JournalReceiver;

public record JournalReceiverInfo(JournalReceiver receiver, Date attachTime, JournalStatus status, Optional<Integer> chain) { 
	public JournalReceiverInfo withStatus(JournalStatus newStatus) {
		return new JournalReceiverInfo(receiver, attachTime, newStatus, chain);
	}
}