package com.fnz.db2.journal.retrieve.rnrn0200;

import java.util.Date;
import java.util.Optional;

public record JournalReceiverInfo(String name, String library, Date attachTime, JournalStatus status, Optional<Integer> chain) { 
	public JournalReceiverInfo withStatus(JournalStatus newStatus) {
		return new JournalReceiverInfo(name, library, attachTime, newStatus, chain);
	}	
}
