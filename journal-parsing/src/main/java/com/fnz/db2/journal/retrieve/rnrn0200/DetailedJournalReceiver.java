package com.fnz.db2.journal.retrieve.rnrn0200;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public record DetailedJournalReceiver(
        JournalReceiverInfo info, 
        BigInteger start, 
        BigInteger end, 
        String nextReceiver,
        String nextDualReceiver, 
        long maxEntryLength, 
        long numberOfEntries) {
	
	public static Optional<DetailedJournalReceiver> firstInLatestChain(List<DetailedJournalReceiver> l) {
		Collections.sort(l, (x, y) -> y.info.attachTime().compareTo(x.info.attachTime()));
		Optional<DetailedJournalReceiver> firstInChain = l.stream().reduce((x,y) -> {
			if (x.info.chain().equals(y.info.chain())) {
				return y;
			}
			return x;
		});
		return firstInChain;
	}
	
	public static Optional<DetailedJournalReceiver> latest(List<DetailedJournalReceiver> l) {
		return l.stream().max((x, y) -> y.info.attachTime().compareTo(x.info.attachTime()));
	}
};