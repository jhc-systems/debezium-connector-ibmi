package com.fnz.db2.journal.retrieve.rnrn0200;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public record DetailedJournalReceiver(
        JournalReceiverInfo info, 
        BigInteger start, 
        BigInteger end, 
        String nextReceiver,
        String nextDualReceiver, 
        long maxEntryLength, 
        long numberOfEntries) {
	
	public static Optional<DetailedJournalReceiver> firstInLatestChain(List<DetailedJournalReceiver> l) {
		Optional<ReceiverChain> lastDisjointNamed = lastDisjointNamedReceivers(l);
		
		// find the first in latest chain
		Optional<DetailedJournalReceiver> firstInChain = lastDisjointNamed.map(x -> {
			DetailedJournalReceiver first = x.dr;
			while (x.tail != null) {
				if (!x.dr.info.chain().equals(x.tail.dr.info.chain())) {
					first = x.tail.dr;
				}
				x = x.tail;
			}
			return first;
		});
		
		return firstInChain;
	}
	
	// find last
	public static Optional<DetailedJournalReceiver> latest(List<DetailedJournalReceiver> l) {
		Optional<ReceiverChain> lastDisjointNamed = lastDisjointNamedReceivers(l);
		
		Optional<DetailedJournalReceiver> last = lastDisjointNamed.map(x -> {
			while (x.tail != null) {
				x = x.tail;
			}
			return x.dr;
		});
		return last;
	}
	
	static Optional<ReceiverChain> lastDisjointNamedReceivers(List<DetailedJournalReceiver> l) {
		List<DetailedJournalReceiver> filtered = l.stream().filter(x -> {
			switch (x.info.status()) {
				case Attached, OnlineSavedDetached, SavedDetchedNotFreed:
					return true;
				default:
					return false;
			}
		}).toList(); // ignore any deleted or unused journals
		
		Map<String, ReceiverChain> m = filtered.stream().collect(Collectors.<DetailedJournalReceiver, String, ReceiverChain>toMap(x -> x.info.name(), y -> new ReceiverChain(y, null)));
		
		Set<ReceiverChain> noNext = new HashSet<>(m.values()); // set of head elements
		
		// create an linked chain of receivers
		for (ReceiverChain or: m.values()) {
			if (or.dr.nextReceiver != null) {
				ReceiverChain nr = m.get(or.dr.nextReceiver);
				if (nr != null) {
					noNext.remove(nr);
					or.tail = nr;
				}
				ReceiverChain dualr = m.get(or.dr.nextDualReceiver);
				if (dualr != null) {
					noNext.remove(dualr);
				}
			}
		}


		Optional<ReceiverChain> lastDetached = noNext.stream().sorted((x, y) -> x.dr.info().attachTime().compareTo(y.dr.info().attachTime())).reduce((first, second) -> second);
		return lastDetached;
	}
	
	/**
	 * Used to create an ordered list based on the chained next receiver name
	 */
	static class ReceiverChain { 
		DetailedJournalReceiver dr;
		ReceiverChain tail;
		
		public ReceiverChain(DetailedJournalReceiver head, ReceiverChain tail) {
			super();
			if (head == null  || head.info() == null || head.info().name() == null) {
				throw new IllegalArgumentException("neither head, head.info or head.info.name can be null "+ head);
			}
			this.dr = head;
			this.tail = tail;
		}

		@Override
		public int hashCode() { // simplified hashcode and equals so we can lookup based on name only
			return Objects.hash(dr.info().name());
		}

		@Override
		public boolean equals(Object obj) { // simplified hashcode and equals so we can lookup based on name only
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ReceiverChain other = (ReceiverChain) obj;
			return this.dr.info().name().equals(other.dr.info().name());
		}

	}
};