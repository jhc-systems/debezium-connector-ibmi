package com.fnz.db2.journal.retrieve.rnrn0200;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public	class ReceiverChain {
	DetailedJournalReceiver details;
	ReceiverChain next;
	ReceiverChain previous;

	/**
	 * assumed to be unique on DetailedJournalReceiver.info.name
	 * @param details
	 * @param next
	 */
	public ReceiverChain(DetailedJournalReceiver details) {
		super();
		if (details == null || details.info() == null || details.info().name() == null) {
			throw new IllegalArgumentException("neither head, head.info or head.info.name can be null " + details);
		}
		this.details = details;
	}

	@Override
	public int hashCode() { // simplified hashcode and equals so we can lookup based on name only
		return Objects.hash(details.info().name());
	}

	@Override
	public boolean equals(Object obj) { // simplified hashcode and equals so we can lookup based on name only
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final ReceiverChain other = (ReceiverChain) obj;
		return this.details.info().name().equals(other.details.info().name());
	}
	
		@Override
	public String toString() {
		return String.format("ReceiverChain [details=%s, next=%s, previous=%s]", details, next, previous);
	}

	public static List<DetailedJournalReceiver> chainContaining(List<DetailedJournalReceiver> l, DetailedJournalReceiver needle) {
		final Map<String, ReceiverChain> m = availableSingleChainElement(l);

		buildReceiverChains(m);

		final Optional<ReceiverChain> lastDetached = findChain(m, needle);
		
		return lastDetached.map(x ->chainToList(x)).orElse(Collections.emptyList());
	}

	static Map<String, ReceiverChain> availableSingleChainElement(List<DetailedJournalReceiver> l) {
		final List<DetailedJournalReceiver> validReceivers = l.stream().filter(x -> {
			return switch (x.info().status()) {
			case Attached, OnlineSavedDetached, SavedDetchedNotFreed -> true;
			default -> false;
			};
		}).toList(); // ignore any deleted or unused journals

		final Map<String, ReceiverChain> m = validReceivers.stream()
				.collect(Collectors.<DetailedJournalReceiver, String, ReceiverChain>toMap(x -> x.info().name(),
						y -> new ReceiverChain(y)));
		return m;
	}

	static Optional<ReceiverChain> findChain(final Map<String, ReceiverChain> m, DetailedJournalReceiver needle) {
		String key = needle.info().name();
		if (m.containsKey(key)) {
			ReceiverChain rc = m.get(key);
			while (rc.previous != null) {
				rc = rc.previous;
			}
			return Optional.of(rc);
		} else {
			return Optional.empty();
		}
	}
	
	static List<DetailedJournalReceiver> chainToList(ReceiverChain chain) {
		List<DetailedJournalReceiver> l = new ArrayList<>();
		l.add(chain.details);
		
		while (chain.next != null) {
			chain = chain.next;
			l.add(chain.details);
		}
		return l;
	}
	
	static Set<ReceiverChain> buildReceiverChains(final Map<String, ReceiverChain> m) {
		final Set<ReceiverChain> noNext = new HashSet<>(m.values()); // set of all receivers not mentioned by others
	
		// each receiver lookup nextReceiver in hash and add to next 
		// references to other receivers to remove from noNext
		for (final ReceiverChain or : m.values()) {
			if (or.details.nextReceiver() != null) {
				String nextReceiverName = or.details.nextReceiver();
				if (nextReceiverName != null && !nextReceiverName.isBlank()) {
					final ReceiverChain nr = m.get(nextReceiverName);
					if (nr != null) {
						noNext.remove(nr);
						or.next = nr;
						nr.previous = or;
					}
				}
			}
		}
		return noNext;
	}
}