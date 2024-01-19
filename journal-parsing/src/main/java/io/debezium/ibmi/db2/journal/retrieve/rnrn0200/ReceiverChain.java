/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

public class ReceiverChain {
    DetailedJournalReceiver details;
    Optional<ReceiverChain> next = Optional.empty();
    Optional<ReceiverChain> previous = Optional.empty();

    /**
     * assumed to be unique on DetailedJournalReceiver.info.name
     * @param details
     * @param next
     */
    public ReceiverChain(DetailedJournalReceiver details) {
        super();
        if (details == null || details.info() == null || details.info().receiver() == null || details.info().receiver().name() == null) {
            throw new IllegalArgumentException("neither head, head.info or head.info.name can be null " + details);
        }
        this.details = details;
    }

    @Override
    public int hashCode() { // simplified hashcode and equals so we can lookup based on receiver only
        return Objects.hash(details.info().receiver());
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
        return this.details.info().receiver().equals(other.details.info().receiver());
    }

    @Override
    public String toString() {
        return String.format("ReceiverChain [details=%s, next=%s, previous=%s]", details, next, previous);
    }

    public static List<DetailedJournalReceiver> chainContaining(List<DetailedJournalReceiver> l, DetailedJournalReceiver needle) {
        final Map<JournalReceiver, ReceiverChain> m = availableSingleChainElement(l);

        buildReceiverChains(m);

        final Optional<ReceiverChain> lastDetached = findChain(m, needle);

        return lastDetached.map(x -> chainToList(x)).orElse(Collections.emptyList());
    }

    static Map<JournalReceiver, ReceiverChain> availableSingleChainElement(List<DetailedJournalReceiver> l) {
        final List<DetailedJournalReceiver> validReceivers = l.stream().filter(x -> switch (x.info().status()) {
            case Attached, OnlineSavedDetached, SavedDetchedNotFreed -> true;
            default -> false;
        }).toList(); // ignore any deleted or unused journals

        final Map<JournalReceiver, ReceiverChain> m = validReceivers.stream()
                .collect(Collectors.<DetailedJournalReceiver, JournalReceiver, ReceiverChain> toMap(x -> x.info().receiver(),
                        y -> new ReceiverChain(y)));
        return m;
    }

    /**
     * Finds the first element in the chain with this receiver or empty if not in map
     * @param m
     * @param needle
     * @return
     */
    static Optional<ReceiverChain> findChain(final Map<JournalReceiver, ReceiverChain> m, DetailedJournalReceiver needle) {
        JournalReceiver key = needle.info().receiver();
        if (m.containsKey(key)) {
            ReceiverChain rc = m.get(key);
            while (rc.previous.isPresent()) {
                rc = rc.previous.get();
            }
            return Optional.of(rc);
        }
        else {
            return Optional.empty();
        }
    }

    static List<DetailedJournalReceiver> chainToList(ReceiverChain chain) {
        List<DetailedJournalReceiver> l = new ArrayList<>();
        l.add(chain.details);

        while (chain.next.isPresent()) {
            chain = chain.next.get();
            l.add(chain.details);
        }
        return l;
    }

    static Set<ReceiverChain> buildReceiverChains(final Map<JournalReceiver, ReceiverChain> m) {
        final Set<ReceiverChain> noNext = new HashSet<>(m.values()); // set of all receivers not mentioned by others

        // each receiver lookup nextReceiver in hash and add to next
        // references to other receivers to remove from noNext
        for (final ReceiverChain or : m.values()) {
            or.details.nextReceiver().ifPresent(nextReceiver -> {
                if (m.containsKey(nextReceiver)) {
                    final ReceiverChain nr = m.get(nextReceiver);
                    noNext.remove(nr);
                    or.next = Optional.of(nr);
                    nr.previous = Optional.of(or);
                }
            });
        }
        return noNext;
    }
}