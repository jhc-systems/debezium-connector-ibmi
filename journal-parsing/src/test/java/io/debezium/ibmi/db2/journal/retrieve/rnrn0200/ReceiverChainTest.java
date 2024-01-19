/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class ReceiverChainTest {

    DetailedJournalReceiver dr6 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("6", "lib"), new Date(6),
            JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, Optional.empty(), 1, 1);
    DetailedJournalReceiver dr5 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("5", "lib"), new Date(5),
            JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, Optional.of(dr6.info().receiver()), 1, 1);
    DetailedJournalReceiver dr4 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("4", "lib"), new Date(4),
            JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, Optional.of(dr5.info().receiver()), 1, 1);
    DetailedJournalReceiver dr3 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("3", "lib"), new Date(3),
            JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, Optional.of(dr4.info().receiver()), 1, 1);
    DetailedJournalReceiver dr2 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("2", "lib"), new Date(2),
            JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, Optional.of(dr3.info().receiver()), 1, 1);
    DetailedJournalReceiver dr1 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("1", "lib"), new Date(1),
            JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, Optional.of(dr2.info().receiver()), 1, 1);

    DetailedJournalReceiver dr1Unavailable = dr1.withStatus(JournalStatus.Partial);

    @BeforeEach
    protected void setUp() throws Exception {
    }

    @Test
    void testAvailableSingleChainElement() {
        DetailedJournalReceiver available = dr2;
        List<DetailedJournalReceiver> l = Arrays.asList(
                dr1Unavailable,
                available);
        Map<JournalReceiver, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
        Map<JournalReceiver, ReceiverChain> expected = new HashMap<>();
        expected.put(available.info().receiver(), new ReceiverChain(available));
        assertEquals(expected, map);
    }

    @Test
    void testBuildReceiverChains() {
        DetailedJournalReceiver firstAvailable = dr2;
        DetailedJournalReceiver secondAvailable = dr3;
        List<DetailedJournalReceiver> l = Arrays.asList(
                dr1Unavailable,
                firstAvailable,
                secondAvailable);
        Map<JournalReceiver, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
        assertEquals(2, map.size());

        Set<ReceiverChain> detachedSet = ReceiverChain.buildReceiverChains(map);
        assertEquals(1, detachedSet.size());

        ReceiverChain chain = detachedSet.iterator().next();
        assertEquals(firstAvailable, chain.details, "first avaiable");
        assertEquals(secondAvailable, chain.next.get().details, "second availabe");
        assertEquals(Optional.empty(), chain.next.get().next, "end of list");
    }

    @Test
    void testFindCurrentChain() {
        List<DetailedJournalReceiver> l = Arrays.asList(
                dr1.withStatus(JournalStatus.Partial), // break in chain no next receiver
                dr2,
                dr3);
        Map<JournalReceiver, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
        assertEquals(2, map.size());

        Set<ReceiverChain> detachedSet = ReceiverChain.buildReceiverChains(map);
        assertEquals(1, detachedSet.size());

        Optional<ReceiverChain> chainOpt = ReceiverChain.findChain(map, dr3);

        assertTrue(chainOpt.isPresent(), "chain found");
        ReceiverChain chain = chainOpt.get();
        assertEquals(dr2, chain.details, "first avaiable");
        assertEquals(dr3, chain.next.get().details, "second availabe");
        assertEquals(Optional.empty(), chain.next.get().next, "end of list");
    }

    @Test
    void testFindCurrentChainWithDetachedChains() {
        List<DetailedJournalReceiver> l = Arrays.asList(
                dr1,
                dr2,
                dr3,
                new DetailedJournalReceiver(dr4.info(), dr4.start(), dr4.end(), Optional.empty(), dr4.maxEntryLength(), dr4.numberOfEntries()), // break in chain no next receiver
                dr5,
                dr6);
        Map<JournalReceiver, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
        assertEquals(6, map.size());

        Set<ReceiverChain> detachedSet = ReceiverChain.buildReceiverChains(map);
        assertEquals(2, detachedSet.size());

        Optional<ReceiverChain> chainOpt = ReceiverChain.findChain(map, dr6);

        assertTrue(chainOpt.isPresent(), "chain found");
        ReceiverChain chain = chainOpt.get();
        assertEquals(dr5, chain.details, "first avaiable");
        assertEquals(dr6, chain.next.get().details, "second availabe");
        assertEquals(Optional.empty(), chain.next.get().next, "end of list");
    }
}
