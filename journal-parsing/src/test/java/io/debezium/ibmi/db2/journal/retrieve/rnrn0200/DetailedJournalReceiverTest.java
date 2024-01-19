/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class DetailedJournalReceiverTest {
    DetailedJournalReceiver dr3 = new DetailedJournalReceiver(
            new JournalReceiverInfo(new JournalReceiver("3", "lib"), new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO,
            Optional.empty(), 1, 1);
    DetailedJournalReceiver dr2 = new DetailedJournalReceiver(
            new JournalReceiverInfo(new JournalReceiver("2", "lib"), new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE,
            Optional.of(dr3.info().receiver()), 1, 1);
    DetailedJournalReceiver dr1 = new DetailedJournalReceiver(
            new JournalReceiverInfo(new JournalReceiver("1", "lib"), new Date(1), JournalStatus.Partial, Optional.of(1)), BigInteger.ONE, BigInteger.TWO,
            Optional.of(dr2.info().receiver()), 1, 1);

    @BeforeEach
    void setUp() throws Exception {
    }

    @Test
    void testLatestChain() {
        List<DetailedJournalReceiver> l = new ArrayList<>(Arrays.asList(
                dr1));

        List<DetailedJournalReceiver> active = List.of(
                dr2,
                dr3);

        l.addAll(active);

        List<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.lastJoined(l);
        assertEquals(active, firstChain);
    }

    @Test
    void testLatestChainSorting() {
        List<DetailedJournalReceiver> inactive = Arrays.asList(
                dr1);
        List<DetailedJournalReceiver> active = List.of(
                dr2,
                dr3);

        List<DetailedJournalReceiver> l = new ArrayList<>(active); // added in wrong date order
        l.addAll(inactive);

        List<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.lastJoined(l);
        assertEquals(active, firstChain);
    }

    @Test
    void testWithStatus() {
        DetailedJournalReceiver dr = dr1;
        DetailedJournalReceiver expected = new DetailedJournalReceiver(
                new JournalReceiverInfo(dr1.info().receiver(), dr1.info().attachTime(), JournalStatus.Attached, dr1.info().chain()), // mark as partial - not available
                dr1.start(), dr1.end(), dr1.nextReceiver(), dr1.maxEntryLength(), dr1.numberOfEntries());
        assertEquals(expected, dr.withStatus(JournalStatus.Attached), "only the status should change");
    }

}
