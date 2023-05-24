package com.fnz.db2.journal.retrieve.rnrn0200;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DetailedJournalReceiverTest {

	@BeforeEach
	public void setUp() throws Exception {
	}

	@Test
	public void testLatestChain() {
		List<DetailedJournalReceiver> l = new ArrayList<>(Arrays.asList(
				new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(1), JournalStatus.Partial, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1)
		));

		List<DetailedJournalReceiver> active = List.of(
			new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "2", "", 1, 1),
			new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1)
		);
			
		l.addAll(active);
		
		List<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.lastJoined(l);
		assertEquals(active, firstChain);
	}

	@Test
	public void testLatestChainSorting() {
		List<DetailedJournalReceiver> inactive = Arrays.asList( 
				new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(1), JournalStatus.Partial, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1)
		);
		List<DetailedJournalReceiver> active = List.of(
			new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "2", "", 1, 1),
			new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1)
		);
			
		List<DetailedJournalReceiver> l = new ArrayList<>(active); // added in wrong date order
		l.addAll(inactive);
		
		List<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.lastJoined(l);
		assertEquals(active, firstChain);
	}

}
