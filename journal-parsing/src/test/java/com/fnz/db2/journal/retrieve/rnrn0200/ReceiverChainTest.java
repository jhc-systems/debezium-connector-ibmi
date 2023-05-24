package com.fnz.db2.journal.retrieve.rnrn0200;

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

class ReceiverChainTest {

	@BeforeEach
	protected void setUp() throws Exception {
	}

	@Test
	void testAvailableSingleChainElement() {		
		DetailedJournalReceiver available = new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1); 
		List<DetailedJournalReceiver> l = Arrays.asList(		
			new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(1), JournalStatus.Partial, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "2", "", 1, 1),
			available
		);
		Map<String, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
		Map<String, ReceiverChain> expected = new HashMap<>();
		expected.put(available.info().name(), new ReceiverChain(available));
		assertEquals(expected, map);
	}

	@Test
	void testBuildReceiverChains() {
		DetailedJournalReceiver firstAvailable = new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "3", "", 1, 1);
		DetailedJournalReceiver secondAvailable = new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1);
		List<DetailedJournalReceiver> l = Arrays.asList(		
			new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(1), JournalStatus.Partial, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "2", "", 1, 1),
			firstAvailable,
			secondAvailable
		);
		Map<String, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
		assertEquals(2, map.size());
		
		Set<ReceiverChain> detachedSet = ReceiverChain.buildReceiverChains(map);		
		assertEquals(1, detachedSet.size());
		
		ReceiverChain chain = detachedSet.iterator().next();
		assertEquals(firstAvailable, chain.details, "first avaiable");
		assertEquals(secondAvailable, chain.next.details, "second availabe");
		assertEquals(null, chain.next.next, "end of list");
	}
	
	@Test
	void testFindCurrentChain() {
		DetailedJournalReceiver firstAvailable = new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "3", "", 1, 1);
		DetailedJournalReceiver secondAvailable = new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1);
		List<DetailedJournalReceiver> l = Arrays.asList(		
			new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(1), JournalStatus.Partial, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "2", "", 1, 1),
			firstAvailable,
			secondAvailable
		);
		Map<String, ReceiverChain> map = ReceiverChain.availableSingleChainElement(l);
		assertEquals(2, map.size());
		
		Set<ReceiverChain> detachedSet = ReceiverChain.buildReceiverChains(map);		
		assertEquals(1, detachedSet.size());
		
		Optional<ReceiverChain> chainOpt = ReceiverChain.findChain(map, secondAvailable);

		assertTrue(chainOpt.isPresent(), "chain found");
		ReceiverChain chain = chainOpt.get();
		assertEquals(firstAvailable, chain.details, "first avaiable");
		assertEquals(secondAvailable, chain.next.details, "second availabe");
		assertEquals(null, chain.next.next, "end of list");
	}
}
