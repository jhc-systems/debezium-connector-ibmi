package com.fnz.db2.journal.retrieve.rnrn0200;



import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DetailedJournalReceiverTest {

	@BeforeEach
	public void setUp() throws Exception {
	}

	@Test
	public void testFirstInLatestChainSingleChainJoined() {
	DetailedJournalReceiver first = new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "2", "", 1, 1);
		List<DetailedJournalReceiver> l = List.of( new DetailedJournalReceiver[] {
		new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(), JournalStatus.Attached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1),
		first,
		new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1),
		});
		
		Optional<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.firstInLatestChain(l);
		assertTrue(firstChain.isPresent());
		assertEquals(first, firstChain.get());
	}

	@Test
	public void testFirstInLatestChainSingleChainDisjoint() {
		DetailedJournalReceiver firstAfterMissingNextReceiver = new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(123000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1);
		List<DetailedJournalReceiver> l = List.of( new DetailedJournalReceiver[] {				
		new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(100000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1), // no next receiver 2
		new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(124000l), JournalStatus.Attached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1),
		firstAfterMissingNextReceiver
		});
		
		Optional<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.firstInLatestChain(l);
		assertTrue(firstChain.isPresent());
		assertEquals(firstAfterMissingNextReceiver, firstChain.get());
	}
	
	@Test
	public void testFirstInLatestChainMultipleChainDisjoint() {
		DetailedJournalReceiver firstInChain2 = new DetailedJournalReceiver(new JournalReceiverInfo("4", "lib", new Date(125000l), JournalStatus.OnlineSavedDetached, Optional.of(2)), BigInteger.ONE, BigInteger.TWO, "5", "", 1, 1);
		List<DetailedJournalReceiver> l = List.of( new DetailedJournalReceiver[] {				
		new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(100000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1), // no next receiver 2
		new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(124000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "4", "", 1, 1),
		new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(123000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1),
		firstInChain2,
		new DetailedJournalReceiver(new JournalReceiverInfo("5", "lib", new Date(126000l), JournalStatus.Attached, Optional.of(2)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1),
		});
		
		Optional<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.firstInLatestChain(l);
		assertTrue(firstChain.isPresent());
		assertEquals(firstInChain2, firstChain.get());
	}
	
	@Test
	public void testLatestChainMultipleChainDisjoint() {
		DetailedJournalReceiver latest = new DetailedJournalReceiver(new JournalReceiverInfo("5", "lib", new Date(126000l), JournalStatus.Attached, Optional.of(2)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1);
		List<DetailedJournalReceiver> l = List.of( new DetailedJournalReceiver[] {				
		new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(100000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1), // no next receiver 2
		latest,
		new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(124000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "4", "", 1, 1),
		new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(123000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1),
		new DetailedJournalReceiver(new JournalReceiverInfo("4", "lib", new Date(125000l), JournalStatus.OnlineSavedDetached, Optional.of(2)), BigInteger.ONE, BigInteger.TWO, "5", "", 1, 1),
		});
		
		Optional<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.latest(l);
		assertTrue(firstChain.isPresent());
		assertEquals(latest, firstChain.get());
	}
	
	@Test
	public void testLatestFiltered() {
		DetailedJournalReceiver latest = new DetailedJournalReceiver(new JournalReceiverInfo("4", "lib", new Date(125000l), JournalStatus.Attached, Optional.of(2)), BigInteger.ONE, BigInteger.TWO, "5", "", 1, 1);
		List<DetailedJournalReceiver> l = List.of( new DetailedJournalReceiver[] {				
		new DetailedJournalReceiver(new JournalReceiverInfo("1", "lib", new Date(100000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ZERO, BigInteger.ONE, "", "", 1, 1), // no next receiver 2
		latest,
		new DetailedJournalReceiver(new JournalReceiverInfo("3", "lib", new Date(124000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "4", "", 1, 1),
		new DetailedJournalReceiver(new JournalReceiverInfo("2", "lib", new Date(123000l), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO, "3", "", 1, 1),
		new DetailedJournalReceiver(new JournalReceiverInfo("5", "lib", new Date(126000l), JournalStatus.Empty, Optional.of(2)), BigInteger.ONE, BigInteger.TWO, "", "", 1, 1), // status empty - ignored
		});
		
		Optional<DetailedJournalReceiver> firstChain = DetailedJournalReceiver.latest(l);
		assertTrue(firstChain.isPresent());
		assertEquals(latest, firstChain.get());
	}

}
