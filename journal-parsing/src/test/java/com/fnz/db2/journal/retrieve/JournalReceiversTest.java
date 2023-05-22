package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalStatus;



public class JournalReceiversTest {
	JournalReceivers receivers;
	@Mock JournalInfoRetrieval journalInfoRetrieval;
	JournalInfo journalInfo = new JournalInfo("journal", "journallib");
	
	@BeforeEach
	public void setUp() throws Exception {
	}
	
	@Test
	void testFindRangeMidFirstEntry() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(5), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(6), BigInteger.valueOf(20), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(22), "3", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(3), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(4), position.get().getOffset());
	}


	@Test
	void testFindRangeMidSecondEntryReset() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "3", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(15), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(6), position.get().getOffset());
	}
	
	@Test
	void testFindRangeMidSecondContiguous() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(2), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3), BigInteger.valueOf(20), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(22), "3", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(10), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(11), position.get().getOffset());
	}

	@Test
	void testFindRangeMidEndEntry() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(2), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3), BigInteger.valueOf(20), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(35), "3", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(30), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(31), position.get().getOffset());
	}
	
	@Test
	void testFindRangePastEnd() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(2), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3), BigInteger.valueOf(20), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(35), "3", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(100), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(35), position.get().getOffset());
	}
	
	@Test
	void testFindMidStartingMid() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(11), BigInteger.valueOf(20), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(31), "3", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j4", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(41), BigInteger.valueOf(50), "3", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.valueOf(10), "j3", "jlib", true), BigInteger.valueOf(15), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(25), position.get().getOffset());

	}
	
	@Test
	void testFindStartingPastEnd() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(new DetailedJournalReceiver[] {
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "2", "", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(11), BigInteger.valueOf(20), "2", "", 1, 1),
		});
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.valueOf(30), "j3", "jlib", true), BigInteger.valueOf(15), list);
		assertTrue(position.isEmpty());
	}

	@Test
	void testMaxOffsetInSameReceiverEnd() throws Exception {
		int maxOffset=1000;
		BigInteger maxServerSideEntriesBI = BigInteger.valueOf(maxOffset);
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, maxOffset, journalInfo);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ZERO, "j1", "jlib", true);
		DetailedJournalReceiver endJournalPosition = new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(100), "2", "", 1, 1);

		PositionRange range = jreceivers.maxOffsetInSameReceiver(startPosition, endJournalPosition, maxServerSideEntriesBI);
		assertEquals(endJournalPosition.end(), range.end().getOffset());
	}
	
	@Test
	void testMaxOffsetInSameReceiverLimited() throws Exception {
		int maxOffset=10;
		BigInteger maxServerSideEntriesBI = BigInteger.valueOf(maxOffset);
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, maxOffset, journalInfo);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ZERO, "j1", "jlib", true);
		DetailedJournalReceiver endJournalPosition = new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(100), "2", "", 1, 1);

		PositionRange range = jreceivers.maxOffsetInSameReceiver(startPosition, endJournalPosition, maxServerSideEntriesBI);
		assertEquals(startPosition.getOffset().add(maxServerSideEntriesBI), range.end().getOffset());
	}
}
