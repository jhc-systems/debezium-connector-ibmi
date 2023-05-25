package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalStatus;
import com.ibm.as400.access.AS400;


@ExtendWith(MockitoExtension.class)
public class JournalReceiversTest {
	JournalReceivers receivers;
	@Mock JournalInfoRetrieval journalInfoRetrieval;
	JournalInfo journalInfo = new JournalInfo("journal", "journallib");
	@Mock AS400 as400;
	
	@BeforeEach
	public void setUp() throws Exception {
	}
	
	@Test
	void findRangeWithinCurrentPosistion() throws Exception {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		
		DetailedJournalReceiver dp = new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(5), "", 1, 1);
		List<DetailedJournalReceiver> list = List.of(dp);

		when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);
		
		when(journalInfoRetrieval.getCurrentDetailedJournalReceiver(any(), any())).thenReturn(dp);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ONE, "j1", "jlib", true);
		Optional<PositionRange> result = jreceivers.findRange(as400, startPosition);
		assertTrue(result.isPresent());
		PositionRange rangeAnswer = new PositionRange(startPosition, new JournalPosition(dp.end(), dp.info().name(), dp.info().library(), true)); 
		assertEquals(Optional.of(rangeAnswer), result);
	}
	
	@Test
	void findRangeInList() throws Exception {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		
		DetailedJournalReceiver detailedStart = new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(10), BigInteger.valueOf(5), "j2", 1, 1);
		DetailedJournalReceiver detailedEnd = new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "", 1, 1);
		List<DetailedJournalReceiver> list = new ArrayList<>(List.of(
				detailedStart,
				detailedEnd
				));

		when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);
		
		when(journalInfoRetrieval.getCurrentDetailedJournalReceiver(any(), any())).thenReturn(detailedEnd);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ONE, "j1", "jlib", true);
		Optional<PositionRange> result = jreceivers.findRange(as400, startPosition);
		assertTrue(result.isPresent());
		PositionRange rangeAnswer = new PositionRange(startPosition, new JournalPosition(detailedEnd.end(), detailedEnd.info().name(), detailedEnd.info().library(), true)); 
		assertEquals(Optional.of(rangeAnswer), result);
	}
	
	@Test
	void findRangeReFetchList() throws Exception {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 15, journalInfo);
		
		DetailedJournalReceiver detailedEnd = new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "", 1, 1);
		List<DetailedJournalReceiver> list = new ArrayList<>(List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(10), BigInteger.valueOf(10), "j2", 1, 1),
				detailedEnd
				));

		DetailedJournalReceiver detailedEnd2 = new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "", 1, 1);
		List<DetailedJournalReceiver> list2 = new ArrayList<>(List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(10), BigInteger.valueOf(10), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "j3", 1, 1),
				detailedEnd2
				));
		when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list).thenReturn(list2);
		when(journalInfoRetrieval.getCurrentDetailedJournalReceiver(any(), any())).thenReturn(detailedEnd).thenReturn(detailedEnd2);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ONE, "j1", "jlib", true);
		Optional<PositionRange> result = jreceivers.findRange(as400, startPosition);
		assertTrue(result.isPresent());
		PositionRange rangeAnswer = new PositionRange(startPosition, new JournalPosition(BigInteger.valueOf(6), detailedEnd.info().name(), detailedEnd.info().library(), true)); 
		assertEquals(Optional.of(rangeAnswer), result);
		
		
		JournalPosition startPosition2 = new JournalPosition(BigInteger.valueOf(2), "j2", "jlib", true);
		Optional<PositionRange> result2 = jreceivers.findRange(as400, startPosition2);
		assertTrue(result2.isPresent());
		PositionRange rangeAnswer2 = new PositionRange(startPosition2, new JournalPosition(BigInteger.valueOf(7), detailedEnd2.info().name(), detailedEnd2.info().library(), true)); 
		assertEquals(Optional.of(rangeAnswer2), result2);

	}
	
	
	@Test
	void testFindRangeMidFirstEntry() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(5), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(6), BigInteger.valueOf(20), "j3", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(22), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(3), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(4), position.get().getOffset());
	}


	@Test
	void testFindRangeMidSecondEntryReset() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "j3", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(15), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(6), position.get().getOffset());
	}
	
	@Test
	void testFindRangeMidSecondContiguous() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(2), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3), BigInteger.valueOf(20), "j3", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(22), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(10), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(11), position.get().getOffset());
	}

	@Test
	void testFindRangeMidEndEntry() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(2), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3), BigInteger.valueOf(20), "j3", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(35), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(30), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(31), position.get().getOffset());
	}
	
	@Test
	void testFindRangePastEnd() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(2), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3), BigInteger.valueOf(20), "j3", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(35), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.ONE, "j1", "jlib", true), BigInteger.valueOf(100), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(35), position.get().getOffset());
	}
	
	@Test
	void testFindMidStartingMid() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(11), BigInteger.valueOf(20), "j3", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j3", "jlib", new Date(3), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(21), BigInteger.valueOf(31), "j4", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j4", "jlib", new Date(4), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(41), BigInteger.valueOf(50), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.valueOf(10), "j3", "jlib", true), BigInteger.valueOf(15), list);
		assertTrue(position.isPresent());
		assertEquals(BigInteger.valueOf(25), position.get().getOffset());

	}
	
	@Test
	void testFindStartingPastEnd() {
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, 100, journalInfo);
		List<DetailedJournalReceiver> list = List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(11), BigInteger.valueOf(20), "", 1, 1)
		);
		Optional<JournalPosition> position = jreceivers.findPosition(new JournalPosition(BigInteger.valueOf(30), "j3", "jlib", true), BigInteger.valueOf(15), list);
		assertTrue(position.isEmpty());
	}

	@Test
	void testMaxOffsetInSameReceiverEnd() throws Exception {
		int maxOffset=1000;
		BigInteger maxServerSideEntriesBI = BigInteger.valueOf(maxOffset);
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, maxOffset, journalInfo);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ONE, "j1", "jlib", true);
		DetailedJournalReceiver endJournalPosition = new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(100), "", 1, 1);

		PositionRange range = jreceivers.maxOffsetInSameReceiver(startPosition, endJournalPosition, maxServerSideEntriesBI);
		assertEquals(endJournalPosition.end(), range.end().getOffset());
	}
	
	@Test
	void testMaxOffsetInSameReceiverLimited() throws Exception {
		int maxOffset=10;
		BigInteger maxServerSideEntriesBI = BigInteger.valueOf(maxOffset);
		JournalReceivers jreceivers = new JournalReceivers(journalInfoRetrieval, maxOffset, journalInfo);
		
		JournalPosition startPosition = new JournalPosition(BigInteger.ONE, "j1", "jlib", true);
		DetailedJournalReceiver endJournalPosition = new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(100), "", 1, 1);

		PositionRange range = jreceivers.maxOffsetInSameReceiver(startPosition, endJournalPosition, maxServerSideEntriesBI);
		assertEquals(startPosition.getOffset().add(maxServerSideEntriesBI), range.end().getOffset());
	}
	
	@Test
	void testUpdateEndPosition() {
		List<DetailedJournalReceiver> list = new ArrayList<>(List.of(
				new DetailedJournalReceiver(new JournalReceiverInfo("j1", "jlib", new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(1), BigInteger.valueOf(10), "j2", 1, 1),
				new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(11), BigInteger.valueOf(20), "", 1, 1)
		));
		DetailedJournalReceiver endPosition = new DetailedJournalReceiver(new JournalReceiverInfo("j2", "jlib", new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(11), BigInteger.valueOf(200), "", 1, 1);
		JournalReceivers.updateEndPosition(list, endPosition);
		
		assertEquals(endPosition, list.get(1));
	}

}
