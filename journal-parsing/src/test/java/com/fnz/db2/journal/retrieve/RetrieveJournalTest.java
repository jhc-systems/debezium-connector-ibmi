package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.fnz.db2.journal.retrieve.RetrieveJournal.PositionRange;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalStatus;
import com.ibm.as400.access.AS400;

public class RetrieveJournalTest {

	private RetrieveJournal createTestSubject() {
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
		return createTestSubject(journalInfoRetrieval);
	}
	
	private RetrieveJournal createTestSubject(JournalInfoRetrieval journalInfoRetrieval) {
		return new RetrieveJournal(new RetrieveConfig(null, 
				new JournalInfo("receiver", "lib"), 65535, true, new JournalCode[0], 
				new ArrayList<FileFilter>(), RetrieveConfig.DEFAULT_MAX_JOURNAL_TIMEOUT,
				RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES, null),
				journalInfoRetrieval);
	}

	@Test
	public void testWithinRangeBetween() throws Exception {
		RetrieveJournal testSubject = createTestSubject();
		boolean inRange = testSubject.withinRange(BigInteger.valueOf(1), BigInteger.valueOf(0), BigInteger.valueOf(2));
		assertTrue(inRange);
	}

	@Test
	public void testWithinRangeAtEnds() throws Exception {
		RetrieveJournal testSubject = createTestSubject();
		boolean beginning = testSubject.withinRange(BigInteger.valueOf(0), BigInteger.valueOf(0), BigInteger.valueOf(2));
		assertTrue(beginning);
		boolean end = testSubject.withinRange(BigInteger.valueOf(2), BigInteger.valueOf(0), BigInteger.valueOf(2));
		assertTrue(end);
	}

	@Test
	public void testWithinRangeOutside() throws Exception {
		RetrieveJournal testSubject = createTestSubject();
		boolean before = testSubject.withinRange(BigInteger.valueOf(0), BigInteger.valueOf(1), BigInteger.valueOf(2));
		assertFalse(before);
		boolean after = testSubject.withinRange(BigInteger.valueOf(3), BigInteger.valueOf(0), BigInteger.valueOf(2));
		assertFalse(after);
	}
	
	@Test
	public void testJournalAtMaxOffsetInMiddle() throws Exception {
		RetrieveJournal testSubject = createTestSubject();
		long size = 2*RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES/3;
		List<DetailedJournalReceiver> receivers = createReceiversFixedSequence(0, size);
		BigInteger currentOffset = BigInteger.valueOf(size*4);
		Optional<JournalPosition> found = testSubject.journalAtMaxOffset(currentOffset.add(BigInteger.valueOf(testSubject.config.maxServerSideEntries())), receivers);
		BigInteger maxOffset = currentOffset.add(BigInteger.valueOf(testSubject.config.maxServerSideEntries()));
		assertEquals(found, Optional.of(new JournalPosition(maxOffset, "name5", "lib", true)));
	}

	@Test
	public void testJournalAtMaxOffsetBoundries() throws Exception {
		RetrieveJournal testSubject = createTestSubject();
		List<DetailedJournalReceiver> receivers = new ArrayList<>();
		long size = RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES;
		for (int i=0; i<10; i++) {
			BigInteger start = BigInteger.valueOf(i*size + 1);
			BigInteger end = BigInteger.valueOf((i+1)*size);
			DetailedJournalReceiver j = new DetailedJournalReceiver(new JournalReceiverInfo("name"+i, "lib", new Date(), JournalStatus.Attached, Optional.<Integer>empty()), start, end, "next", "nextDual", 100, 10000);
			receivers.add(j);
		}
		BigInteger startOffset = BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES*2);
		Optional<JournalPosition> foundStart = testSubject.journalAtMaxOffset(startOffset.add(BigInteger.valueOf(testSubject.config.maxServerSideEntries())), receivers);
		BigInteger maxStartOffset = startOffset.add(BigInteger.valueOf(testSubject.config.maxServerSideEntries()));
		assertEquals(foundStart, Optional.of(new JournalPosition(maxStartOffset, "name2", "lib", true)));

		BigInteger endOffset = BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES*4-1);
		Optional<JournalPosition> foundEnd = testSubject.journalAtMaxOffset(endOffset.add(BigInteger.valueOf(testSubject.config.maxServerSideEntries())), receivers);
		BigInteger maxEndOffset = endOffset.add(BigInteger.valueOf(testSubject.config.maxServerSideEntries()));
		assertEquals(foundEnd, Optional.of(new JournalPosition(maxEndOffset, "name4", "lib", true)));
	}
	

	private List<DetailedJournalReceiver> createReceiversFixedSequence(long offset, long size) {
		List<DetailedJournalReceiver> receivers = new ArrayList<>();
		for (int i=0; i<10; i++) {
			BigInteger start = BigInteger.valueOf(offset + i*size + 1);
			BigInteger end = BigInteger.valueOf(offset + (i+1)*size);
			DetailedJournalReceiver j = new DetailedJournalReceiver(new JournalReceiverInfo("name"+i, "lib", new Date(), JournalStatus.Attached, Optional.<Integer>empty()), start, end, "next", "nextDual", 100, 10000);
			receivers.add(j);
		}
		return receivers;
	}
	
	@Test
	public void testFindRange_EndPastCurrentJournal() throws Exception {
		BigInteger end = BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES/200);
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval() {
			@Override
			public DetailedJournalReceiver getCurrentDetailedJournalReceiver(AS400 as400, JournalInfo journalLib) throws Exception {
				return new DetailedJournalReceiver(
						new JournalReceiverInfo("latest", "lib", new Date(), JournalStatus.Attached, Optional.empty()),
						BigInteger.ZERO,
						end, // less than RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES
						"", "",
						0l, 0l);
			}
		};
		RetrieveJournal testSubject = createTestSubject(journalInfoRetrieval);
		JournalPosition start = new JournalPosition(BigInteger.valueOf(1), "rec", "lib", true);
		Optional<PositionRange> r = testSubject.findRange(null, start);
		assertEquals(r, Optional.of(new PositionRange(start, 
				new JournalPosition(end, "latest", "lib", true))));
	}
	
	@Test
	public void testFindRange_WithinCurrentJournal() throws Exception {
		BigInteger end = BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES+200);
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval() {
			@Override
			public DetailedJournalReceiver getCurrentDetailedJournalReceiver(AS400 as400, JournalInfo journalLib) throws Exception {
				return new DetailedJournalReceiver(
						new JournalReceiverInfo("latest", "lib", new Date(), JournalStatus.Attached, Optional.empty()),
						BigInteger.ZERO,
						end,
						"", "",
						0l, 0l);
			}
		};
		RetrieveJournal testSubject = createTestSubject(journalInfoRetrieval);
		JournalPosition start = new JournalPosition(BigInteger.valueOf(1), "rec", "lib", true);
		Optional<PositionRange> r = testSubject.findRange(null, start);
		assertEquals(r, Optional.of(new PositionRange(start, 
				new JournalPosition(start.getOffset().add(BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES)), "latest", "lib", true))));
	}
	
	@Test
	public void testFindRange_WithinJournalList() throws Exception {
		final List<DetailedJournalReceiver> receivers = new ArrayList<>();
		for (int i=0; i<10; i++) {
			receivers.add(new DetailedJournalReceiver(
					new JournalReceiverInfo(String.format("receiver%d",i), "lib", new Date(i), JournalStatus.Attached, Optional.empty()),
					BigInteger.valueOf(i*RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES + 1),
					BigInteger.valueOf((i+1)*RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES),
					String.format("receiver%d",i+1), "",
					0l, 0l));
		}
		
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval() {
			@Override
			public DetailedJournalReceiver getCurrentDetailedJournalReceiver(AS400 as400, JournalInfo journalLib) throws Exception {
				return receivers.get(receivers.size()-1);
			}
			
			@Override
			public List<DetailedJournalReceiver> getReceivers(AS400 as400, JournalInfo journalLib) throws Exception {
				return receivers;
			}
		};
		RetrieveJournal testSubject = createTestSubject(journalInfoRetrieval);
		JournalPosition start = new JournalPosition(BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES*5), "rec", "lib", true);
		Optional<PositionRange> r = testSubject.findRange(null, start);
		assertEquals(r, Optional.of(new PositionRange(start, 
				new JournalPosition(start.getOffset().add(BigInteger.valueOf(RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES)), "receiver5", "lib", true))));
	}	
	
	@Test
	public void testFindRange_WithinJournalListNoStart() throws Exception {
		int journalEntries=10;
		long journalStep = RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES/(journalEntries*2);
		final List<DetailedJournalReceiver> receivers = new ArrayList<>();
		for (int i=0; i<10; i++) {
			receivers.add(new DetailedJournalReceiver(
					new JournalReceiverInfo(String.format("receiver%d",i), "lib", new Date(i), JournalStatus.Attached, Optional.empty()),
					BigInteger.valueOf(i*journalStep + 1),
					BigInteger.valueOf((i+1)*journalStep),
					String.format("receiver%d",i+1), "",
					0l, 0l));
		}
		
		JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval() {
			@Override
			public DetailedJournalReceiver getCurrentDetailedJournalReceiver(AS400 as400, JournalInfo journalLib) throws Exception {
				return receivers.get(receivers.size()-1);
			}
			
			@Override
			public List<DetailedJournalReceiver> getReceivers(AS400 as400, JournalInfo journalLib) throws Exception {
				return receivers;
			}
		};
		RetrieveJournal testSubject = createTestSubject(journalInfoRetrieval);
		JournalPosition unsetOffset = new JournalPosition("", "rec", "lib", true);
		Optional<PositionRange> r = testSubject.findRange(null, unsetOffset);
		DetailedJournalReceiver first = receivers.get(0);
		DetailedJournalReceiver last = receivers.get(receivers.size()-1);
		
		assertEquals(r, Optional.of(new PositionRange(new JournalPosition(first.start(), first.info().name(), first.info().library(), false), 
				new JournalPosition(last.end(), last.info().name(), last.info().library(), true))));
	}
}