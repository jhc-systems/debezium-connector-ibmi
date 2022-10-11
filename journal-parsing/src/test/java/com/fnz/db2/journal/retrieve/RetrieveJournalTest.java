package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalStatus;

public class RetrieveJournalTest {

	private RetrieveJournal createTestSubject() {
		return new RetrieveJournal(new RetrieveConfig(null, new JournalInfo("receiver", "lib"), 65535, true, new JournalCode[0], new ArrayList<FileFilter>(), RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES, null));
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
			System.out.println(j);
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
}