package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.ibm.as400.access.AS400;

public class JournalReceivers {
	static final Logger log = LoggerFactory.getLogger(JournalReceivers.class);
	
	private final JournalInfoRetrieval journalInfoRetrieval;
	private final JournalInfo journalInfo;
	private final BigInteger maxServerSideEntriesBI;

	
	JournalReceivers(JournalInfoRetrieval journalInfoRetrieval, int maxServerSideEntries, JournalInfo journalInfo) {
		this.journalInfoRetrieval = journalInfoRetrieval;
		maxServerSideEntriesBI = BigInteger.valueOf(maxServerSideEntries);
		this.journalInfo=  journalInfo;
	}
	
	List<DetailedJournalReceiver> cachedReceivers = null;

	Optional<PositionRange> findRange(AS400 as400, JournalPosition startPosition) throws Exception {
		BigInteger start = startPosition.getOffset();
		final boolean startValid = startPosition.isOffsetSet() && !start.equals(BigInteger.ZERO);
		
		if (!startValid) {
			return Optional.empty();
		}
		
		if (cachedReceivers == null) {
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
		}

		DetailedJournalReceiver endPosition = journalInfoRetrieval.getCurrentDetailedJournalReceiver(as400, journalInfo);
		if (startPosition.isSameReceiver(endPosition)) {
			// we're currently on the same journal just check the relative offset is within range
			// don't update the cache as we are not going to know the real end offset for this journal receiver until we move on to the next
			return  Optional.of(maxOffsetInSameReceiver(startPosition, endPosition, maxServerSideEntriesBI));
		} else {
			// last call to current position won't include the correct end offset so we need to refresh the list
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
		}

		Optional<JournalPosition> endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers);
		return endOpt.map(end -> new PositionRange(startPosition, end));
	}

	/**
	 * only valid when startPosition and endJournalPosition are the same receiver and library
	 * @param startPosition
	 * @param endJournalPosition
	 * @param maxServerSideEntriesBI
	 * @return
	 * @throws Exception 
	 */
	PositionRange maxOffsetInSameReceiver(JournalPosition startPosition, DetailedJournalReceiver endJournalPosition, BigInteger maxServerSideEntriesBI) throws Exception {
		if (!startPosition.isSameReceiver(endJournalPosition)) {
			throw new Exception(String.format("Error this method is only valid for same receiver start %s, end %s", startPosition, endJournalPosition));
		}
		BigInteger diff = endJournalPosition.end().subtract(startPosition.getOffset());
		if (diff.compareTo(maxServerSideEntriesBI) > 0) {
			BigInteger restricted = startPosition.getOffset().add(maxServerSideEntriesBI);
			return new PositionRange(startPosition, new JournalPosition(restricted, startPosition.getReciever(), startPosition.getReceiverLibrary(), true));
		}
		return new PositionRange(startPosition, new JournalPosition(endJournalPosition.end(), startPosition.getReciever(), startPosition.getReceiverLibrary(), true));
	}

	/**
	 * should handle reset offset numbers between subsequent entries in the list
	 * @param start
	 * @param offsetFromStart
	 * @param receivers
	 * @return try and find end position at most offsetFromStart from start using the receiver list 
	 */
	Optional<JournalPosition> findPosition(JournalPosition start, BigInteger offsetFromStart, List<DetailedJournalReceiver> receivers) {
		BigInteger remaining = offsetFromStart;
		boolean found = false;
		DetailedJournalReceiver last = null;
		for (int i=0; i < receivers.size(); i++) {
			last = receivers.get(i);
			if (found) {
				BigInteger toEnd = last.end().subtract(last.start()).add(BigInteger.ONE); // add one include end offset 1 -> 100 we get back 1 and 100
				if (remaining.compareTo(toEnd) <= 0) {
					BigInteger endOffset = last.start().add(remaining);
					return Optional.of(new JournalPosition(endOffset, last.info().name(), last.info().library(), true));
				}
				remaining = remaining.subtract(toEnd);
			}
			if (last.isSameReceiver(start)) {
				found = true;
				BigInteger toEnd = last.end().subtract(start.getOffset()).add(BigInteger.ONE); // add one include end offset 1 -> 100 we get back 1 and 100
				if (remaining.compareTo(toEnd) <= 0) {
					BigInteger offset = start.getOffset().add(remaining);
					return Optional.of(new JournalPosition(offset, last.info().name(), last.info().library(), true));
				}
				remaining = remaining.subtract(toEnd);
			}			
		}
		if (found && last != null) {
			return Optional.of(new JournalPosition(last.end(), last.info().name(), last.info().library(), true));
		} else {
			log.warn("position {} not found in active receivers {}", start, receivers);
		}
		return Optional.empty();
	}
}
