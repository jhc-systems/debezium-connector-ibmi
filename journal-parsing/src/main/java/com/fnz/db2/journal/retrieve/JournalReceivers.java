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
	private DetailedJournalReceiver cachedEndPosition;
	private List<DetailedJournalReceiver> cachedReceivers = null;

	JournalReceivers(JournalInfoRetrieval journalInfoRetrieval, int maxServerSideEntries, JournalInfo journalInfo) {
		this.journalInfoRetrieval = journalInfoRetrieval;
		maxServerSideEntriesBI = BigInteger.valueOf(maxServerSideEntries);
		this.journalInfo=  journalInfo;
	}
	
	PositionRange findRange(AS400 as400, JournalProcessedPosition startPosition) throws Exception {
		BigInteger start = startPosition.getOffset();
		final boolean fromBeginning = !startPosition.isOffsetSet() || start.equals(BigInteger.ZERO);
		
		DetailedJournalReceiver endPosition = journalInfoRetrieval.getCurrentDetailedJournalReceiver(as400, journalInfo);
		if (fromBeginning) {
			return new PositionRange(fromBeginning, startPosition, new JournalPosition(endPosition.end(), endPosition.info().receiver()));
		}
		
		
		if (cachedEndPosition == null) {
			cachedEndPosition = endPosition; 
		}

		if (cachedReceivers == null) {
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
		}
		if (cachedEndPosition.isSameReceiver(endPosition)) {
			// we're currently on the same journal just check the relative offset is within range
			// don't update the cache as we are not going to know the real end offset for this journal receiver until we move on to the next
			if (startPosition.isSameReceiver(endPosition)) {
				return  maxOffsetInSameReceiver(startPosition, endPosition, maxServerSideEntriesBI);
			} else {
				// refresh end position in cached list
				updateEndPosition(cachedReceivers, endPosition);
			}
		} else {
			// last call to current position won't include the correct end offset so we need to refresh the list
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
			cachedEndPosition = endPosition;
		}

		Optional<JournalPosition> endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers, cachedEndPosition);
		if (endOpt.isEmpty()) {
			log.warn("retrying to find end offset");
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
			endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers, endPosition);
		}
		return endOpt.map(end -> new PositionRange(fromBeginning, startPosition, end)).orElseGet(
				() -> new PositionRange(fromBeginning, startPosition, new JournalPosition(endPosition.end(), endPosition.info().receiver())));
	}

	
	static void updateEndPosition(List<DetailedJournalReceiver> list, DetailedJournalReceiver endPosition) {
		// should be last entry
		for (int i = list.size()-1; i >= 0 ; i--) {
			DetailedJournalReceiver d = list.get(i);
			if (d.isSameReceiver(endPosition)) {
				list.set(i, endPosition);
				return;
			}
		}
		list.add(endPosition);
	}
	
	/**
	 * only valid when startPosition and endJournalPosition are the same receiver and library
	 * @param startPosition
	 * @param endJournalPosition
	 * @param maxServerSideEntriesBI
	 * @return
	 * @throws Exception 
	 */
	PositionRange maxOffsetInSameReceiver(JournalProcessedPosition startPosition, DetailedJournalReceiver endJournalPosition, BigInteger maxServerSideEntriesBI) throws Exception {
		if (!startPosition.isSameReceiver(endJournalPosition)) {
			throw new Exception(String.format("Error this method is only valid for same receiver start %s, end %s", startPosition, endJournalPosition));
		}
		BigInteger diff = endJournalPosition.end().subtract(startPosition.getOffset());
		if (diff.compareTo(maxServerSideEntriesBI) > 0) {
			BigInteger restricted = startPosition.getOffset().add(maxServerSideEntriesBI);
			return new PositionRange(false, startPosition, 
					new JournalPosition(restricted, startPosition.getReceiver()));
		}
		return new PositionRange(false, startPosition, 
				new JournalPosition(endJournalPosition.end(), startPosition.getReceiver()));
	}

	/**
	 * should handle reset offset numbers between subsequent entries in the list
	 * @param start
	 * @param offsetFromStart
	 * @param receivers
	 * @return try and find end position at most offsetFromStart from start using the receiver list 
	 */
	Optional<JournalPosition> findPosition(JournalProcessedPosition start, BigInteger offsetFromStart, List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {
		BigInteger remaining = offsetFromStart;
		boolean found = false;
		DetailedJournalReceiver last = null;
		
		if (!containsEndPosition(receivers, endPosition)) {
			log.warn("unable to find active journal {} in receiver list", endPosition);
			return Optional.empty();
		}
		
		for (int i=0; i < receivers.size(); i++) {
			last = receivers.get(i);
			if (found) {
				BigInteger toEnd = last.end().subtract(last.start()).add(BigInteger.ONE); // add one include end offset 1 -> 100 we get back 1 and 100
				if (remaining.compareTo(toEnd) <= 0) {
					BigInteger endOffset = last.start().add(remaining);
					return Optional.of(new JournalPosition(endOffset, last.info().receiver()));
				}
				remaining = remaining.subtract(toEnd);
			}
			if (last.isSameReceiver(start)) {
				found = true;
				BigInteger toEnd = last.end().subtract(start.getOffset()).add(BigInteger.ONE); // add one include end offset 1 -> 100 we get back 1 and 100
				if (remaining.compareTo(toEnd) <= 0) {
					BigInteger offset = start.getOffset().add(remaining);
					return Optional.of(new JournalPosition(offset, last.info().receiver()));
				}
				remaining = remaining.subtract(toEnd);
			}			
		}
		if (found && last != null) {
			return Optional.of(new JournalPosition(last.end(), last.info().receiver()));
		} else {
			log.warn("Current position {} not found in available receivers {}", start, receivers);
			return Optional.empty();
		}
	}

	private boolean containsEndPosition(List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {
		boolean containsEndPosition = false;
		for (int i=0; i < receivers.size(); i++) {
			if (receivers.get(i).info().receiver().equals(endPosition.info().receiver())) {
				containsEndPosition = true;
			}					
		}
		return containsEndPosition;
	}
}
