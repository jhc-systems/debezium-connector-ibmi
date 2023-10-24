package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;
import java.time.Instant;
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

	PositionRange findRange(AS400 as400, boolean isFiltering, JournalProcessedPosition startPosition) throws Exception {
		final BigInteger start = startPosition.getOffset();
		final boolean fromBeginning = !startPosition.isOffsetSet() || start.equals(BigInteger.ZERO);

		final DetailedJournalReceiver endPosition = journalInfoRetrieval.getCurrentDetailedJournalReceiver(as400, journalInfo);
		log.info("end {}", endPosition);

		if (fromBeginning) {
			return new PositionRange(fromBeginning, startPosition,
					new JournalPosition(endPosition.end(), endPosition.info().receiver()));
		}

		if (cachedEndPosition == null) {
			cachedEndPosition = endPosition;
		}

		if (cachedReceivers == null) {
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
		}
		if (cachedEndPosition.isSameReceiver(endPosition)) {
			// refresh end position in cached list
			updateEndPosition(cachedReceivers, endPosition);
			// we're currently on the same journal just check the relative offset is within range
			// don't update the cache as we are not going to know the real end offset for this journal receiver until we move on to the next
			if (startPosition.isSameReceiver(endPosition)) {
				return maxOffsetInSameReceiver(startPosition, endPosition, maxServerSideEntriesBI);
			}
		} else {
			// last call to current position won't include the correct end offset so we need to refresh the list
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
			cachedEndPosition = endPosition;
		}
		//		log.info("recievers {}", cachedReceivers);

		Optional<PositionRange> endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers,
				cachedEndPosition);
		if (endOpt.isEmpty()) {
			log.warn("retrying to find end offset");
			cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
			endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers, endPosition);
		}

		log.info("journals {}", cachedReceivers);

		return endOpt.orElseGet(
				() -> new PositionRange(fromBeginning, startPosition,
						new JournalPosition(endPosition.end(), endPosition.info().receiver())));
	}


	static void updateEndPosition(List<DetailedJournalReceiver> list, DetailedJournalReceiver endPosition) {
		// should be last entry
		for (int i = list.size()-1; i >= 0 ; i--) {
			final DetailedJournalReceiver d = list.get(i);
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
		final BigInteger diff = endJournalPosition.end().subtract(startPosition.getOffset());
		if (diff.compareTo(maxServerSideEntriesBI) > 0) {
			final BigInteger restricted = startPosition.getOffset().add(maxServerSideEntriesBI);
			return new PositionRange(false, startPosition,
					new JournalPosition(restricted, startPosition.getReceiver()));
		}
		return new PositionRange(false, startPosition,
				new JournalPosition(endJournalPosition.end(), startPosition.getReceiver()));
	}

	/**
	 * should handle reset offset numbers between subsequent entries in the list
	 * @param start
	 * @param maxEntries
	 * @param receivers
	 * @return try and find end position at most offsetFromStart from start using the receiver list
	 */
	Optional<PositionRange> findPosition(JournalProcessedPosition start, BigInteger maxEntries,
			List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {
		BigInteger remaining = maxEntries;
		boolean found = false;
		DetailedJournalReceiver receiver = null;
		DetailedJournalReceiver last = null;

		if (!containsEndPosition(receivers, endPosition)) {
			log.warn("unable to find active journal {} in receiver list", endPosition);
			return Optional.empty();
		}
		// adding one to the range and then adding as we include both ends
		// but we must not use the add one when setting the end point
		// i.e. 1-> 10 is a total of 10 entries but the range can only go to 10
		for (int i=0; i < receivers.size(); i++) {
			receiver = receivers.get(i);
			if (found) {
				// if the journal has wrapped use just go to the end
				if (last != null && receiver.start().compareTo(last.end()) < 0) {
					// if start == end then API throws error
					if (start.getOffset().equals(last.end())) {
						if (start.processed()) {
							start = new JournalProcessedPosition(
									new JournalPosition(receiver.start(), receiver.info().receiver()), Instant.EPOCH,
									false);
						} else {
							return rangeWhenResetAtEnd(start, receiver, last);
						}
					} else {
						return Optional.of(new PositionRange(false, start,
								new JournalPosition(last.end(), last.info().receiver())));
					}
				}

				final BigInteger difference = receiver.end().subtract(receiver.start());
				final BigInteger entriesInJournal = difference.add(BigInteger.ONE); // add one as range is inclusive
				if (remaining.compareTo(difference) <= 0) { // range is inclusive but don't go past end when adding
					// remaining
					final BigInteger endOffset = receiver.start().add(remaining);
					return Optional.of(new PositionRange(false, start,
							new JournalPosition(endOffset, receiver.info().receiver())));
				}
				remaining = remaining.subtract(entriesInJournal);
			}
			if (receiver.isSameReceiver(start)) {
				found = true;
				final BigInteger difference = receiver.end().subtract(start.getOffset());
				final BigInteger entriesInJournal = difference.add(BigInteger.ONE); // add one as range is inclusive
				if (remaining.compareTo(difference) <= 0) { // range is inclusive but don't go past end when adding
					// remaining
					final BigInteger offset = start.getOffset().add(remaining);
					return Optional.of(
							new PositionRange(false, start, new JournalPosition(offset, receiver.info().receiver())));
				}
				remaining = remaining.subtract(entriesInJournal);
			}
			last = receiver;
		}
		if (found && receiver != null) {
			return Optional.of(
					new PositionRange(false, start, new JournalPosition(receiver.end(), receiver.info().receiver())));
		} else {
			log.warn("Current position {} not found in available receivers {}", start, receivers);
			return Optional.empty();
		}
	}

	private Optional<PositionRange> rangeWhenResetAtEnd(JournalProcessedPosition start,
			DetailedJournalReceiver receiver, DetailedJournalReceiver last) {
		if (last.start().equals(last.end())) {
			// only one entry in this receiver use this receiver and next both with offset 1
			return Optional.of(new PositionRange(false, start,
					new JournalPosition(receiver.start(), receiver.info().receiver())));
		} else {
			// move start to previous one and set as processed
			final JournalProcessedPosition processedStart = new JournalProcessedPosition(
					new JournalPosition(receiver.start().subtract(BigInteger.ONE),
							receiver.info().receiver()),
					Instant.EPOCH, true);
			return Optional.of(
					new PositionRange(false, processedStart,
							new JournalPosition(last.end(), last.info().receiver())));
		}
	}

	boolean containsEndPosition(List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {
		boolean containsEndPosition = false;
		for (int i = receivers.size() - 1; i >= 0 ; i--) {
			if (receivers.get(i).info().receiver().equals(endPosition.info().receiver())) {
				containsEndPosition = true;
			}
		}
		return containsEndPosition;
	}
}
