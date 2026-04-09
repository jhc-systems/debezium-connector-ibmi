/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidPositionException;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

public class ReceiverPagination {
    static final Logger log = LoggerFactory.getLogger(ReceiverPagination.class);

    private final JournalInfoRetrieval journalInfoRetrieval;
    private final JournalInfo journalInfo;
    private final BigInteger maxServerSideEntriesBI;
    private DetailedJournalReceiver cachedEndPosition;
    private List<DetailedJournalReceiver> cachedReceivers = null;
    private int receiverListRetries = 0;

    ReceiverPagination(JournalInfoRetrieval journalInfoRetrieval, int maxServerSideEntries, JournalInfo journalInfo) {
        this.journalInfoRetrieval = journalInfoRetrieval;
        maxServerSideEntriesBI = BigInteger.valueOf(maxServerSideEntries);
        this.journalInfo = journalInfo;
    }

    Optional<PositionRange> findRange(AS400 as400, JournalProcessedPosition startPosition) throws Exception {
        final Optional<DetailedJournalReceiver> endPositionOpt = journalInfoRetrieval.getDelayedDetailedJournalReceiver(as400, journalInfo);

        return endPositionOpt.flatMap(endPosition -> {
            try {
                return _findRange(as400, startPosition, endPosition);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    Optional<PositionRange> _findRange(AS400 as400, JournalProcessedPosition startPosition, DetailedJournalReceiver endPosition) throws Exception {
        final BigInteger start = startPosition.getOffset();
        final boolean fromBeginning = !startPosition.isOffsetSet() || start.equals(BigInteger.ZERO);

        if (cachedEndPosition == null) {
            cachedEndPosition = endPosition;
        }

        if (cachedReceivers == null) {
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
        }

        if (fromBeginning) {
            DetailedJournalReceiver first = cachedReceivers.get(0);
            startPosition = new JournalProcessedPosition(first.start(), first.info().receiver(), Instant.EPOCH, false);
        }

        if (cachedEndPosition.isSameReceiver(endPosition)) {
            // we're currently on the same journal just check the relative offset is within range
            if (startPosition.isSameReceiver(endPosition)) {
                return Optional.of(paginateInSameReceiver(startPosition, endPosition, maxServerSideEntriesBI));
            }
        }
        else {
            // last call to current position won't include the correct end offset so we need to refresh the list
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);

            if (!isValid(startPosition, endPosition, cachedReceivers)) {
            	cachedReceivers = null;
            	receiverListRetries++;
            	if (receiverListRetries < 100) {
            		log.debug("assuming our receiver list is stale");
            		return Optional.<PositionRange>empty();
            	} else {
            		String receivers = cachedReceivers.stream().map(DetailedJournalReceiver::toString).collect(Collectors.joining(","));
            		throw new InvalidPositionException(String.format("invalid receiver list after %d retries position was %s receivers were %s", receiverListRetries, startPosition, receivers));
            	}
            } else {
            	receiverListRetries = 0;
            }

            cachedEndPosition = endPosition;
        }

        Optional<PositionRange> endOpt = findPosition(startPosition, endPosition, maxServerSideEntriesBI, cachedReceivers);
        if (endOpt.isEmpty()) {
    		String receivers = cachedReceivers.stream().map(DetailedJournalReceiver::toString).collect(Collectors.joining(","));
    		throw new InvalidPositionException(String.format("invalid receiver list after %d retries position was %s receivers were %s", receiverListRetries, startPosition, receivers));
        }

        log.debug("end {} journals {}", endPosition, cachedReceivers);
        return endOpt;
    }

    static boolean isValid(JournalProcessedPosition startPosition, DetailedJournalReceiver endPositionOpt, List<DetailedJournalReceiver> receivers) {
        for (int i = receivers.size() - 1; i >= 0; i--) {
            final DetailedJournalReceiver r = receivers.get(i);
			if (r.isSameReceiver(endPositionOpt)) {
				if (!r.isAttached()) {
					log.warn("the current reciver {} is not attached {}, it is likely we have state data", r, endPositionOpt);
					return false;
				}
			}
			if (r.isSameReceiver(startPosition)) {
				if (r.isAttached()) {
					log.warn("we have a new receiver {} but the list shows the one we were processing before {} is still attached, it is likely we have state data", r, startPosition);
					return false;
				}
				if (r.end().compareTo(startPosition.getOffset()) < 0) {
					log.warn("we have a new receiver but the end offset in the receiver list {} is less than the current position {}", r, startPosition);
					return false;
				}
			}
		}
		return false;
	}

    /**
     * only valid when startPosition and endJournalPosition are the same receiver and library
     * @param startPosition
     * @param endJournalPosition
     * @param maxServerSideEntriesBI
     * @return
     * @throws Exception
     */
    PositionRange paginateInSameReceiver(JournalProcessedPosition startPosition, DetailedJournalReceiver endJournalPosition, BigInteger maxServerSideEntriesBI)
            throws Exception {
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
     * @param startPosition
     * @param maxEntries
     * @param receivers
     * @return try and find end position at most offsetFromStart from start using the receiver list
     */
    Optional<PositionRange> findPosition(JournalProcessedPosition startPosition, DetailedJournalReceiver endPosition, BigInteger maxEntries,
                                         List<DetailedJournalReceiver> receivers)
            throws Exception {

        for (Iterator<DetailedJournalReceiver> it = receivers.iterator(); it.hasNext();) {
            DetailedJournalReceiver nextReceiver = it.next();
            if (nextReceiver.isSameReceiver(startPosition)) {
                if (startEqualsEndAndProcessed(startPosition, nextReceiver)) { // finished processing this receiver
                    if (it.hasNext()) { // paginate within next receiver if it exists
                        nextReceiver = it.next();
                        startPosition.setPosition(new JournalPosition(nextReceiver.start(), nextReceiver.info().receiver()), false);
                        if (nextReceiver.isSameReceiver(endPosition)) { // delayed end offset is the next receiver to process
                        	return Optional.of(
                                    paginateInSameReceiver(startPosition, endPosition, maxEntries));
                        }
                        // just paginate within the receiver in the list
                        return Optional.of(
                                paginateInSameReceiver(startPosition, nextReceiver, maxEntries));
                    }
                    else {
                        // we're at the end and we've processed everything, there are no more receivers
                        return Optional.of(
                                paginateInSameReceiver(startPosition, nextReceiver, maxEntries));
                    }
                }
                // we haven't finished this receiver yet
                return Optional.of(
                        paginateInSameReceiver(startPosition, nextReceiver, maxEntries));
            }
        }

        log.error("Current position {} not found in available receivers {}", startPosition, receivers);
        return Optional.empty();
    }

    private boolean startEqualsEndAndProcessed(JournalProcessedPosition start, DetailedJournalReceiver last) {
        return start.processed() && start.getOffset().equals(last.end());
    }

    boolean containsEndPosition(List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {
        boolean containsEndPosition = false;
        for (int i = receivers.size() - 1; i >= 0; i--) {
            if (receivers.get(i).info().receiver().equals(endPosition.info().receiver())) {
                containsEndPosition = true;
            }
        }
        return containsEndPosition;
    }

    static class RangeFinder {
        private boolean found = false;
        private DetailedJournalReceiver lastReceiver = null;
        private BigInteger remaining;
        private final JournalProcessedPosition startPosition;

        RangeFinder(JournalProcessedPosition startPosition, BigInteger maxEntries) {
            this.remaining = maxEntries;
            this.startPosition = startPosition;
        }

        public Optional<PositionRange> next(DetailedJournalReceiver nextReceiver) {
            if (found) {
                // if the next journal has wrapped use just go to the end of the previous one
                if (lastReceiver != null && nextReceiver.start().compareTo(lastReceiver.end()) < 0) {
                    // we're at the end and we've processed it move start on to next receiver
                    if (startEqualsEndAndProcessed(startPosition, lastReceiver)) {
                        startPosition.setPosition(new JournalPosition(nextReceiver.start(), nextReceiver.info().receiver()), false);
                    }
                    else {
                        // the only way we can get here is if we have already checked for pagination
                        return Optional.of(new PositionRange(false, startPosition,
                                new JournalPosition(lastReceiver.end(), lastReceiver.info().receiver())));
                    }
                }

                final Optional<PositionRange> r = rangeWithinCurrentPosition(nextReceiver, nextReceiver.start());
                lastReceiver = nextReceiver;
                return r;
            }
            else {
                if (nextReceiver.isSameReceiver(startPosition)) {
                    found = true;
                    final Optional<PositionRange> r = rangeWithinCurrentPosition(nextReceiver, startPosition.getOffset());
                    lastReceiver = nextReceiver;
                    return r;
                }
            }
            lastReceiver = nextReceiver;
            return Optional.empty();
        }

        // adding one to the range and then adding as we include both ends
        // but we must not use the add one when setting the end point
        // i.e. 1-> 10 is a total of 10 entries but the range can only go to 10
        private Optional<PositionRange> rangeWithinCurrentPosition(DetailedJournalReceiver nextReceiver,
                                                                   BigInteger currentOffset) {
            final BigInteger difference = nextReceiver.end().subtract(currentOffset);
            final BigInteger entriesInJournal = difference.add(BigInteger.ONE); // add one as range is inclusive
            if (remaining.compareTo(difference) <= 0) { // range is inclusive but don't go past end when adding
                // remaining
                final BigInteger offset = currentOffset.add(remaining);
                return Optional.of(new PositionRange(false, startPosition,
                        new JournalPosition(offset, nextReceiver.info().receiver())));
            }
            remaining = remaining.subtract(entriesInJournal);
            return Optional.empty();
        }

        public Optional<PositionRange> endRange() {
            if (found && lastReceiver != null) {
                return Optional.of(
                        new PositionRange(false, startPosition, JournalPosition.endPosition(lastReceiver)));
            }
            return Optional.empty();
        }

        public boolean startFound() {
            return found;
        }

        private boolean startEqualsEndAndProcessed(JournalProcessedPosition start, DetailedJournalReceiver last) {
            return start.processed() && start.getOffset().equals(last.end());
        }
    }
}
