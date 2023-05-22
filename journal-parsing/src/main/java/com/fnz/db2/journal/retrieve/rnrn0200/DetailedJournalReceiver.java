package com.fnz.db2.journal.retrieve.rnrn0200;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import com.fnz.db2.journal.retrieve.JournalPosition;

public record DetailedJournalReceiver(JournalReceiverInfo info, BigInteger start, BigInteger end, String nextReceiver,
		String nextDualReceiver, long maxEntryLength, long numberOfEntries) {
	
	public boolean isSameReceiver(JournalPosition position) {
		if (info == null || position == null)
			return false;
		return info.name().equals(position.getReciever()) && info.library().equals(position.getReceiverLibrary());
	}

	public boolean isSameReceiver(DetailedJournalReceiver otherReceiver) {
		if (info == null || otherReceiver == null || otherReceiver.info() == null)
			return false;
		return info.name().equals(otherReceiver.info().name()) && info.library().equals(otherReceiver.info().library());
	}
	
	public static List<DetailedJournalReceiver> lastJoined(List<DetailedJournalReceiver> list) {
		list.sort((DetailedJournalReceiver f, DetailedJournalReceiver s) -> f.info().attachTime()
				.compareTo(s.info().attachTime()));
		int last = indexOfLastJoined(list);
		if (last == 0)
			return list;
		if (last >= list.size())
			return Collections.<DetailedJournalReceiver>emptyList();
		return list.subList(last, list.size());
	}

	public static int indexOfLastJoined(List<DetailedJournalReceiver> list) {
		int size = list.size();
		int last = 0;
		for (int i=0; i<size; i++) {
			if (!list.get(i).isJoined()) {
				last = i + 1;
			}				
		}
		return last;
	}

	public boolean isJoined() {
		return isJoined(this.info.status());
	}
	
	public static boolean isJoined(JournalStatus status) {
		return switch (status) {
			case Attached, OnlineSavedDetached, SavedDetchedNotFreed -> true;
			default -> false;
		};
	}

}