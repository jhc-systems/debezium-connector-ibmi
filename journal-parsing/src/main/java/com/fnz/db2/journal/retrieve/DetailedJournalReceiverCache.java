package com.fnz.db2.journal.retrieve;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalStatus;

/**
 * caches index on receiver name and lib
 * does not cache attached receivers as they will update
 * @author martin.sillence
 *
 */
public class DetailedJournalReceiverCache {
	private Map<String, DetailedJournalReceiver> cached = new HashMap<>();
	
	public boolean containsKey(JournalReceiverInfo receiverInfo)  {
		String key = toKey(receiverInfo);
		return cached.containsKey(key);
	}

	public DetailedJournalReceiver get(JournalReceiverInfo receiverInfo)  {
		String key = toKey(receiverInfo);
		return cached.get(key);
	}

	public void put(JournalReceiverInfo receiverInfo, DetailedJournalReceiver details) {
		if (!JournalStatus.Attached.equals(receiverInfo.status())) { // don't cache attached as the end offset will change
			cached.put(toKey(receiverInfo), details);
		}
	}
	
	public void keepOnly(List<DetailedJournalReceiver> list) {
		Set<String> toGo = cached.keySet();
		Set<String> keep = list.stream().map(this::toKey).collect(Collectors.toSet());
		toGo.removeAll(keep);
		toGo.stream().forEach(x -> cached.remove(x));
	}
	
	private String toKey(JournalReceiverInfo receiverInfo) {
		String key = String.format("%s:%s", receiverInfo.name(), receiverInfo.library());
		return key;
	}

	private String toKey(DetailedJournalReceiver receiverInfo) {
		String key = String.format("%s:%s", receiverInfo.info().name(), receiverInfo.info().library());
		return key;
	}
}
