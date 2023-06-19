package com.fnz.db2.journal.retrieve;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;

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

	public DetailedJournalReceiver getUpdatingStatus(JournalReceiverInfo receiverInfo)  {
		String key = toKey(receiverInfo);
		DetailedJournalReceiver dr = cached.get(key);
		if (receiverInfo.status() != null && dr.info() != null && ! receiverInfo.status().equals(dr.info().status())) {
			dr = dr.withStatus(receiverInfo.status());
			cached.put(key, dr);
		}
		return dr;
	}

	public void put(JournalReceiverInfo receiverInfo, DetailedJournalReceiver details) {
		cached.put(toKey(receiverInfo), details);
	}
	
	public void keepOnly(List<DetailedJournalReceiver> list) {
		Set<String> toGo = new HashSet<>(cached.keySet()); // note delete operations on keySet will delete from underlying hash
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
