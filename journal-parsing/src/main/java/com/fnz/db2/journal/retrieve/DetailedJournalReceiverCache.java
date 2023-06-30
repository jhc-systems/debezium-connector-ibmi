package com.fnz.db2.journal.retrieve;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger log = LoggerFactory.getLogger(DetailedJournalReceiverCache.class);
	
	public boolean containsKey(JournalReceiverInfo receiverInfo)  {
		String key = toKey(receiverInfo);
		return cached.containsKey(key);
	}

	public DetailedJournalReceiver getUpdatingStatus(JournalReceiverInfo receiverInfo)  {
		String key = toKey(receiverInfo);
		DetailedJournalReceiver dr = cached.get(key);
		if (dr.info() == null || dr.info().status() == null) {
			log.warn("null in receiver info", dr);
		}
		if (receiverInfo.status() != null && dr.info() != null && ! receiverInfo.status().equals(dr.info().status())) {
			dr = dr.withStatus(receiverInfo.status());
			cached.put(key, dr);
		}
		return dr;
	}

	public void put(DetailedJournalReceiver details) {
		if (details.info().status() == null || details.info().attachTime() == null || JournalStatus.Attached.equals(details.info().status())) {
			log.debug("not caching {} ", details);
		}
		cached.put(toKey(details.info()), details);
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
