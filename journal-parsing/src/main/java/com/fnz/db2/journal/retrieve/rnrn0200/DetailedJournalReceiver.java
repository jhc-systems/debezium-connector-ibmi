package com.fnz.db2.journal.retrieve.rnrn0200;


public record DetailedJournalReceiver(
        JournalReceiverInfo info, 
        Integer start, 
        Integer end, 
        String nextReceiver,
        String nextDualReceiver, 
        long maxEntryLength, 
        long numberOfEntries) {};