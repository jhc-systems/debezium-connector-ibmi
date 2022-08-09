package com.fnz.db2.journal.retrieve.rnrn0200;

import java.math.BigInteger;

public record DetailedJournalReceiver(
        JournalReceiverInfo info, 
        BigInteger start, 
        BigInteger end, 
        String nextReceiver,
        String nextDualReceiver, 
        long maxEntryLength, 
        long numberOfEntries) {};