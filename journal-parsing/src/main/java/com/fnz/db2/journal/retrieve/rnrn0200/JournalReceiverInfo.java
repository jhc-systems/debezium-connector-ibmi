package com.fnz.db2.journal.retrieve.rnrn0200;

import java.util.Date;

public record JournalReceiverInfo(String name, String library, Date attachTime, JournalStatus status) { }
