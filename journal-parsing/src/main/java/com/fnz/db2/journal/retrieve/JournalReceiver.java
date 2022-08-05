package com.fnz.db2.journal.retrieve;

public record JournalReceiver(String reciever, String library) {
	@Override
	public String toString() {
		return "JournalReciever [reciever=" + reciever + ", library=" + library + "]";
	}
}
