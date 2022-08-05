package com.fnz.db2.journal.retrieve;

public class JournalInfo {
	public String receiver;
	public String receiverLibrary;

	public JournalInfo(String receiver, String receiverLibrary) {
		super();
		if (receiver == null || receiver.trim().length() == 0 || receiver.trim().length() > 10) {
			throw new IllegalArgumentException("receiver name must not be null and length must be <= to 10.");
		}
		if (receiverLibrary == null || receiverLibrary.trim().length() == 0 || receiverLibrary.trim().length() > 10) {
			throw new IllegalArgumentException("receiverLibrary name must not be null and length must be <= to 10.");
		}	
		this.receiver = receiver.trim();
		this.receiverLibrary = receiverLibrary.trim();
	}

	@Override
	public String toString() {
		return "JournalLib [receiver=" + receiver + ", receiverLibrary=" + receiverLibrary + "]";
	}
	
	
}
