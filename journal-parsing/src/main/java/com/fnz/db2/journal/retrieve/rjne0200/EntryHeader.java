package com.fnz.db2.journal.retrieve.rjne0200;

import java.math.BigInteger;
import java.sql.Timestamp;

import com.fnz.db2.journal.retrieve.JournalEntryType;
import com.fnz.db2.journal.retrieve.StringHelpers;

public class EntryHeader {
	private final int nextEntryOffset;
	private final int nullValueOffest;
	private final int entrySpecificDataOffset;
//	private final int transactionDataOffset;
//	private final int logicalWorkOffset;
//	private final int receiverOffset;
	private final BigInteger sequenceNumber;
	private final BigInteger systemSequenceNumber;
	private final java.sql.Timestamp timestamp;
	private final char journalCode;
	private final String entryType;
	private final String objectName;
	private final BigInteger commitCycle;
	private final int endOffset;
	private final long pointerHandle;
	private final String receiver;
	private final String receiverLibrary;
	
	public EntryHeader(int nextEntryOffset, int nullValueOffest, long entrySpecificDataOffset, BigInteger sequenceNumber, BigInteger systemSequenceNumber,
			java.sql.Timestamp timestamp, char journalCode, String entryType, String objectName, BigInteger commitCycle, int endOffset, long pointerHandle, 
			String receiver, String receiverLibrary) {
		super();
		this.nextEntryOffset = nextEntryOffset;
		this.nullValueOffest = nullValueOffest;
		this.entrySpecificDataOffset = (int)entrySpecificDataOffset;
		this.sequenceNumber = sequenceNumber;
		this.systemSequenceNumber = systemSequenceNumber;
		this.timestamp = timestamp;
		this.journalCode = journalCode;
		this.entryType = entryType;
		this.objectName = objectName;
		this.commitCycle = commitCycle;
		this.endOffset = endOffset;
		this.pointerHandle = pointerHandle;
		this.receiver = receiver;
		this.receiverLibrary = receiverLibrary;
	}



	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("EntryHeader [nextEntryOffset=");
		builder.append(nextEntryOffset);
		builder.append(", nullValueOffest=");
		builder.append(nullValueOffest);
		builder.append(", entrySpecificDataOffset=");
		builder.append(entrySpecificDataOffset);
		builder.append(", sequenceNumber=");
		builder.append(sequenceNumber);
		builder.append(", systemSequenceNumber=");
		builder.append(systemSequenceNumber);
		builder.append(", timestamp=");
		builder.append(timestamp);
		builder.append(", journalCode=");
		builder.append(journalCode);
		builder.append(", entryType=");
		builder.append(entryType);
		builder.append(", objectName=");
		builder.append(objectName);
		builder.append(", commitCycle=");
		builder.append(commitCycle);
		builder.append(", endOffset=");
		builder.append(endOffset);
		builder.append(", pointerHandle=");
		builder.append(pointerHandle);
		builder.append(", receiver=");
		builder.append(receiver);
		builder.append(", receiverLibrary=");
		builder.append(receiverLibrary);
		builder.append("]");
		return builder.toString();
	}

	public int getLength() {
	    return getEndOffset() - getEntrySpecificDataOffset();
	}

	public int getNextEntryOffset() {
		return nextEntryOffset;
	}

	public int getEntrySpecificDataOffset() {
		return entrySpecificDataOffset;
	}

	public BigInteger getSequenceNumber() {
		return sequenceNumber;
	}

	public BigInteger getSystemSequenceNumber() {
		return systemSequenceNumber;
	}

//	public java.sql.Timestamp getTime() {
//		return timestamp;//new BigDecimal(timestamp).divide(BigDecimal.valueOf(4096000), 10, RoundingMode.HALF_UP).subtract(BigDecimal.valueOf(1305118613685l)).longValue();
//	}
	
	public Timestamp getTimestamp() {
		return timestamp;//new Timestamp(getTime());
	}

	public char getJournalCode() {
		return journalCode;
	}

	public String getEntryType() {
		return entryType;
	}

	public String getObjectName() {
		return objectName;
	}
	
	public JournalEntryType getJournalEntryType() {
        String entryType = String.format("%s.%s", getJournalCode(), getEntryType());
        return JournalEntryType.toValue(entryType);
	}
	
	/**
	 * @return table name
	 */
	public String getFile() {
		return StringHelpers.safeTrim(objectName.substring(0, 10));
	}
	/**
	 * @return schema
	 */
	public String getLibrary() {
		return StringHelpers.safeTrim(objectName.substring(10, 20));
	}
	/**
	 * @return magic stuff within a file
	 */
	public String getMember() {
		return StringHelpers.safeTrim(objectName.substring(20, 30));
	}
	public BigInteger getCommitCycle() {
		return commitCycle;
	}
	public int getEndOffset() {
		return endOffset;
	}
	public int getNullValueOffest() {
		return nullValueOffest;
	}
	
	public boolean hasReceiver() {
		return !receiver.isEmpty();
	}

	public String getReceiver() {
		return receiver;
	}
	public String getReceiverLibrary() {
		return receiverLibrary;
	}
}
