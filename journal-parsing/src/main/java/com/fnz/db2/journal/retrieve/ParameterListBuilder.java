package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalEntryType;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.ProgramParameter;

// TODO remove now we've sanitised RetrievalCriteria
public class ParameterListBuilder {
	public static final int DEFAULT_JOURNAL_BUFFER_SIZE = 65536 * 2;
	public static final int ERROR_CODE = 0;
	private static final byte[] errorCodeData = new AS400Bin4().toBytes(ERROR_CODE);
	public static final String FORMAT_NAME = "RJNE0200";
	private static final byte[] formatNameData = new AS400Text(8).toBytes(FORMAT_NAME);

	private int bufferLength = DEFAULT_JOURNAL_BUFFER_SIZE;
	private byte[] bufferLengthData = new AS400Bin4().toBytes(bufferLength);

	private String receiver = "";
	private String receiverLibrary = "";
	private final RetrievalCriteria criteria = new RetrievalCriteria();
	private byte[] journalData;
	
	// for diagnostics
	private String startReceiver;
	private String startLibrary;
	private String endReceiver;
	private String endLibrary;
	private List<FileFilter> tableFilters;
	private String startOffset;
	private String endOffset;
	private RetrievalCriteria.JournalEntryType[] journalEntryTypes;
	private JournalCode[] journalCode;

	public ParameterListBuilder() {
		criteria.withLenNullPointerIndicatorVarLength();
	}

	public ParameterListBuilder withBufferLenth(int bufferLength) {
		this.bufferLength = bufferLength;
		this.bufferLengthData = new AS400Bin4().toBytes(bufferLength);
		return this;
	}

	public ParameterListBuilder withJournal(String receiver, String receiverLibrary) {
		if (!this.receiver.equals(receiver) && !this.receiverLibrary.equals(receiverLibrary)) {
			this.receiver = receiver;
			this.receiverLibrary = receiverLibrary;

			final String jrnLib = StringHelpers.padRight(receiver, 10)
					+ StringHelpers.padRight(receiverLibrary, 10);
			journalData = new AS400Text(20).toBytes(jrnLib);
		}
		return this;
	}

	public void init() {
		criteria.reset();
	}

	public ParameterListBuilder withJournalEntryType(JournalEntryType type) {
		criteria.withEntTyp(new JournalEntryType[] { type });
		return this;
	}

	public ParameterListBuilder withReceivers(String startReceiver, String startLibrary, String endReceiver,
			String endLibrary) {
		this.startReceiver = startReceiver;
		this.startLibrary = startLibrary;
		this.endReceiver = endReceiver;
		this.endLibrary = endLibrary;
		criteria.withReceiverRange(startReceiver, startLibrary, endReceiver, endLibrary);
		return this;
	}

	public ParameterListBuilder withEnd() {
		endOffset="last";
		criteria.withEnd();
		return this;
	}

	public ParameterListBuilder withEnd(BigInteger end) {
		endOffset = end.toString();
		criteria.withEnd(end);
		return this;
	}

	public ParameterListBuilder withReceivers(String receivers) {
		this.startReceiver = receivers;
		this.startLibrary = "";
		this.endReceiver = "";
		this.endLibrary = "";		
		criteria.withReceiverRange(receivers);
		return this;
	}


	public ParameterListBuilder withStartingSequence(BigInteger start) {
		startOffset = start.toString();
		criteria.withFromEnt(start);
		return this;
	}

	public ParameterListBuilder withFromStart() {
		startOffset="first";
		criteria.withFromEnt(RetrievalCriteria.FromEnt.FIRST);
		return this;
	}

	public ParameterListBuilder filterJournalCodes(JournalCode[] journalCode) {
		this.journalCode = journalCode;
		criteria.withJrnCde(journalCode);
		return this;
	}

	public ParameterListBuilder withFileFilters(List<FileFilter> tableFilters) {
		this.tableFilters = tableFilters;
		criteria.withFILE(tableFilters);
		return this;
	}

	public ParameterListBuilder filterJournalEntryType(RetrievalCriteria.JournalEntryType[] journalEntryTypes) {
		this.journalEntryTypes = journalEntryTypes;
		criteria.withEntTyp(journalEntryTypes);
		return this;
	}
	
	public void fromPositionToEnd(JournalPosition retrievePosition) {
		if (retrievePosition.isOffsetSet()) {
			withStartingSequence(retrievePosition.getOffset());
		} else {
			withFromStart();
		}
		withReceivers("*CURCHAIN");
		withEnd();
	}

	public ProgramParameter[] build() {
		final byte[] criteriaData = new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject());
		return new ProgramParameter[] { new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufferLength), // 1
																												// Receiver
																												// variable
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufferLengthData), // 2 Length of receiver
																							// variable
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, journalData), // 3 Qualified journal name
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, formatNameData), // 4 Format name
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, criteriaData), // 5 Journal entries to
																						// retrieve
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, errorCodeData) }; // 6 Error code
	}

	@Override
	public String toString() {
		return String.format(
				"ParameterListBuilder [receiver=%s, receiverLibrary=%s, criteria=%s, startReceiver=%s, startLibrary=%s, endReceiver=%s, endLibrary=%s, tableFilters=%s, startOffset=%s, endOffset=%s, journalEntryTypes=%s, journalCode=%s]",
				receiver, receiverLibrary, criteria, startReceiver, startLibrary,
				endReceiver, endLibrary, tableFilters, startOffset, endOffset, Arrays.toString(journalEntryTypes),
				Arrays.toString(journalCode));
	}
}
