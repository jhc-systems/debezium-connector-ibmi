package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.fnz.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import com.fnz.db2.journal.retrieve.rnrn0200.KeyDecoder;
import com.fnz.db2.journal.retrieve.rnrn0200.KeyHeader;
import com.fnz.db2.journal.retrieve.rnrn0200.ReceiverDecoder;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Bin8;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.FileAttributes;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;

/**
 * @see http://www.setgetweb.com/p/i5/rzakiwrkjrna.htm
 * @author sillencem
 *
 */
public class JournalInfoRetrieval {
	public static final String JOURNAL_SERVICE_LIB = "/QSYS.LIB/QJOURNAL.SRVPGM";

	private static final byte[] EMPTY_AS400_TEXT = new AS400Text(0).toBytes("");
	private static final AS400Text AS400_TEXT_8 = new AS400Text(8);
	private static final AS400Text AS400_TEXT_20 = new AS400Text(20);
	private static final AS400Bin8 AS400_BIN8 = new AS400Bin8();
	private static final AS400Bin4 AS400_BIN4 = new AS400Bin4();
	private static final int KEY_HEADER_LENGTH = 20;
	static final Logger log = LoggerFactory.getLogger(JournalInfoRetrieval.class);

	public JournalInfoRetrieval() {
		super();
	}

	public JournalPosition getCurrentPosition(AS400 as400, JournalInfo journalLib) throws Exception {
		final JournalInfo ji = JournalInfoRetrieval.getReceiver(as400, journalLib);
		final BigInteger offset = getOffset(as400, ji).end();
		return new JournalPosition(offset, ji.journalName, ji.journalLibrary, false);
	}

	public DetailedJournalReceiver getCurrentDetailedJournalReceiver(AS400 as400, JournalInfo journalLib)
			throws Exception {
		final JournalInfo ji = JournalInfoRetrieval.getReceiver(as400, journalLib);
		return getOffset(as400, ji);
	}

	static final Pattern JOURNAL_REGEX = Pattern.compile("\\/[^/]*\\/([^.]*).LIB\\/(.*).JRN");

	public static JournalInfo getJournal(AS400 as400, String schema) throws IllegalStateException {
		try {
			final FileAttributes fa = new FileAttributes(as400, String.format("/QSYS.LIB/%s.LIB", schema));
			final Matcher m = JOURNAL_REGEX.matcher(fa.getJournal());
			if (m.matches()) {
				return new JournalInfo(m.group(2), m.group(1));
			} else {
				log.error("no match searching for journal filename {}", fa.getJournal());
			}
		} catch (final Exception e) {
			throw new IllegalStateException("Journal not found", e);
		}
		throw new IllegalStateException("Journal not found");
	}

	public static class JournalRetrievalCriteria {
		private static final AS400Text AS400_TEXT_1 = new AS400Text(1);
		private static final Integer ZERO_INT = Integer.valueOf(0);
		private static final Integer TWELVE_INT = Integer.valueOf(12);
		private static final Integer ONE_INT = Integer.valueOf(1);
		private final ArrayList<AS400DataType> structure = new ArrayList<>();
		private final ArrayList<Object> data = new ArrayList<>();

		public JournalRetrievalCriteria() {
			// first element is the number of variable length records
			structure.add(AS400_BIN4);
			structure.add(AS400_BIN4);
			structure.add(AS400_BIN4);
			structure.add(AS400_BIN4);
			structure.add(AS400_TEXT_1);
			data.add(ONE_INT); // number of records
			data.add(TWELVE_INT); // data length
			data.add(ONE_INT); // 1 = journal directory info
			data.add(ZERO_INT);
			data.add("");
		}

		public AS400DataType[] getStructure() {
			return structure.toArray(new AS400DataType[structure.size()]);
		}

		public Object[] getObject() {
			return data.toArray(new Object[0]);
		}
	}

	/**
	 * uses the current attached journal information in the header
	 * 
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNI.htm
	 * @param as400
	 * @param journalLibrary
	 * @param journalFile
	 * @return
	 * @throws Exception
	 */
	public static JournalInfo getReceiver(AS400 as400, JournalInfo journalLib) throws Exception {
		final int rcvLen = 4096;
		final String jrnLib = padRight(journalLib.journalName, 10) + padRight(journalLib.journalLibrary, 10);
		final String format = "RJRN0200";
		final ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(rcvLen / 4096)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_20.toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_8.toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, EMPTY_AS400_TEXT),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(0)) };

		return callServiceProgram(as400, JOURNAL_SERVICE_LIB, "QjoRetrieveJournalInformation", parameters,
				(byte[] data) -> {
					// Attached journal receiver name. The name of the journal receiver that is
					// currently attached to this journal. This field will be blank if no journal
					// receivers are attached.
					final String journalReceiver = decodeString(data, 200, 10);
					final String journalLibrary = decodeString(data, 210, 10);
					return new JournalInfo(journalReceiver, journalLibrary);
				});
	}
 
	private byte[] getReceiversForJournal(AS400 as400, JournalInfo journalLib, int bufSize) throws Exception {
		final String jrnLib = padRight(journalLib.journalName, 10) + padRight(journalLib.journalLibrary, 10);
		final String format = "RJRN0200";

		final JournalRetrievalCriteria criteria = new JournalRetrievalCriteria();
		final byte[] toRetrieve = new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject());
		final ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufSize),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(bufSize / 4096)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_20.toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_8.toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, toRetrieve),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(0)) };
		return callServiceProgram(as400, JOURNAL_SERVICE_LIB, "QjoRetrieveJournalInformation", parameters,
				(byte[] data) -> data);
	}

	/**
	 * requests the list of receivers and orders them in attach time
	 * 
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNI.htm
	 * @param as400
	 * @param journalLibrary
	 * @param journalFile
	 * @return
	 * @throws Exception
	 */
	public List<DetailedJournalReceiver> getReceivers(AS400 as400, JournalInfo journalLib) throws Exception {
		final int defaultSize = 32768;
		byte[] data = getReceiversForJournal(as400, journalLib, 32768);
		final int actualSizeRequired = decodeInt(data, 4) * 4096; // bytes available - value returned for rjrn0200 is 4k
																	// pages
		if (actualSizeRequired > defaultSize) {
			data = getReceiversForJournal(as400, journalLib, actualSizeRequired);
		}

		final Integer keyOffset = decodeInt(data, 8) + 4;
		final Integer totalKeys = decodeInt(data, keyOffset);
		final KeyDecoder keyDecoder = new KeyDecoder();

		final List<DetailedJournalReceiver> l = new ArrayList<>();

		for (int k = 0; k < totalKeys; k++) {
			final KeyHeader kheader = keyDecoder.decode(data, keyOffset + k * KEY_HEADER_LENGTH);
			if (kheader.getKey() == 1) {

				final ReceiverDecoder dec = new ReceiverDecoder();
				for (int i = 0; i < kheader.getNumberOfEntries(); i++) {
					final int kioffset = keyOffset + kheader.getOffset() + kheader.getLengthOfHeader()
							+ i * kheader.getLengthOfKeyInfo();

					final JournalReceiverInfo r = dec.decode(data, kioffset);
					final DetailedJournalReceiver details = getOffset(as400, r);

					l.add(details);
				}
			}
		}
		
		return DetailedJournalReceiver.lastJoined(l);
	}

	static DetailedJournalReceiver getOffset(AS400 as400, JournalInfo info) throws Exception {
		return getOffset(as400,
				new JournalReceiverInfo(info.journalName, info.journalLibrary, null, null, Optional.empty()));
	}

	/**
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORRCVI.htm
	 * @param as400
	 * @param receiverInfo
	 * @return
	 * @throws Exception
	 */
	private static DetailedJournalReceiver getOffset(AS400 as400, JournalReceiverInfo receiverInfo) throws Exception {
		final int rcvLen = 32768;
		final String jrnLib = padRight(receiverInfo.name(), 10) + padRight(receiverInfo.library(), 10);
		final String format = "RRCV0100";
		final ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(rcvLen)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_20.toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_8.toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(0)) };

		return callServiceProgram(as400, JOURNAL_SERVICE_LIB, "QjoRtvJrnReceiverInformation", parameters,
				(byte[] data) -> {
					final String journalName = decodeString(data, 8, 10);
					final String nextReceiver = decodeString(data, 332, 10);
					final Long numberOfEntries = Long.valueOf(decodeString(data, 372, 20));
					final Long maxEntryLength = Long.valueOf(decodeString(data, 392, 20));
					final BigInteger firstSequence = decodeBigIntFromString(data, 412);
					final BigInteger lastSequence = decodeBigIntFromString(data, 432);

					if (!journalName.equals(receiverInfo.name())) {
						final String msg = String.format("journal names don't match requested %s got %s",
								receiverInfo.name(), journalName);
						throw new Exception(msg);
					}
					return new DetailedJournalReceiver(receiverInfo, firstSequence, lastSequence, nextReceiver,
							maxEntryLength, numberOfEntries);
				});
	}

	/**
	 *
	 * @param <T>            return type of processor
	 * @param as400
	 * @param programLibrary
	 * @param program
	 * @param parameters     assumes first parameter is output
	 * @param processor
	 * @return output of processor
	 * @throws Exception
	 */
	public static <T> T callServiceProgram(AS400 as400, String programLibrary, String program,
			ProgramParameter[] parameters, ProcessData<T> processor) throws Exception {
		final ServiceProgramCall spc = new ServiceProgramCall(as400);

		spc.getServerJob().setLoggingLevel(0);
		spc.setProgram(programLibrary, parameters);
		spc.setProcedureName(program);
		spc.setAlignOn16Bytes(true);
		spc.setReturnValueFormat(ServiceProgramCall.NO_RETURN_VALUE);
		final boolean success = spc.run();
		if (success) {
			return processor.process(parameters[0].getOutputData());
		} else {
			final String msg = Arrays.asList(spc.getMessageList()).stream().map(AS400Message::getText).reduce("",
					(a, s) -> a + s);
			log.error(String.format("service program %s/%s call failed %s", programLibrary, program, msg));
			throw new Exception(msg);
		}
	}

	public static Integer decodeInt(byte[] data, int offset) {
		final byte[] b = Arrays.copyOfRange(data, offset, offset + 4);
		return Integer.valueOf(AS400_BIN4.toInt(b));
	}

	public static Long decodeLong(byte[] data, int offset) {
		final byte[] b = Arrays.copyOfRange(data, offset, offset + 8);
		return Long.valueOf(AS400_BIN8.toLong(b));
	}

	public static String decodeString(byte[] data, int offset, int length) {
		final byte[] b = Arrays.copyOfRange(data, offset, offset + length);
		return StringHelpers.safeTrim((String) new AS400Text(length).toObject(b));
	}

	public static String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}

	public interface ProcessData<T> {
		public T process(byte[] data) throws Exception;
	}

	public static BigInteger decodeBigIntFromString(byte[] data, int offset) {
		final byte[] b = Arrays.copyOfRange(data, offset, offset + 20);
		final String s = (String) AS400_TEXT_20.toObject(b);
		return new BigInteger(s);
	}
}
