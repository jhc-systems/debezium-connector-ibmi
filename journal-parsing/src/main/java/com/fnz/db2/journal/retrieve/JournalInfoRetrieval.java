package com.fnz.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
	private static final byte[] EMPTY_AS400_TEXT = new AS400Text(0).toBytes("");
    private static final AS400Text AS400_TEXT_8 = new AS400Text(8);
    private static final AS400Text AS400_TEXT_20 = new AS400Text(20);
    private static final AS400Bin8 AS400_BIN8 = new AS400Bin8();
    private static final AS400Bin4 AS400_BIN4 = new AS400Bin4();
    private static final int KEY_HEADER_LENGTH = 20;
	static final Logger log = LoggerFactory.getLogger(JournalInfoRetrieval.class);
	
	public static JournalPosition getCurrentPosition(AS400 as400, JournalInfo journalLib) throws Exception {
		JournalInfo ji = JournalInfoRetrieval.getReceiver(as400, journalLib);
		BigInteger offset = getOffset(as400, ji).end();
		return new JournalPosition(offset, ji.receiver, ji.receiverLibrary, false);
	}
	
	static final Pattern JOURNAL_REGEX = Pattern.compile("\\/[^/]*\\/([^.]*).LIB\\/(.*).JRN");
	public static JournalInfo getJournal(AS400 as400, String schema) throws IllegalStateException {
		try {
			FileAttributes fa = new FileAttributes(as400, String.format("/QSYS.LIB/%s.LIB", schema));
			Matcher m = JOURNAL_REGEX.matcher(fa.getJournal());
			if (m.matches()) {
				return new JournalInfo(m.group(2), m.group(1));
			} else {
				log.error("no match searching for journal filename {}", fa.getJournal());
			}
		} catch (Exception e) {
			throw new IllegalStateException("Journal not found", e);
		}
		throw new IllegalStateException("Journal not found");
	}

	public static class JournalRetrievalCriteria {
        private static final AS400Text AS400_TEXT_1 = new AS400Text(1);
        private static final Integer ZERO_INT = Integer.valueOf(0);
        private static final Integer TWELVE_INT = Integer.valueOf(12);
        private static final Integer ONE_INT = Integer.valueOf(1);
        private ArrayList<AS400DataType> structure = new ArrayList<AS400DataType>();
		private ArrayList<Object> data = new ArrayList<Object>();

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
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNI.htm
	 * @param as400
	 * @param receiverLibrary
	 * @param journalFile
	 * @return
	 * @throws Exception
	 */
	public static JournalInfo getReceiver(AS400 as400, JournalInfo journalLib) throws Exception {
		int rcvLen = 32768;
		String jrnLib = padRight(journalLib.receiver, 10) + padRight(journalLib.receiverLibrary, 10);
		String format = "RJRN0200";
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(rcvLen)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_20.toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_8.toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, EMPTY_AS400_TEXT),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(0))
		};

		return callServiceProgram(as400, "/QSYS.LIB/QJOURNAL.SRVPGM", "QjoRetrieveJournalInformation", parameters, (byte[] data) -> {
			String journalReceiver = decodeString(data, 200, 10);
			String journalLibrary = decodeString(data, 210, 10);
			return new JournalInfo(journalReceiver, journalLibrary);
		});
	}
	
	/**
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNI.htm
	 * @param as400
	 * @param receiverLibrary
	 * @param journalFile
	 * @return
	 * @throws Exception
	 */
	public static List<DetailedJournalReceiver> getReceivers(AS400 as400, JournalInfo journalLib) throws Exception {
		int rcvLen = 32768;
		String jrnLib = padRight(journalLib.receiver, 10) + padRight(journalLib.receiverLibrary, 10);
		String format = "RJRN0200";
		
		JournalRetrievalCriteria criteria = new JournalRetrievalCriteria();
		byte[] toRetrieve = new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject());
		
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(rcvLen)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_20.toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_8.toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, toRetrieve),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(0))
		};

		return callServiceProgram(as400, "/QSYS.LIB/QJOURNAL.SRVPGM", "QjoRetrieveJournalInformation", parameters, (byte[] data) -> {
			Integer keyOffset = decodeInt(data, 8) + 4;
			Integer totalKeys = decodeInt(data, keyOffset);
			KeyDecoder keyDecoder = new KeyDecoder();
			
			List<DetailedJournalReceiver> l = new ArrayList<>();

			for (int k=0; k<totalKeys; k++) {
				KeyHeader kheader = keyDecoder.decode(data,  keyOffset + k * KEY_HEADER_LENGTH);
				if (kheader.getKey() == 1) {
					
					ReceiverDecoder dec = new ReceiverDecoder();
					
					for (int i=0; i<kheader.getNumberOfEntries(); i++) {
						int kioffset = keyOffset + kheader.getOffset() + kheader.getLengthOfHeader() + i*kheader.getLengthOfKeyInfo();
						
						JournalReceiverInfo r = dec.decode(data, kioffset);
						DetailedJournalReceiver details = getOffset(as400, r);
						
						l.add(details);
					}
				}
			}
			
			l.sort((DetailedJournalReceiver f, DetailedJournalReceiver s) -> f.info().attachTime().compareTo(s.info().attachTime()));
			
			return l;
		});
	}

	static DetailedJournalReceiver getOffset(AS400 as400, JournalInfo info) throws Exception {
	    return getOffset(as400, new JournalReceiverInfo(info.receiver, info.receiverLibrary, null, null));
	}
	
	/**
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORRCVI.htm
	 * @param as400
	 * @param receiverInfo
	 * @return
	 * @throws Exception
	 */
	private static DetailedJournalReceiver getOffset(AS400 as400, JournalReceiverInfo receiverInfo)
			throws Exception {
		int rcvLen = 32768;
		String jrnLib = padRight(receiverInfo.name(), 10) + padRight(receiverInfo.library(), 10);
		String format = "RRCV0100";
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(rcvLen)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_20.toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_TEXT_8.toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, AS400_BIN4.toBytes(0)) };

		return callServiceProgram(as400, "/QSYS.LIB/QJOURNAL.SRVPGM", "QjoRtvJrnReceiverInformation", parameters, (byte[] data) -> {
			String journalName = decodeString(data, 8, 10);
			String nextReceiver = decodeString(data, 332, 10);
			String nextDualReceiver = decodeString(data, 352, 10);
			Long numberOfEntries = Long.valueOf(decodeString(data, 372, 20));
            Long maxEntryLength = Long.valueOf(decodeString(data, 392, 20));
			BigInteger firstSequence = decodeBigIntFromString(data, 412);
			BigInteger lastSequence = decodeBigIntFromString(data, 432);
			
			if (!journalName.equals(receiverInfo.name())) {
				String msg = String.format("journal names don't match requested %s got %s", receiverInfo.name(), journalName);
				throw new Exception(msg);
			}
			return new DetailedJournalReceiver(receiverInfo, firstSequence, lastSequence, nextReceiver, nextDualReceiver, maxEntryLength, numberOfEntries);
		});
	}
	
	
	/**
	 * 
	 * @param <T> return type of processor
	 * @param as400
	 * @param programLibrary
	 * @param program
	 * @param parameters assumes first parameter is output
	 * @param processor
	 * @return output of processor
	 * @throws Exception 
	 */
	public static <T> T callServiceProgram(AS400 as400, String programLibrary, String program, ProgramParameter[] parameters, ProcessData<T> processor) throws Exception {
		ServiceProgramCall spc = new ServiceProgramCall(as400);
		
		spc.getServerJob().setLoggingLevel(0);
		spc.setProgram(programLibrary, parameters);
		spc.setProcedureName(program);
		spc.setAlignOn16Bytes(true);
		spc.setReturnValueFormat(ServiceProgramCall.NO_RETURN_VALUE);
		boolean success = spc.run();
		if (success) {
			return processor.process(parameters[0].getOutputData());
		} else {
			String msg = Arrays.asList(spc.getMessageList()).stream().map(x -> x.getText()).reduce("", (a, s) -> a + s);
			log.error(String.format("service program %s/%s call failed %s", programLibrary, program, msg));
			throw new Exception(msg);
		}
	}
	
	public static Integer decodeInt(byte[] data, int offset) {
		byte [] b = Arrays.copyOfRange(data, offset, offset + 4); 
		return Integer.valueOf(AS400_BIN4.toInt(b));		
	}
	
   public static Long decodeLong(byte[] data, int offset) {
        byte [] b = Arrays.copyOfRange(data, offset, offset + 8); 
        return Long.valueOf(AS400_BIN8.toLong(b));        
    }
	
	public static String decodeString(byte[] data, int offset, int length) {
		byte[] b = Arrays.copyOfRange(data, offset, offset+length);
		return StringHelpers.safeTrim((String) new AS400Text(length).toObject(b));
	}
	
	public static String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}
	
	public interface ProcessData<T>  {
		public T process(byte[] data) throws Exception;
	}
	
	public static BigInteger decodeBigIntFromString(byte[] data, int offset) {
		byte [] b = Arrays.copyOfRange(data, offset, offset + 20); 
		String s = (String)AS400_TEXT_20.toObject(b);
		return new BigInteger(s);
	}
}
