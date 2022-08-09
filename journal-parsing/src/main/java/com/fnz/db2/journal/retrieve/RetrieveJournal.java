package com.fnz.db2.journal.retrieve;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalEntryType;
import com.fnz.db2.journal.retrieve.exception.InvalidPositionException;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import com.fnz.db2.journal.retrieve.rjne0200.FirstHeader;
import com.fnz.db2.journal.retrieve.rjne0200.FirstHeaderDecoder;
import com.fnz.db2.journal.retrieve.rjne0200.OffsetStatus;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;

/**
 * based on the work of Stanley Vong
 *
 */
public class RetrieveJournal {
    private static final Logger log = LoggerFactory.getLogger(RetrieveJournal.class);

    private final static JournalCode[] REQUIRED_JOURNAL_CODES = new JournalCode[] {JournalCode.D, JournalCode.R, JournalCode.C};
    private final static JournalEntryType[] REQURED_ENTRY_TYPES = new JournalEntryType[] {       
            JournalEntryType.PT, JournalEntryType.PX, JournalEntryType.UP, JournalEntryType.UB, 
            JournalEntryType.DL, JournalEntryType.DR, JournalEntryType.CT, JournalEntryType.CG, 
            JournalEntryType.SC, JournalEntryType.CM
    };
    private final static FirstHeaderDecoder firstHeaderDecoder = new FirstHeaderDecoder();
    private final static EntryHeaderDecoder entryHeaderDecoder = new EntryHeaderDecoder();
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyMMdd-hhmm");
    
	private ParameterListBuilder builder = new ParameterListBuilder();
	private final Connect<AS400, IOException> as400;
	private byte[] outputData = null;
	private FirstHeader header = null;
	private EntryHeader entryHeader = null;
	private int offset = -1;
	private JournalPosition position;
    private final File dumpFolder; 
	private final int journalBufferSize;
	private long totalTransferred = 0;
	private final boolean filterCodes;
	private final List<String> includeFiles;
	private boolean hasMoreData = true;
	private final boolean filteirng;
	private final JournalInfo journalInfo;

	public RetrieveJournal(Connect<AS400, IOException> as400, JournalInfo journalLib) {
		this(as400, journalLib, null, ParameterListBuilder.DEFAULT_JOURNAL_BUFFER_SIZE, true, null);
	}
	
	public RetrieveJournal(Connect<AS400, IOException> as400, JournalInfo journalLib, int journalBufferSize, boolean filterCodes, List<String> includeFiles) {
		this(as400, journalLib, null, journalBufferSize, true, includeFiles);
	}
	
	public RetrieveJournal(Connect<AS400, IOException> as400, JournalInfo journalLib, String dumpFolder) {
		this(as400, journalLib, dumpFolder, ParameterListBuilder.DEFAULT_JOURNAL_BUFFER_SIZE, true, null);
	}
	
	public RetrieveJournal(Connect<AS400, IOException> as400, JournalInfo journalInfo, String dumpFolder, int journalBufferSize, boolean filterCodes, List<String> includeFiles) {
	    this.filterCodes = filterCodes;
		this.journalBufferSize = journalBufferSize;
		if (includeFiles != null) {
		    if (includeFiles.size() < 300) {
		        this.includeFiles = includeFiles;
		        filteirng = true;
		    } else {
		        log.error("ignoring filter list as too many files included {} limit 300", includeFiles.size());
		        this.includeFiles = null;
		        filteirng = filterCodes;
		    }
		} else {
		    filteirng = filterCodes;
            this.includeFiles = null;
		}

		this.journalInfo = journalInfo;
		builder.withJournal(journalInfo.receiver, journalInfo.receiverLibrary); 
		this.as400 = as400;
		if (dumpFolder != null) {
			File f = new File(dumpFolder);
			if (f.exists())
				this.dumpFolder = f;
			else
				this.dumpFolder = null;
		} else {
			this.dumpFolder = null;
		}
	}

	private Pattern NOT_FOUND_PATTERN = Pattern.compile(".*Object [^ ]* in library [^ ]* not found.*");
	
	/**
	 * retrieves a block of journal data
	 * @param position
	 * @return true if the journal was read successfully false if there was some problem reading the journal
	 * @throws Exception
	 * 
	 * CURAVLCHN - returns only available journals
	 * CURCHAIN will work though journals that have happened but may no longer be available
	 * if the journal is no longer available we need to capture this and log an error as we may have missed data
	 */
	public boolean retrieveJournal(JournalPosition position) throws Exception {
        offset = -1;
        entryHeader = null;
        header = null;

        this.position = position;
		this.entryHeader = null;
		log.debug("Fetch journal at postion {}", position);
		ServiceProgramCall spc = new ServiceProgramCall(as400.connection());
		spc.getServerJob().setLoggingLevel(0);
		builder.init();
		builder.withJournalEntryType(JournalEntryType.ALL);
        if (includeFiles != null && !includeFiles.isEmpty()) {
            builder.filterFiles(includeFiles);
        }

		if (position.isOffsetSet()) {
			builder.withStartingSequence(position.getOffset());
		} else {
			builder.withFromStart();
		}
		if (position.getJournal().length > 0) {
			builder.withReceivers(position.getReciever(), position.getReceiverLibrary());
		} else {
			builder.withReceivers("*CURCHAIN"); 
		}
		builder.withBufferLenth(journalBufferSize);
		if (filterCodes) {
		    builder.filterJournalCodes(REQUIRED_JOURNAL_CODES);
		}
		builder.withEnd();

		JournalPosition currentPosition = null;
		if (!hasMoreData && filteirng) { // we didn't have any more data last time
		    currentPosition = JournalInfoRetrieval.getCurrentPosition(as400.connection(), journalInfo);
		}
		hasMoreData = false;
		
		ProgramParameter[] parameters = builder.build();
		spc.setProgram("/QSYS.LIB/QJOURNAL.SRVPGM", parameters);
		spc.setProcedureName("QjoRetrieveJournalEntries");
		spc.setAlignOn16Bytes(true);
		spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
		boolean success = spc.run();
		if (success) {
			outputData = parameters[0].getOutputData();
			header = firstHeaderDecoder.decode(outputData);
            totalTransferred += header.totalBytes();
			log.debug("first header: {} ", header);
			offset = -1;
			if (header.status() == OffsetStatus.MORE_DATA_NEW_OFFSET && header.offset()==0) {
				log.error("buffer too small skipping this entry {}", position);
				header.nextPosition().map(offset -> {
	                position.setPosition(offset); 
	                return null;    
	            });
			} 
			if (header.status() == OffsetStatus.NO_MORE_DATA && currentPosition != null) {
			    log.debug("moving on to current position");
			    header = header.withCurrentJournalPosition(currentPosition);
			}
				
		} else {
			for (AS400Message id: spc.getMessageList()) {
			    String idt = id.getID();
			    if (idt == null) {
			        log.error("Call failed position {} no Id, message: {}", position, id.getText());
			        continue;
			    }
    			switch (idt) {
        			case "CPF7053": { // sequence number does not exist or break in receivers
        			    throw new InvalidPositionException(String.format("Call failed position %s failed to find sequence or break in receivers: %s", position, id.getText()));
        			}
        			case "CPF9801": { // specify invalid receiver
                      throw new InvalidPositionException(String.format("Call failed position %s failed to find receiver: %s", position, id.getText()));
        			}
        			case "CPF7054": { // e.g. last < first
        			  throw new InvalidPositionException(String.format("Call failed position %s failed to find offset or invalid offsets: %s", position, id.getText()));
        			}
        			case "CPF7062": {
        			    log.debug("Call failed position {} no data received, probably all filtered: {}", position, id.getText());
        		        header = new FirstHeader(0, 0, 0, OffsetStatus.NO_MORE_DATA, Optional.<JournalPosition>empty());
        			    return true;
        			}
                    default: 
                        log.error("Call failed position {} with error code {} message {}", position, idt, id.getText());                        
    			}
			}
			
			throw new Exception(String.format("Call failed position %s", position));
		}
		return success;
	}
	
	/**
	 * @return the current position or the next offset for fetching data when the end of data is reached
	 */
	public JournalPosition getPosition() {
		return position;
	}
	
	public void setOutputData(byte[] b, FirstHeader header, JournalPosition position) {
		outputData = b;
		this.header = header;
		this.position = position;
	}

	public boolean futureDataAvailable() {
	    return (header != null && header.hasFutureDataAvailable());
	}
	
	public String headerAsString() {
	    StringBuilder sb = new StringBuilder();
	    if (header == null)
	        sb.append("null header\n");
	    else
	        sb.append(header);
        return sb.toString();
	}
	
	// test without moving on
	public boolean hasData() {
	    if (offset < 0 && header.size() > 0)
	        return true;
	    if (offset > 0 && entryHeader.getNextEntryOffset() > 0)
	        return true;
	    return false;
	}
	
	public boolean nextEntry() {
		if (offset < 0) {
			if (header.size() > 0) {
				offset=header.offset();
				entryHeader = entryHeaderDecoder.decode(outputData, offset);
				updatePosition(entryHeader);
				return true;
			} else {
				return false;
			}
		} else {
			long nextOffset = entryHeader.getNextEntryOffset();
			if (nextOffset > 0) {
				offset += (int)nextOffset;
				entryHeader = entryHeaderDecoder.decode(outputData, offset);
				updatePosition(entryHeader);
				return true;
			}
			
			// after we hit the end use the continuation header for the next offset
			header.nextPosition().map(offset -> {
			    log.debug("Setting confinuation offset {}", offset);
        		position.setPosition(offset); 
        		return null;	
        	});
			return false;
		}
	}

	private void updatePosition(EntryHeader entryHeader) {
		if (entryHeader.hasReceiver()) {
			position.setJournalReciever(entryHeader.getSequenceNumber(), entryHeader.getReceiver(), entryHeader.getReceiverLibrary(), true);
		} else {
			position.setOffset(entryHeader.getSequenceNumber(), true);
		}
	}
	
	public EntryHeader getEntryHeader() {
		return entryHeader;
	}
	
	public void dumpEntry() {
		int start = offset + entryHeader.getEntrySpecificDataOffset();
        long end = entryHeader.getNextEntryOffset();
		log.debug("total offset {} entry specific offset {} ",  start, entryHeader.getEntrySpecificDataOffset());
		
		log.debug("next offset {}", end);
	}
	
 	public int getOffset() {
		return offset;
	}
	
	public <T> T decode(JournalEntryDeocder<T> decoder) throws Exception {
//		Diagnostics.dump(outputData, start);
		try {
			T t = decoder.decode(entryHeader, outputData, offset);
			return t;
		} catch (Exception e) {
			dumpEntryToFile(dumpFolder);
			throw e;
		}
	}
	

	public void dumpEntryToFile(File path) {
		File dumpFile = null;
		if (path != null) {
			boolean created = false;
			for (int i=0; !created && i < 100 ; i++) {
				File f = new File(path, String.format("%s-%s", DATE_FORMATTER.format( new Date() ), Integer.toString(i)));
				try {
					created = f.createNewFile();
					if (created)
						dumpFile=f;
				} catch (IOException e) {
				}
			}
			if (dumpFile != null) {
				try {
					int start = offset;
					int end = outputData.length;
					
					byte[] bdata = Arrays.copyOfRange(outputData, start, end);
					Files.write(dumpFile.toPath(), bdata);
							
					File entryInfo = new File(dumpFile.getPath() + ".txt");
							
					try(FileWriter fw = new FileWriter(entryInfo, true);
					    BufferedWriter bw = new BufferedWriter(fw);
					    PrintWriter out = new PrintWriter(bw))
					{
					    out.println(entryHeader.toString());
					    out.print("dumped: ");
					    out.println(end-start);
					    out.print("total length: ");
					    out.println(outputData.length);
					}
				 } catch (IOException e) {
					 log.error("failed to dump problematic data", e);
				 }
			} else {
				 log.error("failed to create a dump file");
			}
		}
	}
	
	public FirstHeader getFirstHeader() {
		return header;
	}

	public static class ParameterListBuilder {
		public static final int DEFAULT_JOURNAL_BUFFER_SIZE = 65536 * 2;
		public static final int ERROR_CODE = 0;
		private static final byte[] errorCodeData = new AS400Bin4().toBytes(ERROR_CODE);
		public static final String FORMAT_NAME = "RJNE0200";
		private static final byte[] formatNameData = new AS400Text(8).toBytes(FORMAT_NAME);

		private int bufferLength = DEFAULT_JOURNAL_BUFFER_SIZE;
		private byte[] bufferLengthData = new AS400Bin4().toBytes(bufferLength);
		
		private String receiver = "";
		private String receiverLibrary = "";
		private RetrievalCriteria criteria = new RetrievalCriteria();
		private byte[] journalData;

		public ParameterListBuilder() {
			criteria.varLenNullPointerIndicatorLength();
		}
		
		public ParameterListBuilder withBufferLenth(int bufferLength) {
			this.bufferLength = bufferLength;
			this.bufferLengthData = new AS400Bin4().toBytes(bufferLength);
			return this;
		}

		public ParameterListBuilder withJournal(String receiver, String receiverLibrary) {
			if (this.receiver != receiver && this.receiverLibrary != receiverLibrary) {
				this.receiver = receiver;
				this.receiverLibrary = receiverLibrary;
				
				String jrnLib = StringHelpers.padRight(receiver, 10) + StringHelpers.padRight(receiverLibrary, 10);
				journalData = new AS400Text(20).toBytes(jrnLib);
			}
			return this;
		}

		public void init() {
			criteria.reset();
		}
		
		public ParameterListBuilder withJournalEntryType(JournalEntryType type) {
			criteria.addEntTyp(new JournalEntryType[] {type});
			return this;
		}

		public ParameterListBuilder withReceivers(String startReceiver, String startLibrary) {
			criteria.addReceiverRange(startReceiver, startLibrary, "*CURRENT", startLibrary);
			return this;
		}
		
		public ParameterListBuilder withReceivers(String startReceiver, String startLibrary, String endReceiver, String endLibrary) {
			criteria.addReceiverRange(startReceiver, startLibrary, endReceiver, endLibrary);
			return this;
		}
		
		public ParameterListBuilder withEnd() {
			criteria.addToEnd();
			return this;
		}

		public ParameterListBuilder withReceivers(String receivers) {
			criteria.addRcvRng(receivers);
			return this;
		}

		public ParameterListBuilder withStartingSequence(BigInteger start) {
			criteria.addFromEnt(start);
			return this;
		}

		public ParameterListBuilder withFromStart() {
			criteria.addFromEnt(RetrievalCriteria.FromEnt.FIRST);
			return this;
		}
		
		public ParameterListBuilder filterJournalCodes(JournalCode[] codes) {
		    criteria.addJrnCde(codes);
		    return this;
		}

	      public ParameterListBuilder filterFiles(List<String> includeFiles) {
	            criteria.addFILE(this.receiverLibrary, includeFiles);
	            return this;
	        }
		
		public ParameterListBuilder filterJournalEntryType(RetrievalCriteria.JournalEntryType[] codes) {
            criteria.addEntTyp(codes);
            return this;
        }
		
		public ProgramParameter[] build() {
			byte[] criteriaData = new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject());
			return new ProgramParameter[] { 
					new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufferLength),
					new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufferLengthData),
					new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, journalData),
					new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, formatNameData),
					new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, criteriaData),
					new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, errorCodeData) };
		}
	}

	
	public long getTotalTransferred() {
	    return totalTransferred;
	}
}
