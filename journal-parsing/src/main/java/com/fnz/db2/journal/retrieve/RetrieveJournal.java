package com.fnz.db2.journal.retrieve;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import com.fnz.db2.journal.retrieve.RetrievalCriteria.JournalEntryType;
import com.fnz.db2.journal.retrieve.exception.InvalidJournalFilterException;
import com.fnz.db2.journal.retrieve.exception.InvalidPositionException;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import com.fnz.db2.journal.retrieve.rjne0200.FirstHeader;
import com.fnz.db2.journal.retrieve.rjne0200.FirstHeaderDecoder;
import com.fnz.db2.journal.retrieve.rjne0200.OffsetStatus;
import com.fnz.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.MessageFile;
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
	
	private RetrieveConfig config;
	private byte[] outputData = null;
	private FirstHeader header = null;
	private EntryHeader entryHeader = null;
	private int offset = -1;
	private JournalPosition position;
	private long totalTransferred = 0;
	private boolean hasMoreData = true;
	
	public RetrieveJournal(RetrieveConfig config) {
		this.config = config;
		builder.withJournal(config.journalInfo().receiver, config.journalInfo().receiverLibrary); 
	}

	
	/**
	 * retrieves a block of journal data
	 * @param retrievePosition
	 * @return true if the journal was read successfully false if there was some problem reading the journal
	 * @throws Exception
	 * 
	 * CURAVLCHN - returns only available journals
	 * CURCHAIN will work though journals that have happened but may no longer be available
	 * if the journal is no longer available we need to capture this and log an error as we may have missed data
	 */
	public boolean retrieveJournal(JournalPosition retrievePosition) throws Exception {
        offset = -1;
        entryHeader = null;
        header = null;

        this.position = retrievePosition;
		this.entryHeader = null;
		log.debug("Fetch journal at postion {}", retrievePosition);
		ServiceProgramCall spc = new ServiceProgramCall(config.as400().connection());
		spc.getServerJob().setLoggingLevel(0);
		builder.init();
		builder.withJournalEntryType(JournalEntryType.ALL);
        if (config.filtering() && !config.includeFiles().isEmpty()) {
            builder.withFileFilters(config.includeFiles());
        }

		if (retrievePosition.isOffsetSet()) {
			builder.withStartingSequence(retrievePosition.getOffset());
		} else {
			builder.withFromStart();
		}
		if (retrievePosition.getJournal().length > 0) {
			builder.withReceivers(retrievePosition.getReciever(), retrievePosition.getReceiverLibrary());
		} else {
			builder.withReceivers("*CURCHAIN"); 
		}
		builder.withBufferLenth(config.journalBufferSize());
		if (config.filtering() && config.filterCodes().length > 0) {
		    builder.filterJournalCodes(REQUIRED_JOURNAL_CODES);
		}
		Optional<JournalPosition> latestJournalPosition = Optional.<JournalPosition>empty();
		if (config.filtering()) {			
			Optional<JournalPosition> endPosition = findEndPosition(config.as400().connection(), retrievePosition);
			endPosition.ifPresent(jp -> {
				builder.withEnd(jp.getOffset());
			});
			if (positionsEqual(retrievePosition, endPosition)) { // we are already at the end
				header = new FirstHeader(0, 0, 0, OffsetStatus.NO_MORE_DATA, endPosition);
				return true;
			}
			latestJournalPosition = endPosition;
		} else {
			builder.withEnd();
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
				log.error("buffer too small skipping this entry {}", retrievePosition);
				header.nextPosition().ifPresent(offset -> {
	                retrievePosition.setPosition(offset); 
	            });
			} 
			if (!hasData()) {
			    log.debug("moving on to current position");
			    latestJournalPosition.ifPresent(l -> {
				    header = header.withCurrentJournalPosition(l);
				    retrievePosition.setPosition(l);
			    });
			}
		} else {
			for (AS400Message id: spc.getMessageList()) {
			    String idt = id.getID();
			    if (idt == null) {
			        log.error("Call failed position {} no Id, message: {}", retrievePosition, id.getText());
			        continue;
			    }
    			switch (idt) {
        			case "CPF7053": { // sequence number does not exist or break in receivers
        			    throw new InvalidPositionException(String.format("Call failed position %s failed to find sequence or break in receivers: %s", retrievePosition, getFullAS400MessageText(id)));
        			}
        			case "CPF9801": { // specify invalid receiver
                        throw new InvalidPositionException(String.format("Call failed position %s failed to find receiver: %s", retrievePosition, getFullAS400MessageText(id)));
        			}
        			case "CPF7054": { // e.g. last < first
        			    throw new InvalidPositionException(String.format("Call failed position %s failed to find offset or invalid offsets: %s", retrievePosition, id.getText()));
        			}
					case "CPF7060": { // object in filter doesn't exist, or was not journaled 
						throw new InvalidJournalFilterException(
        			    	String.format("Call failed position %s object not found or not journaled: %s", retrievePosition, getFullAS400MessageText(id))
						);
        			}
        			case "CPF7062": { 
        			    log.debug("Call failed position {} no data received, probably all filtered: {}", retrievePosition, id.getText());
        			 // if we're filtering we get no continuation offset just an error
        		        header = new FirstHeader(0, 0, 0, OffsetStatus.NO_MORE_DATA, latestJournalPosition);
        			    latestJournalPosition.ifPresent(l -> { 
        				    header = header.withCurrentJournalPosition(l);
        				    retrievePosition.setPosition(l);
        			    });
        			    return true;
        			}
                    default: 
                        log.error("Call failed position {} with error code {} message {}", retrievePosition, idt, getFullAS400MessageText(id));                        
    			}
			}
			
			throw new Exception(String.format("Call failed position %s", retrievePosition));
		}
		return success;
	}


	private boolean positionsEqual(JournalPosition retrievePosition, Optional<JournalPosition> endPosition) {
		return endPosition.isPresent() && endPosition.get().equals(retrievePosition);
	}

	private Optional<JournalPosition> findEndPosition(AS400 as400, JournalPosition position) throws Exception {
		if (position.getOffset().compareTo(BigInteger.ZERO)>0) {
	        JournalPosition endPosition = JournalInfoRetrieval.getCurrentPosition(as400, config.journalInfo());
	        
	        JournalPosition p = limitEndPosition(position, position.getOffset(), endPosition.getOffset());
			builder.withEnd(p.getOffset());
			return Optional.of(p);
		} else {
			List<DetailedJournalReceiver> receivers = JournalInfoRetrieval.getReceivers(as400, config.journalInfo());
			Optional<DetailedJournalReceiver> first = DetailedJournalReceiver.firstInLatestChain(receivers);
			Optional<DetailedJournalReceiver> last = DetailedJournalReceiver.latest(receivers);
			return first.flatMap(f -> {
				return last.map(l -> {
					return limitEndPosition(position, f.start(), l.end());				        
				});
			});
		}
	}
	
	private JournalPosition limitEndPosition(JournalPosition position, BigInteger start,
			BigInteger end) {
		BigInteger nextBlock = start.add(BigInteger.valueOf(config.maxServerSideEntries()));
		if (nextBlock.compareTo(end) > 0) { // don't beyond end
			nextBlock = end;
		}

		JournalPosition p = new JournalPosition(position).setOffset(nextBlock, true);
		return p;
	}

	private String getFullAS400MessageText(AS400Message message)
	{
		try {
			message.load(MessageFile.RETURN_FORMATTING_CHARACTERS);
			return message.getText() + " " + message.getHelp();
		}
		catch(Exception e) {
			return message.getText();
		}
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
		if (header.status() == OffsetStatus.NO_MORE_DATA) {
			return false;
		}
	    if (offset < 0 && header.size() > 0) {
	        return true;
	    }
	    if (offset > 0 && entryHeader.getNextEntryOffset() > 0) {
	    	return true;
	    }
	    return false;
	}
	
	public boolean nextEntry() {
		if (offset < 0) {
			if (header.size() > 0) {
				offset=header.offset();
				entryHeader = entryHeaderDecoder.decode(outputData, offset);
				if (alreadyProcessed(position, entryHeader)) {
					updatePosition(position, entryHeader);
					return nextEntry();
				}
				updatePosition(position, entryHeader);
				return true;
			} else {
				return false;
			}
		} else {
			long nextOffset = entryHeader.getNextEntryOffset();
			if (nextOffset > 0) {
				offset += (int)nextOffset;
				entryHeader = entryHeaderDecoder.decode(outputData, offset);
				updatePosition(position, entryHeader);
				return true;
			}
			
			updateOffsetFromContinuation();
			return false;
		}
	}

	private void updateOffsetFromContinuation() {
		// after we hit the end use the continuation header for the next offset
		header.nextPosition().ifPresent(offset -> {
		    log.debug("Setting confinuation offset {}", offset);
			position.setPosition(offset); 
		});
	}
	
	private static boolean alreadyProcessed(JournalPosition position, EntryHeader entryHeader) {
		JournalPosition entryPosition = new JournalPosition(position);
		updatePosition(entryPosition, entryHeader);
		return position.processed() && entryPosition.equals(position);
		
	}

	private static void updatePosition(JournalPosition p, EntryHeader entryHeader) {
		if (entryHeader.hasReceiver()) {
			p.setJournalReciever(entryHeader.getSequenceNumber(), entryHeader.getReceiver(), entryHeader.getReceiverLibrary(), true);
		} else {
			p.setOffset(entryHeader.getSequenceNumber(), true);
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
			dumpEntryToFile(config.dumpFolder());
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

	// TODO remove now we've sanitised RetrievalCriteria
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
			criteria.withLenNullPointerIndicatorVarLength();
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
			criteria.withEntTyp(new JournalEntryType[] {type});
			return this;
		}

		public ParameterListBuilder withReceivers(String startReceiver, String startLibrary) {
			criteria.withReceiverRange(startReceiver, startLibrary, "*CURRENT", startLibrary);
			return this;
		}
		
		public ParameterListBuilder withReceivers(String startReceiver, String startLibrary, String endReceiver, String endLibrary) {
			criteria.withReceiverRange(startReceiver, startLibrary, endReceiver, endLibrary);
			return this;
		}
		
		public ParameterListBuilder withEnd() {
			criteria.withEnd();
			return this;
		}
		
		public ParameterListBuilder withEnd(BigInteger end) {
			criteria.withEnd(end);
			return this;
		}
		
		public ParameterListBuilder withReceivers(String receivers) {
			criteria.withReceiverRange(receivers);
			return this;
		}

		public ParameterListBuilder withStartingSequence(BigInteger start) {
			criteria.withFromEnt(start);
			return this;
		}

		public ParameterListBuilder withFromStart() {
			criteria.withFromEnt(RetrievalCriteria.FromEnt.FIRST);
			return this;
		}
		
		public ParameterListBuilder filterJournalCodes(JournalCode[] codes) {
		    criteria.withJrnCde(codes);
		    return this;
		}

		public ParameterListBuilder withFileFilters(List<FileFilter> tableFilters) {
			criteria.withFILE(tableFilters);
	        return this;
	    }
		
		public ParameterListBuilder filterJournalEntryType(RetrievalCriteria.JournalEntryType[] codes) {
            criteria.withEntTyp(codes);
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
