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
import java.util.Collections;
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

    private final static JournalCode[] REQUIRED_JOURNAL_CODES = new JournalCode[] {
    		JournalCode.D, JournalCode.R, JournalCode.C
    		};
    private final static JournalEntryType[] REQURED_ENTRY_TYPES = new JournalEntryType[] {       
            JournalEntryType.PT, JournalEntryType.PX, JournalEntryType.UP, JournalEntryType.UB, 
            JournalEntryType.DL, JournalEntryType.DR, JournalEntryType.CT, JournalEntryType.CG, 
            JournalEntryType.SC, JournalEntryType.CM
    };
    private final static FirstHeaderDecoder firstHeaderDecoder = new FirstHeaderDecoder();
    private final static EntryHeaderDecoder entryHeaderDecoder = new EntryHeaderDecoder();
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyMMdd-hhmm");
    
	private ParameterListBuilder builder = new ParameterListBuilder();
	
	RetrieveConfig config;
	private byte[] outputData = null;
	private FirstHeader header = null;
	private EntryHeader entryHeader = null;
	private int offset = -1;
	private JournalPosition position;
	private long totalTransferred = 0;
	private JournalInfoRetrieval journalInfoRetrieval;
	
	public RetrieveJournal(RetrieveConfig config, JournalInfoRetrieval journalRetrieval) {
		this.config = config;
		this.journalInfoRetrieval = journalRetrieval;
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

        Optional<JournalPosition> latestJournalPosition = Optional.empty();
		Optional<PositionRange> range = findRange(config.as400().connection(), retrievePosition);
		if (range.isEmpty()) {
			if (retrievePosition.isOffsetSet()) {
				builder.withStartingSequence(retrievePosition.getOffset());
			} else {
				builder.withFromStart();
			}
			builder.withReceivers("*CURCHAIN");
			builder.withEnd();
		} else {
			PositionRange r = range.get();
			builder.withStartingSequence(r.start.getOffset());
			builder.withReceivers("*CURCHAIN");
			builder.withEnd(r.end.getOffset());
			
			if (retrievePosition.equals(r.end)) { // we are already at the end
				header = new FirstHeader(0, 0, 0, OffsetStatus.NO_MORE_DATA, Optional.of(r.end));
				return true;
			}
			latestJournalPosition = Optional.of(r.end);
		}

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
			    log.debug("moving on to current position {}", latestJournalPosition);
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

	record PositionRange(JournalPosition start, JournalPosition end) {};

	List<DetailedJournalReceiver> cachedReceivers = Collections.emptyList();
	DetailedJournalReceiver cachedCurrentPosition = null;
	
	Optional<PositionRange> findRange(AS400 as400, JournalPosition start) throws Exception {
        if (!shouldLimitRange()) {
        	return Optional.empty();
        }
		BigInteger maxPosition = start.getOffset().add(BigInteger.valueOf(config.maxServerSideEntries()));
        boolean startValid = start.isOffsetSet() && !start.getOffset().equals(BigInteger.ZERO);
		if (startValid) {
			DetailedJournalReceiver currentPosition = cachedCurrentPosition;
			if (cachedCurrentPosition == null || maxPosition.compareTo(cachedCurrentPosition.end()) >= 0 ) {
				currentPosition = journalInfoRetrieval.getCurrentDetailedJournalReceiver(as400, config.journalInfo());
				cachedCurrentPosition = currentPosition;
				// can't go beyond current journal end
				if (maxPosition.compareTo(currentPosition.end()) >= 0) {
					maxPosition = currentPosition.end();
					JournalPosition end = new JournalPosition(maxPosition, currentPosition.info().name(), currentPosition.info().library(), true);
					return Optional.of(new PositionRange(start, end));
				}
			}
	        if (withinRange(maxPosition, currentPosition.start(), currentPosition.end())) {
	        	JournalPosition end = new JournalPosition(maxPosition, currentPosition.info().name(), currentPosition.info().library(), true);
	        	return Optional.of(new PositionRange(start, end));
	        }
		}
		Optional<PositionRange> fromCache = findInReceivers(start, maxPosition, startValid, cachedReceivers);
		if (fromCache.isPresent())
			return fromCache;
		
		List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(as400, config.journalInfo());
		cachedReceivers = receivers;
		return findInReceivers(start, maxPosition, startValid, receivers);
	}


	private Optional<PositionRange> findInReceivers(JournalPosition start, BigInteger maxPosition, boolean startValid,
			List<DetailedJournalReceiver> receivers) {
		if (!startValid) {
			Optional<DetailedJournalReceiver> first = DetailedJournalReceiver.firstInLatestChain(receivers);
			Optional<JournalPosition> startOpt = first.map(x -> new JournalPosition(x.start(), x.info().name(), x.info().library(), false));
			if (startOpt.isPresent()) {
				start = startOpt.get();
				maxPosition = start.getOffset().add(BigInteger.valueOf(config.maxServerSideEntries()));
			} else {
				return Optional.empty();
			}
		}
		Optional<JournalPosition> end = journalAtMaxOffset(maxPosition, receivers);
		if (end.isPresent())
			return Optional.of(new PositionRange(start, end.get()));
		else
			return Optional.empty();
	}
	
	boolean withinRange(BigInteger desiredPosition, BigInteger startPosition, BigInteger endPosition) {
		return startPosition.compareTo(desiredPosition) <= 0 && endPosition.compareTo(desiredPosition) >= 0;
	}
	
	boolean shouldLimitRange() {
		return config.filtering();
	}
	
	// returns journal within range of the max offset
	Optional<JournalPosition> journalAtMaxOffset(BigInteger maxOffset, List<DetailedJournalReceiver> receivers) {
		Optional<DetailedJournalReceiver> found = receivers.stream().filter(p -> {
			return withinRange(maxOffset, p.start(), p.end());
		}).findFirst();
		return found.map(p -> new JournalPosition(maxOffset, p.info().name(), p.info().library(), true));
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
				
				String formattedDate = DATE_FORMATTER.format( new Date() );
				File f = new File(path, String.format("%s-%s", formattedDate, Integer.toString(i)));
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
			if ( ! this.receiver.equals(receiver) && ! this.receiverLibrary.equals(receiverLibrary)) {
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
