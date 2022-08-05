package com.fnz.db2.journal.retrieve;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400UnsignedBin4;


/**
if *VARLEN specified for null value indicators
0    0   BINARY(4)   Length of null value indicators
4   4   CHAR(*)     Null value indicators
 */
public class NullIndicatorDecoder implements JournalEntryDeocder<List<String>> {
    private static final Logger log = LoggerFactory.getLogger(NullIndicatorDecoder.class);

	@Override
	public List<String> decode(EntryHeader entryHeader, byte[] data, int offset) throws Exception {
		AS400UnsignedBin4 lengthDecoder = new AS400UnsignedBin4();
		int nullOffset = entryHeader.getNullValueOffest();
		int entryOffset = entryHeader.getEntrySpecificDataOffset();
		List<String> l = new ArrayList<>();     
		log.info("null offset {}", nullOffset);

		int pos = offset+nullOffset;
		Long length = (Long)lengthDecoder.toObject(data, pos);
		log.info("found length " + length);
		AS400Text textDecoder = new AS400Text(length.intValue());
		pos += 4;
		String nullField = (String)textDecoder.toObject(data, pos);
		l.add(nullField);		
		return l;
	}

}
