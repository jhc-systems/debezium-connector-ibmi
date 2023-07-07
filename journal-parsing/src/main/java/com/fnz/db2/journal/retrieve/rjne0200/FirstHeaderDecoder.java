package com.fnz.db2.journal.retrieve.rjne0200;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Optional;

import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.JournalReceiver;
import com.fnz.db2.journal.retrieve.StringHelpers;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

public class FirstHeaderDecoder {
    private final static AS400Structure structure;
	static {
	    ArrayList<AS400DataType> dataTypes = new ArrayList<AS400DataType>();
		FieldDescription[] fds = new FieldDescription[] {
				new BinaryFieldDescription(new AS400Bin4(), "0 bytes returned"),
				new BinaryFieldDescription(new AS400Bin4(), "1 offset to first journal entry"),
				new BinaryFieldDescription(new AS400Bin4(), "2 number of entries retrieved"),
				new CharacterFieldDescription(new AS400Text(1), "3 continuation indicator"),
				new CharacterFieldDescription(new AS400Text(10), "4 continuation starting receiver"),
				new CharacterFieldDescription(new AS400Text(10), "5 continuation starting receiver library"),
				new CharacterFieldDescription(new AS400Text(20), "6 continutation starting sequence number"),
				//new CharacterFieldDescription(new AS400Text(11), "7 reserved"), 
				};
	    for (int i = 0; i < fds.length; i++) {
	        dataTypes.add(fds[i].getDataType());
	    }
	    structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
	}

	public FirstHeader decode(byte[] data) {
	    Object[] os = (Object[]) structure.toObject(data);
	    
	    Optional<JournalPosition> pos = Optional.<JournalPosition>empty();
	    OffsetStatus status = OffsetStatus.NO_MORE_DATA;
	    if ("1".equals((String)os[3])) {
	        status = OffsetStatus.MORE_DATA_NEW_OFFSET;
	        String receiver = StringHelpers.safeTrim((String)os[4]); 
	        String library = StringHelpers.safeTrim((String)os[5]);
	        String offsetStr = StringHelpers.safeTrim((String)os[6]);
	        BigInteger offset = new BigInteger(offsetStr);
	        pos = Optional.of(new JournalPosition(offset, new JournalReceiver(receiver, library)));
	    }
	    
	    return new FirstHeader(
	            (Integer)os[0], 
	            (Integer)os[1], 
	            (Integer)os[2], 
	            status, 
	            pos);
	}
}
