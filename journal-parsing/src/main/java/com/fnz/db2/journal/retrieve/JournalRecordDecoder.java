/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import java.util.ArrayList;

import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

public class JournalRecordDecoder implements JournalEntryDeocder<JournalReceiver> {
	static final AS400Structure structure;
	static {
		FieldDescription[] fds = new FieldDescription[] {
				new CharacterFieldDescription(new AS400Text(10), "start journal"),
				new CharacterFieldDescription(new AS400Text(10), "start lib"),
				new CharacterFieldDescription(new AS400Text(10), "end journal"),
				new CharacterFieldDescription(new AS400Text(10), "end lib")
		};
	    ArrayList<AS400DataType> dataTypes = new ArrayList<AS400DataType>();
	    for (int i = 0; i < fds.length; i++) {
	        dataTypes.add(fds[i].getDataType());
	    }
	    structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

	@Override
	public JournalReceiver decode(EntryHeader entryHeader, byte[] data, int offset) throws Exception {
	    Object[] os = (Object[]) structure.toObject(data, offset +  ENTRY_SPECIFIC_DATA_OFFSET + entryHeader.getEntrySpecificDataOffset());
	    JournalReceiver start = new JournalReceiver((String)os[0], (String)os[1]);
//	    JournalReciever end = new JournalReciever((String)os[2], (String)os[3]);
		return start;
	}

}
