package com.fnz.db2.journal.retrieve.rjne0200;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

public class ReceiverNameDecoder {
    private final static AS400Structure structure;
    
	static {
	    ArrayList<AS400DataType> dataTypes = new ArrayList<AS400DataType>();
	    AS400Timestamp timeType = new AS400Timestamp();
	    try {
	    	Field privateDTSFormat = AS400Timestamp.class.getDeclaredField("FORMAT_DTS");
	    	privateDTSFormat.setAccessible(true);
	    	int dtsformat = (int) privateDTSFormat.get(timeType);
		    Method privateSetFormat = AS400Timestamp.class.getDeclaredMethod("setFormat", int.class);
		    privateSetFormat.setAccessible(true);
		    privateSetFormat.invoke(timeType, dtsformat);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	    
		FieldDescription[] fds = new FieldDescription[] {
			      new CharacterFieldDescription(new AS400Text(10), "0 receiver name"),
			      new CharacterFieldDescription(new AS400Text(10), "1 library name"),
			      new CharacterFieldDescription(new AS400Text(10), "2 ASP device name"),
			      new BinaryFieldDescription(new AS400Bin2(), "3 ASP number")
		};
	    for (int i = 0; i < fds.length; i++) {
	        dataTypes.add(fds[i].getDataType());
	    }
	    structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
	}
	public String[] decode(byte[] data, int offset) {
	    Object[] os = (Object[]) structure.toObject(data, offset);
	    String receiver = (String)os[0];
	    String library = (String)os[1];
	    return new String[] { receiver, library };
	}
	
}
