package com.fnz.db2.journal.retrieve.rnrn0200;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.fnz.db2.journal.retrieve.StringHelpers;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

// https://www.ibm.com/docs/en/i/7.2?topic=ssw_ibm_i_72/apis/QJORJRNI.htm "Key 1 output section"
public class ReceiverDecoder {
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
			      new CharacterFieldDescription(new AS400Text(5), "2 receiver number"),
			      new CharacterFieldDescription(new AS400Text(13), "3 attach date and time"),

			      new CharacterFieldDescription(new AS400Text(1), "4 status"),
			      new CharacterFieldDescription(new AS400Text(13), "5 save date and time"),
			      new CharacterFieldDescription(new AS400Text(8), "6 local journal system"),
			      new CharacterFieldDescription(new AS400Text(8), "7 source journal system"),
			      new BinaryFieldDescription(new AS400Bin4(), "8 receiver size")
			      // reserved char(56)
		};
	    for (int i = 0; i < fds.length; i++) {
	        dataTypes.add(fds[i].getDataType());
	    }
	    structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
	}
	public JournalReceiverInfo decode(byte[] data, int offset) {
	    Object[] os = (Object[]) structure.toObject(data, offset);
	    String itime =  (String)os[3];
	    Date attached = toDate(itime);
	    JournalReceiverInfo jr = new JournalReceiverInfo(
	            StringHelpers.safeTrim((String)os[0]), 
	            StringHelpers.safeTrim((String)os[1]), 
	            attached, 
	            JournalStatus.valueOfString((String)os[4]));
        return jr;
	}
	static Date toDate(String itime) {
		try {
			int century = 19 + Integer.valueOf(itime.substring(0,1));
			String stime = String.format("%d%s", century, itime.substring(1));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			Date attached = sdf.parse(stime);
			return attached;
		} catch (ParseException e) {
			return null;
		}
	}
	
}
