package com.fnz.db2.journal.data.types;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Bin8;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;
import com.ibm.as400.access.Trace;

public class AS400Clob implements AS400DataType {
    private final static Logger log = LoggerFactory.getLogger(AS400Clob.class);
	private final static String defaultValue = "";
	private AS400 as400;
	
	public AS400Clob(AS400 as400) {
		this.as400 = as400;
	}

	@Override
	public int getByteLength() {
		return 44;
	}

	@Override
	public Object getDefaultValue() {
		return defaultValue;
	}

	@Override
	public int getInstanceType() {
		return AS400DataType.TYPE_TEXT;
	}

	@Override
	public Class<?> getJavaType() {
		return String.class;
	}

	@Override
	public byte[] toBytes(Object javaValue) {
		return null;
	}

	@Override
	public int toBytes(Object javaValue, byte[] as400Value) {
		return 0;
	}

	@Override
	public int toBytes(Object javaValue, byte[] as400Value, int offset) {
		return 0;
	}

	@Override
	public Object toObject(byte[] data) {
		return toObject(data, 0);
	}
	
	
	/**
	 * TODO clob support not working
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORRCVI.htm
	 * https://www.ibm.com/support/knowledgecenter/en/ssw_ibm_i_72/sqlp/rbafylobjournal.htm
	 * @param as400
	 * @return
	 * @throws Exception
	 */
	private String getClob(long addressHigh, long addressLow, byte[] addressbyte, int length)
			throws Exception {
		
		byte[] hb = new AS400Bin8().toBytes(addressHigh);
		byte[] lb = new AS400Bin8().toBytes(addressLow);
		byte[] address16 = new byte[16];
		System.arraycopy(hb, 0, address16, 0, 8);
		System.arraycopy(lb, 0, address16, 8, 8);
		log.debug("addresss re-encoded " + toHex(address16));
		log.debug("address raw " + toHex(addressbyte));
		
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, length+2),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, addressbyte),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(length)),
				};
		
		
		ServiceProgramCall spc = new ServiceProgramCall(as400);

		spc.setProgram("/QSYS.LIB/MARTINT.LIB/RTVPTRDTA.SRVPGM", parameters);
		spc.setProcedureName("GETDATA");
		spc.setAlignOn16Bytes(true);
		spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
//		log.debug("pre job: " + spc.getServerJob());
		boolean success = spc.run();
//		log.debug("post job: " + spc.getServerJob());
		if (success) {
			byte[] data = parameters[0].getOutputData();
			int retrivedLength = (Integer)new AS400Bin4().toObject(data, 0);
			byte[] btext = Arrays.copyOfRange(data, 2, data.length);
			String text = new String(btext);
			log.debug(text);
			return text;
		} else {
			String msg = "call failed " + Arrays.asList(spc.getMessageList()).stream().map(x -> x.getText()).reduce("", (a, s) -> a + s);
			log.debug(msg);
			throw new Exception(msg);
		}
	}
	
	private String deleteClob(long addressHigh, long addressLow, byte[] addressbyte)
			throws Exception {
		
		byte[] hb = new AS400Bin8().toBytes(addressHigh);
		byte[] lb = new AS400Bin8().toBytes(addressLow);
		byte[] address16 = new byte[16];
		System.arraycopy(hb, 0, address16, 0, 8);
		System.arraycopy(lb, 0, address16, 8, 8);
		log.debug("addresss re-encoded " + toHex(address16));
		log.debug("address raw " + toHex(addressbyte));
		
		
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(Long.valueOf(addressLow).intValue())),
				};
		
		
		ServiceProgramCall spc = new ServiceProgramCall(as400);

		spc.setProgram("/QSYS.LIB/QJOURNAL.SRVPGM", parameters);
		spc.setProcedureName("QjoDeletePointerHandle");
		spc.setAlignOn16Bytes(true);
		spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
		log.debug("pre job: " + spc.getServerJob());
		boolean success = spc.run();
		log.debug("post job: " + spc.getServerJob());
		if (success) {
			return "";
		} else {
			String msg = "call failed " + Arrays.asList(spc.getMessageList()).stream().map(x -> x.getText()).reduce("", (a, s) -> a + s);
			log.debug(msg);
			throw new Exception(msg);
		}
	}


	public String toHex(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
		}
        return sb.toString();
	}

	@Override
	public Object toObject(byte[] data, int offset) {
		// padding to align pointer relative offset 13 to be 16 byte aligned
		int alignment = (16 - ((offset+13)%16));
		// 1 byte of system info (set to 0x00) + alignment
		int relativeOffset = offset + 1 + alignment;
		int length = (Integer)new AS400Bin4().toObject(data, relativeOffset);
		long first = (Long)new AS400Bin8().toObject(data, relativeOffset+12);
		long second = (Long)new AS400Bin8().toObject(data, relativeOffset+20);
		// release handle: 	QjoDeletePointerHandle
		
		log.debug("alignment " + alignment);
		log.debug("alignment zero " + (relativeOffset+12)%16);
		log.debug("length " + length);
		log.debug("first " +first);
		log.debug("second " + second);
//		log.debug("data length " + data.length);
		log.debug(Diagnostics.binAsHex(data, offset, 44));
		
		byte[] addressbyte = Arrays.copyOfRange(data, relativeOffset+12, relativeOffset+28);
		
		
		try {
			log.debug(getClob(first, second, addressbyte, length));
//			log.debug(deleteClob(first, second, addressbyte));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return "";
	}

	@Override
	public Object clone() {
        try
        {
            return super.clone();  // Object.clone does not throw exception.
        }
        catch (CloneNotSupportedException e)
        {
            Trace.log(Trace.ERROR, "Unexpected CloneNotSupportedException:", e);
            throw new InternalErrorException(InternalErrorException.UNEXPECTED_EXCEPTION);
        }
	}

}
