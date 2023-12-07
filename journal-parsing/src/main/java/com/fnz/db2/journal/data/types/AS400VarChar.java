package com.fnz.db2.journal.data.types;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.Trace;

public class AS400VarChar implements AS400DataType {
	private static final AS400Bin2 AS400_BIN2 = new AS400Bin2();
	private static final AS400Bin4 AS400_BIN4 = new AS400Bin4();
	private final int maxLenght;
	private final static String defaultValue = "";
	private int actualLength;
	private final int ccsid;
	private final int bytesPerChar;

	public AS400VarChar(int maxLenght, int bytesPerChar) {
		this.maxLenght = maxLenght * bytesPerChar;
		this.bytesPerChar = bytesPerChar;
		ccsid = -1;
	}

	public AS400VarChar(int maxLenght, int bytesPerChar, int ccsid) {
		this.ccsid = ccsid;
		this.bytesPerChar = bytesPerChar;
		this.maxLenght = maxLenght * bytesPerChar;
	}

	@Override
	public int getByteLength() {
		return maxLenght + 2;
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

	@Override
	public Object toObject(byte[] data, int offset) {
		final int lenOffset = 2;
		actualLength = (Short)AS400_BIN2.toObject(data, offset);
		actualLength *= bytesPerChar;
		final AS400Text txt = (ccsid > 0) ? new AS400Text(actualLength, ccsid) : new AS400Text(actualLength);
		final String text = (String) txt.toObject(data, offset + lenOffset);
		return text;
	}

	@Override
	public Object clone() {
		try
		{
			return super.clone();  // Object.clone does not throw exception.
		}
		catch (final CloneNotSupportedException e)
		{
			Trace.log(Trace.ERROR, "Unexpected CloneNotSupportedException:", e);
			throw new InternalErrorException(InternalErrorException.UNEXPECTED_EXCEPTION);
		}
	}

}
