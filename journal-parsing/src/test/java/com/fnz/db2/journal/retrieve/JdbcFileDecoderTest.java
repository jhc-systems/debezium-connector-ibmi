package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.ibm.as400.access.AS400DataType;

public class JdbcFileDecoderTest {


	@Test
	public void testToDataType() throws Exception {
		JdbcFileDecoder decoder = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1);;
		AS400DataType passwordNoLength = decoder.toDataType("schem", "table", "password", "CHAR () FOR BIT DATA", 10, 0);
		assertEquals(AS400DataType.TYPE_BYTE_ARRAY, passwordNoLength.getInstanceType());
		AS400DataType passwordLength = decoder.toDataType("schem", "table", "password", "CHAR (20) FOR BIT DATA", 10, 0);
		assertEquals(AS400DataType.TYPE_BYTE_ARRAY, passwordLength.getInstanceType());
		assertEquals(20, passwordLength.getByteLength());
		AS400DataType varPasswordNoLength = decoder.toDataType("schem", "table", "password", "VARCHAR () FOR BIT DATA", 10, 0);
		assertEquals(-1, varPasswordNoLength.getInstanceType());
		AS400DataType varPasswordLength = decoder.toDataType("schem", "table", "password", "VARCHAR (20) FOR BIT DATA", 10, 0);
		assertEquals(-1, varPasswordLength.getInstanceType());
		assertEquals(20, passwordLength.getByteLength());
	}
}