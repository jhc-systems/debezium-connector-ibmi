package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.ibm.as400.access.AS400DataType;

public class JdbcFileDecoderTest {

	private JdbcFileDecoder createTestSubject() {
		return new JdbcFileDecoder(null, null, new SchemaCacheHash());
	}

	@Test
	public void testToDataType() throws Exception {
		AS400DataType passwordNoLength = JdbcFileDecoder.toDataType("password", "CHAR () FOR BIT DATA", 10, 0);
		assertEquals(9, passwordNoLength.getInstanceType());
		AS400DataType passwordLength = JdbcFileDecoder.toDataType("password", "CHAR (20) FOR BIT DATA", 10, 0);
		assertEquals(9, passwordLength.getInstanceType());
		assertEquals(20, passwordLength.getByteLength());
	}
}