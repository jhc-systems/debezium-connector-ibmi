/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.ibm.as400.access.AS400DataType;

public class JdbcFileDecoderTest {

    @Test
    public void testToDataType() throws Exception {
        final JdbcFileDecoder decoder = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1, -1);
        final AS400DataType passwordNoLength = decoder.toDataType("schem", "table", "password", "CHAR () FOR BIT DATA",
                10, 0);
        assertEquals(AS400DataType.TYPE_BYTE_ARRAY, passwordNoLength.getInstanceType());
        final AS400DataType passwordLength = decoder.toDataType("schem", "table", "password", "CHAR (20) FOR BIT DATA",
                10, 0);
        assertEquals(AS400DataType.TYPE_BYTE_ARRAY, passwordLength.getInstanceType());
        assertEquals(20, passwordLength.getByteLength());
        final AS400DataType varPasswordNoLength = decoder.toDataType("schem", "table", "password",
                "VARCHAR () FOR BIT DATA", 10, 0);
        assertEquals(-1, varPasswordNoLength.getInstanceType());
        final AS400DataType varPasswordLength = decoder.toDataType("schem", "table", "password",
                "VARCHAR (20) FOR BIT DATA", 10, 0);
        assertEquals(-1, varPasswordLength.getInstanceType());
        assertEquals(20, passwordLength.getByteLength());
    }
}
