/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class StringHelpersTest {

    @Test
    void testPadRightExactLength() {
        String unpadded = StringHelpers.padRight("1234567890", 10);
        assertEquals("1234567890", unpadded);
    }

    @Test
    void testPadRighTooLong() {
        String unpadded = StringHelpers.padRight("12345", 4);
        assertEquals("1234", unpadded);
    }

    @Test
    void testPadRightShort() {
        String unpadded = StringHelpers.padRight("12", 4);
        assertEquals("12  ", unpadded);
    }

    @Test
    void testPadLeftExactLength() {
        String unpadded = StringHelpers.padLeft("1234567890", 10);
        assertEquals("1234567890", unpadded);
    }

    @Test
    void testPadLeftTooLong() {
        String unpadded = StringHelpers.padLeft("12345", 4);
        assertEquals("1234", unpadded);
    }

    @Test
    void testPadLeftShort() {
        String unpadded = StringHelpers.padLeft("12", 4);
        assertEquals("  12", unpadded);
    }
}
