/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;
import java.util.GregorianCalendar;

import javax.annotation.processing.Generated;

import org.junit.jupiter.api.Test;

@Generated(value = "org.junit-tools-1.1.0")
public class ReceiverDecoderTest {

    @Test
    public void testToDate() throws Exception {
        Date date = ReceiverDecoder.toDate("1180220083847");

        // constructor uses 0 offset for months
        GregorianCalendar c = new GregorianCalendar(2018, 1, 20, 8, 38, 47);
        assertEquals(c.toZonedDateTime().toInstant(), date.toInstant());
    }
}
