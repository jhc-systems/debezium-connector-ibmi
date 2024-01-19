/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import java.awt.event.KeyEvent;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Text;

public class Diagnostics {
    private static final Logger log = LoggerFactory.getLogger(Diagnostics.class);

    public static boolean isPrintableChar(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(c);
        return (!Character.isISOControl(c)) && c != KeyEvent.CHAR_UNDEFINED && block != null
                && block != Character.UnicodeBlock.SPECIALS;
    }

    public static void dumpToFile(String filename, byte[] data, int offset, int length) throws IOException {
        byte[] bdata = Arrays.copyOfRange(data, offset, offset + length);
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            fos.write(bdata);
        }
    }

    public static String binAsHex(byte[] data, int offset, int length) {
        // byte[] bdata = Arrays.copyOfRange(data, offset, offset + length);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%04d: ", (offset)));
        for (int j = 0; j < length; j++) {
            byte b = data[j + offset];
            sb.append(String.format("%02x ", b));
            if ((j + 1) % 80 == 0) {
                sb.append("\n");
                sb.append(String.format("%04d: ", (j + offset)));
            }
        }
        return sb.toString();
    }

    public static String binAsAscii(byte[] data, int offset, int length) {
        byte[] bdata = Arrays.copyOfRange(data, offset, offset + length);
        StringBuilder sb = new StringBuilder("\n");
        int i = 0;
        for (byte b : bdata) {
            char c = (char) b;
            if (isPrintableChar(c)) {
                sb.append(c);
            }
            else {
                sb.append(".");
            }
            if (++i % 80 == 0) {
                sb.append("\n");
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    public static String binAsEbcdic(byte[] data, int offset, int length) {
        // byte[] bdata = Arrays.copyOfRange(data, offset, offset + length);
        StringBuilder sb = new StringBuilder();
        AS400Text td = new AS400Text(1);
        sb.append(String.format("%04d: ", (offset)));
        for (int j = 0; j < length; j++) {
            String s = (String) td.toObject(data, j + offset);
            if (isPrintableChar(s.charAt(0))) {
                sb.append(s);
            }
            else {
                sb.append(".");
            }
            if ((j + 1) % 80 == 0) {
                sb.append("\n");
                sb.append(String.format("%04d: ", (j + offset)));
            }
        }
        sb.append("\n");
        return sb.toString();
    }
}
