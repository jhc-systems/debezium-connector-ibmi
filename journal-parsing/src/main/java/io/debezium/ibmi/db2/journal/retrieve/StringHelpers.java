/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringHelpers {
    private StringHelpers() {
    }

    private static Logger log = LoggerFactory.getLogger(StringHelpers.class);

    /**
     * After the program run, invoke getReceiver() to get the instance of the Receiver, which will provide the returning result.
     * @return
     */

    /**
     * Pad space to the left of the input string s, so that the length of s is n.
     * @param s
     * @param n
     * @return
     */
    public static String padLeft(String s, int n) {
        if (s.length() > n) {
            log.error("String '{}' longer than padded length {} truncating", s, n, new IllegalArgumentException("Too long"));
        }
        return String.format(String.format("%%1$%ds", n), s).substring(0, n);
    }

    /**
     * Pad space to the right of the input string s, so that the length of s is n.
     * @param s
     * @param n
     * @return
     */
    public static String padRight(String s, int n) {
        if (s.length() > n) {
            log.error("String '{}' longer than padded length {} truncating", s, n, new IllegalArgumentException("Too long"));
        }
        return String.format(String.format("%%1$-%ds", n), s).substring(0, n);
    }

    public static String safeTrim(String s) {
        if (s == null) {
            return s;
        }
        return s.trim();
    }
}
