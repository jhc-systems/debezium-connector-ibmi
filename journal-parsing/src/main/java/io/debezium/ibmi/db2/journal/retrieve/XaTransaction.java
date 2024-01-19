/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

public class XaTransaction {

    private long dat;
    private long seq;
    private String dta;

    public XaTransaction(long dat, long seq, String dta) {
        super();
        this.dat = dat;
        this.seq = seq;
        this.dta = dta;
    }

}
