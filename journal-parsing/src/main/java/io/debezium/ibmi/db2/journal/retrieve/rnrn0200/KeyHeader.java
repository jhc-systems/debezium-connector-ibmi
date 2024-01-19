/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

public class KeyHeader {
    int key;
    int offset;
    int lengthOfHeader;
    int numberOfEntries;
    int lengthOfKeyInfo;

    public KeyHeader(int key, int offset, int lengthOfHeader, int numberOfEntries, int lengthOfKeyInfo) {
        super();
        this.key = key;
        this.offset = offset;
        this.lengthOfHeader = lengthOfHeader;
        this.numberOfEntries = numberOfEntries;
        this.lengthOfKeyInfo = lengthOfKeyInfo;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("KeyHeader [key=");
        builder.append(key);
        builder.append(", offset=");
        builder.append(offset);
        builder.append(", lengthOfHeader=");
        builder.append(lengthOfHeader);
        builder.append(", numberOfEntries=");
        builder.append(numberOfEntries);
        builder.append(", lengthOfKeyInfo=");
        builder.append(lengthOfKeyInfo);
        builder.append("]");
        return builder.toString();
    }

    public int getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

    public int getLengthOfHeader() {
        return lengthOfHeader;
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    public int getLengthOfKeyInfo() {
        return lengthOfKeyInfo;
    }

}
