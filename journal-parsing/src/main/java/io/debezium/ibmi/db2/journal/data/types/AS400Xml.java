/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.Trace;

public class AS400Xml implements AS400DataType {
    private final static String defaultValue = "";

    public AS400Xml() {
    }

    @Override
    public int getByteLength() {
        return 44;
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

    // data doesn't appear to be journaled
    @Override
    public Object toObject(byte[] data, int offset) {
        return "";
    }

    @Override
    public Object clone() {
        try {
            return super.clone(); // Object.clone does not throw exception.
        }
        catch (CloneNotSupportedException e) {
            Trace.log(Trace.ERROR, "Unexpected CloneNotSupportedException:", e);
            throw new InternalErrorException(InternalErrorException.UNEXPECTED_EXCEPTION);
        }
    }

}
