/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Bin8;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;
import com.ibm.as400.access.Trace;

import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;

public class AS400Clob implements AS400DataType {
    private static final Logger log = LoggerFactory.getLogger(AS400Clob.class);
    private static final String EMPTY_STRING = "";
    private final AS400 as400;
    private static final byte[] EMPTY_BYTES = new byte[0];

    public AS400Clob(AS400 as400) {
        this.as400 = as400;
    }

    @Override
    public int getByteLength() {
        return 44;
    }

    @Override
    public Object getDefaultValue() {
        return EMPTY_STRING;
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
        return EMPTY_BYTES;
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

    /**
     * TODO clob support not working
     *
     * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORRCVI.htm
     *      https://www.ibm.com/support/knowledgecenter/en/ssw_ibm_i_72/sqlp/rbafylobjournal.htm
     * @param as400
     * @return
     * @throws Exception
     */
    private String getClob(long addressHigh, long addressLow, byte[] addressbyte, int length) throws Exception {

        final byte[] hb = new AS400Bin8().toBytes(addressHigh);
        final byte[] lb = new AS400Bin8().toBytes(addressLow);
        final byte[] address16 = new byte[16];
        System.arraycopy(hb, 0, address16, 0, 8);
        System.arraycopy(lb, 0, address16, 8, 8);
        log.debug("addresss re-encoded {}", toHex(address16));
        log.debug("address raw {}", toHex(addressbyte));

        final ProgramParameter[] parameters = new ProgramParameter[]{
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, length + 2),
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, addressbyte),
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(length)), };

        final ServiceProgramCall spc = new ServiceProgramCall(as400);

        spc.setProgram("/QSYS.LIB/MARTINT.LIB/RTVPTRDTA.SRVPGM", parameters);
        spc.setProcedureName("GETDATA");
        spc.setAlignOn16Bytes(true);
        spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
        // log.debug("pre job: " + spc.getServerJob());
        final boolean success = spc.run();
        // log.debug("post job: " + spc.getServerJob());
        if (success) {
            final byte[] data = parameters[0].getOutputData();
            final int retrivedLength = (Integer) new AS400Bin4().toObject(data, 0);
            final byte[] btext = Arrays.copyOfRange(data, 2, data.length);
            final String text = new String(btext);
            log.debug(text);
            return text;
        }
        else {
            final String msg = "call failed " + Arrays.asList(spc.getMessageList()).stream().map(AS400Message::getText)
                    .reduce("", (a, s) -> a + s);
            log.debug(msg);
            throw new Exception(msg);
        }
    }

    private String deleteClob(long addressHigh, long addressLow, byte[] addressbyte) throws Exception {

        final byte[] hb = new AS400Bin8().toBytes(addressHigh);
        final byte[] lb = new AS400Bin8().toBytes(addressLow);
        final byte[] address16 = new byte[16];
        System.arraycopy(hb, 0, address16, 0, 8);
        System.arraycopy(lb, 0, address16, 8, 8);
        log.debug("addresss re-encoded {}", toHex(address16));
        log.debug("address raw {}", toHex(addressbyte));

        final ProgramParameter[] parameters = new ProgramParameter[]{
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes((int) addressLow)), };

        final ServiceProgramCall spc = new ServiceProgramCall(as400);

        spc.setProgram(JournalInfoRetrieval.JOURNAL_SERVICE_LIB, parameters);
        spc.setProcedureName("QjoDeletePointerHandle");
        spc.setAlignOn16Bytes(true);
        spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
        log.debug("pre job: {}", spc.getServerJob());
        final boolean success = spc.run();
        log.debug("post job: {}", spc.getServerJob());
        if (success) {
            return "";
        }
        else {
            final String msg = String.format("call failed %s", Arrays.asList(spc.getMessageList()).stream()
                    .map(AS400Message::getText).reduce("", (a, s) -> a + s));
            log.debug(msg);
            throw new Exception(msg);
        }
    }

    public String toHex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    @Override
    public Object toObject(byte[] data, int offset) {
        // padding to align pointer relative offset 13 to be 16 byte aligned
        final int alignment = (16 - ((offset + 13) % 16));
        // 1 byte of system info (set to 0x00) + alignment
        final int relativeOffset = offset + 1 + alignment;
        final int length = (Integer) new AS400Bin4().toObject(data, relativeOffset);
        final long first = (Long) new AS400Bin8().toObject(data, relativeOffset + 12);
        final long second = (Long) new AS400Bin8().toObject(data, relativeOffset + 20);
        // release handle: QjoDeletePointerHandle

        log.debug("alignment {}", alignment);
        log.debug("alignment zero {}", (relativeOffset + 12) % 16);
        log.debug("length {}", length);
        log.debug("first {}", first);
        log.debug("second {}", second);
        // log.debug("data length {}", data.length);
        log.debug(Diagnostics.binAsHex(data, offset, 44));

        final byte[] addressbyte = Arrays.copyOfRange(data, relativeOffset + 12, relativeOffset + 28);

        try {
            log.debug(getClob(first, second, addressbyte, length));
            // log.debug(deleteClob(first, second, addressbyte));
        }
        catch (final Exception e) {
            log.error("failed to get clob", e);
        }

        return "";
    }

    @Override
    public Object clone() {
        try {
            return super.clone(); // Object.clone does not throw exception.
        }
        catch (final CloneNotSupportedException e) {
            Trace.log(Trace.ERROR, "Unexpected CloneNotSupportedException:", e);
            throw new InternalErrorException(InternalErrorException.UNEXPECTED_EXCEPTION);
        }
    }

}
