/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Exception;
import com.ibm.as400.access.AS400FileRecordDescription;
import com.ibm.as400.access.AS400SecurityException;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.FieldDescription;
import com.ibm.as400.access.QSYSObjectPathName;
import com.ibm.as400.access.RecordFormat;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

public class RpgFileDecoder implements JournalEntryDeocder<List<FieldEntry>> {
    private final AS400 as400;
    private final Map<String, NamesTypes> recordFormats = new HashMap<>();

    public RpgFileDecoder(AS400 as400) {
        this.as400 = as400;
    }

    @Override
    public List<FieldEntry> decode(EntryHeader eheader, byte[] data, int offset) throws Exception {
        String file = eheader.getFile();
        String lib = eheader.getLibrary();
        String member = eheader.getMember();
        String recordFileName = "/QSYS.LIB/" + lib + ".LIB/" + file + ".FILE/" + member + ".MBR";
        NamesTypes namesTypes = getRecordFormat(recordFileName);

        Object[] os = decodeEntry(namesTypes.getTypes(), data, offset + ENTRY_SPECIFIC_DATA_OFFSET + eheader.getEntrySpecificDataOffset());

        List<FieldEntry> fields = new ArrayList<>();
        List<String> names = namesTypes.getNames();
        for (int i = 0; i < os.length && i < names.size(); i++) {
            FieldEntry field = new FieldEntry(names.get(i), os[i]);
            fields.add(field);
        }
        return fields;

    }

    public NamesTypes getRecordFormat(String name)
            throws AS400Exception, AS400SecurityException, InterruptedException, IOException {
        if (recordFormats.containsKey(name)) {
            return recordFormats.get(name);
        }
        QSYSObjectPathName qpath = new QSYSObjectPathName(name);
        AS400FileRecordDescription recordDescription = new AS400FileRecordDescription(as400, qpath.getPath());
        RecordFormat[] fileFormat = recordDescription.retrieveRecordFormat();

        if (fileFormat.length > 1) {
            throw new IllegalArgumentException(qpath.getPath() + " contains more than one record format.");
        }
        // always default to fileFormat[0] cause only physical files are assumed here.
        ArrayList<AS400DataType> structure = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        for (int i = 0; i < fileFormat[0].getNumberOfFields(); i++) {
            FieldDescription fd = fileFormat[0].getFieldDescription(i);
            structure.add(fd.getDataType());
            names.add(fd.getFieldName());
        }
        AS400Structure entryDetailStructure = new AS400Structure(structure.toArray(new AS400DataType[structure.size()]));

        NamesTypes nt = new NamesTypes(entryDetailStructure, names);
        recordFormats.put(name, nt);
        return nt;
    }

    public Object[] decodeEntry(AS400Structure entryDetailStructure, byte[] data, int offset) {
        Object[] result = (Object[]) entryDetailStructure.toObject(data, offset);
        return result;
    }

}
