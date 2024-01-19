/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.util.List;

import com.ibm.as400.access.AS400Structure;

class NamesTypes {
    private final AS400Structure types;
    private final List<String> names;

    NamesTypes(AS400Structure types, List<String> names) {
        super();
        this.types = types;
        this.names = names;
    }

    public AS400Structure getTypes() {
        return types;
    }

    public List<String> getNames() {
        return names;
    }
}