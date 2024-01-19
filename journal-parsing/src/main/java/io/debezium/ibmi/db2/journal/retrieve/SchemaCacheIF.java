/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.util.List;
import java.util.stream.Collectors;

import com.ibm.as400.access.AS400Structure;

public interface SchemaCacheIF {
    void store(String database, String schema, String table, TableInfo tableInfo);

    TableInfo retrieve(String database, String schema, String table);

    void clearCache(String database, String schema, String table);

    class TableInfo {
        private final List<Structure> structure;
        private final AS400Structure as400Structure;
        // private final AS400Structure as400Keys;
        private final List<String> primaryKeys;

        public TableInfo(List<Structure> structure, List<String> primaryKeys, AS400Structure as400Structure) {
            super();
            this.structure = structure;
            this.as400Structure = as400Structure;
            // this.as400Keys = as400Keys;
            this.primaryKeys = primaryKeys;
        }

        public List<Structure> getStructure() {
            return structure;
        }

        public AS400Structure getAs400Structure() {
            return as400Structure;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        @Override
        public String toString() {
            return "TableInfo [ primaryKeys=" + toString(primaryKeys) + " structure=[" + toString(structure) + "] ]";
        }

        public String toString(List<? extends Object> l) {
            if (l == null) {
                return "";
            }
            return l.stream().map(Object::toString).collect(Collectors.joining(","));
        }
    }

    class Structure {
        private final String name;
        private final String type;
        private final int jdcbType;
        private final int length;
        private final int precision;
        private final boolean optional;
        private final int position;
        private final boolean autoinc;
        private final int ccsid;

        public Structure(String name, String type, int jdcbType, int length, int precision,
                         boolean optional,
                         int position, boolean autoinc) {
            super();
            this.name = name;
            this.type = type;
            this.jdcbType = jdcbType;
            this.length = length;
            this.precision = precision;
            this.optional = optional;
            this.position = position;
            this.autoinc = autoinc;
            this.ccsid = -1;
        }

        public Structure(String name, String type, int jdcbType, int length, int precision, boolean optional,
                         int position, boolean autoinc, int ccsid) {
            super();
            this.name = name;
            this.type = type;
            this.jdcbType = jdcbType;
            this.length = length;
            this.precision = precision;
            this.optional = optional;
            this.position = position;
            this.autoinc = autoinc;
            this.ccsid = ccsid;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public int getLength() {
            return length;
        }

        public int getPrecision() {
            return precision;
        }

        public int getJdcbType() {
            return jdcbType;
        }

        public boolean isOptional() {
            return optional;
        }

        public int getPosition() {
            return position;
        }

        public boolean isAutoinc() {
            return autoinc;
        }

        @Override
        public String toString() {
            return "Structure [name=" + name + ", type=" + type + ", jdcbType=" + jdcbType + ", length=" + length
                    + ", precision=" + precision + ", optional=" + optional + ", position=" + position + ", autoinc="
                    + autoinc + ", ccsid=" + ccsid + "]";
        }

    }
}
