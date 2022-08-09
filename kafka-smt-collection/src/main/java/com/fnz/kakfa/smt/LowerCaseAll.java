/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.kakfa.smt;

import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowerCaseAll<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(LowerCaseAll.class);

    public static final String OVERVIEW_DOC = "<p/>regex schema renamer</p>";

    private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

    @Override
    public void configure(Map<String, ?> props) {
    }

    @Override
    public R apply(R record) {
        if (!hasSchema(record)) {
            return record;
        }

        return applyWithSchema(record);
    }

    @Override
    public void close() {
    }

    boolean hasSchema(R record) {
        Schema key = record.keySchema();
        Schema value = record.valueSchema();
        return ((key != null && Type.STRUCT.equals(key.type())) || (value != null && Type.STRUCT.equals(value.type())));
    }

    R applyWithSchema(R record) {
        RenamedSchema renamedKeySchema = renameSchema(record.keySchema());
        RenamedSchema renamedValueSchema = renameSchema(record.valueSchema());
        if (renamedKeySchema.isRenamed || renamedValueSchema.isRenamed) {
            Object key = updateSchemaIn(record.key(), renamedKeySchema.schema);
            Object value = updateSchemaIn(record.value(), renamedValueSchema.schema);
            R renamedRecord = record.newRecord(record.topic(), record.kafkaPartition(), renamedKeySchema.schema, key, renamedValueSchema.schema, value,
                    record.timestamp());
            return renamedRecord;
        }
        return record;
    }

    static Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                newStruct.put(field, origStruct.get(field));
            }
            return newStruct;
        }
        return keyOrValue;
    }

    RenamedSchema renameSchema(Schema schema) {
        String newName = newSchemaName(schema);
        if (newName != null) {
            Schema renamed = schemaUpdateCache.get(schema);
            if (renamed == null) {
                renamed = renameSchema(schema, newName);
                schemaUpdateCache.put(schema, renamed);
            }

            return new RenamedSchema(true, renamed);
        }
        return new RenamedSchema(false, schema);
    }

    String newSchemaName(Schema schema) {
        if (schema == null) {
            return null;
        }
        String name = schema.name();
        if (name == null) {
            return null;
        }
        String lower = name.toLowerCase();
        if (lower.equals(name)) {
            return null;
        }
        return lower;
    }


    Schema renameSchema(Schema schema, String newName) {
        if (schema == null) {
            return schema;
        }
        SchemaBuilder builder = new SchemaBuilder(schema.type());
        builder.name(newName);
        builder.version(schema.version());
        builder.doc(schema.doc());
        for (Field f : schema.fields()) {
            builder.field(f.name().toLowerCase(), f.schema());
        }

        final Map<String, String> params = schema.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        return builder.build();
    }

    public static class RenamedSchema {
        public final Boolean isRenamed;
        public final Schema schema;

        public RenamedSchema(Boolean isRenamed, Schema schema) {
            this.isRenamed = isRenamed;
            this.schema = schema;
        }
    }

    public static class SimpleConfig extends AbstractConfig {

        public SimpleConfig(ConfigDef configDef, Map<?, ?> originals) {
            super(configDef, originals, false);
        }

    }

    public String capitalizeFirstLowerRest(String original) {
        if (original == null || original.length() == 0) {
            return original;
        }
        return original.substring(0, 1).toUpperCase() + original.substring(1).toLowerCase();
    }

    private final static ConfigDef CONFIG_DEF = new ConfigDef();
    
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
