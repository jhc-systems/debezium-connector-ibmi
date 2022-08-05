/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.logging.structured;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.layout.template.json.resolver.EventResolver;
import org.apache.logging.log4j.layout.template.json.util.JsonWriter;
import org.apache.logging.log4j.message.Message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


final class StructuredMessageResolver implements EventResolver {

    private static final StructuredMessageResolver INSTANCE = new StructuredMessageResolver();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static StructuredMessageResolver getInstance() {
        return INSTANCE;
    }

    static String getName() {
        return "structuredMessage";
    }

    @Override
    public boolean isResolvable(LogEvent logEvent) {
        return (logEvent.getMessage() instanceof StructuredMessage);
    }

    @Override
    public void resolve(LogEvent logEvent, JsonWriter jsonWriter) {
        Message msg = logEvent.getMessage();
        if (msg instanceof StructuredMessage sm) {
            Object value = sm.getParameter();
            try {
                // With sufficient sorcery, this call can be made garbage-free.
                String valueJson = OBJECT_MAPPER.writeValueAsString(value);
                jsonWriter.writeRawString(valueJson);
            } catch (JsonProcessingException error) {
                throw new RuntimeException(error);
            }
        } else { 
            jsonWriter.writeString(msg.getFormattedMessage());
        }
    }

}
