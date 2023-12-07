/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.kakfa.smt;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.kakfa.smt.RepackageJavaFriendlySchemaRenamer.SimpleConfig;

public class LowerCaseTopicRePrefixSink<R extends ConnectRecord<R>> implements Transformation<R> {
	private static final Logger log = LoggerFactory.getLogger(LowerCaseTopicRePrefixSink.class);

	public static final String OVERVIEW_DOC = "<p/>lowercase topic, replace prefix</p>";

	public static final String PREFIX = "prefix";

	public static final ConfigDef CONFIG_DEF = new ConfigDef().define(PREFIX, ConfigDef.Type.STRING,
			ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
		@Override
		public void ensureValid(String name, Object valueObject) {
			final String value = (String) valueObject;
			if (value == null || value.isEmpty()) {
				throw new ConfigException("Must specify replacement e.g. 'schema'");
			}
		}

		@Override
		public String toString() {
			return "Replacement string";
		}
	}, ConfigDef.Importance.HIGH, "replace topic prefix with schema for sink");

	private static final String PURPOSE = "rename to schemas";

	private String prefix;

	@Override
	public void configure(Map<String, ?> props) {
		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
		prefix = String.format("%s", config.getString(PREFIX));
	}

	@Override
	public R apply(R record) {
		String topic = record.topic();
		final int pos = topic.lastIndexOf(".");
		if (pos >= 0 && ((pos + 1) <= topic.length())) {
			topic = topic.substring(pos + 1);
		}
		topic = String.format("%s.%s", prefix, topic.toLowerCase());
		return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(),
				record.value(), record.timestamp());
	}

	@Override
	public void close() {
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}
