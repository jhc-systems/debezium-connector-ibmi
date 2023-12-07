package com.fnz.kakfa.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LowerCaseTopicRePrefixSinkTest {
	LowerCaseTopicRePrefixSink<SourceRecord> lct = new LowerCaseTopicRePrefixSink<>();
	Schema schema;

	@BeforeEach
	void setUp() throws Exception {
		final SchemaBuilder sb = new SchemaBuilder(Type.STRING);
		schema = sb.build();

		final Map<String, String> m = Map.of("prefix", "myschema");
		lct.configure(m);
	}

	@Test
	void testNormalString() {
		final Map<String, Integer> sourcePartition = new HashMap<>();
		final Map<String, Integer> sourceOffset = new HashMap<>();
		final SourceRecord r = new SourceRecord(sourcePartition, sourceOffset, "FOO.BAR.TOPIC", 1, schema, "key",
				schema, "value", 1l);
		final SourceRecord lowered = lct.apply(r);

		assertEquals("myschema.topic", lowered.topic());
	}

	@Test
	void testNoPrefix() {
		final Map<String, Integer> sourcePartition = new HashMap<>();
		final Map<String, Integer> sourceOffset = new HashMap<>();
		final SourceRecord r = new SourceRecord(sourcePartition, sourceOffset, "TOPIC", 1, schema, "key", schema,
				"value", 1l);
		final SourceRecord lowered = lct.apply(r);

		assertEquals("myschema.topic", lowered.topic());
	}

	@Test
	void testEndsWithDot() {
		final Map<String, Integer> sourcePartition = new HashMap<>();
		final Map<String, Integer> sourceOffset = new HashMap<>();
		final SourceRecord r = new SourceRecord(sourcePartition, sourceOffset, "TOPIC.", 1, schema, "key", schema,
				"value", 1l);
		final SourceRecord lowered = lct.apply(r);

		assertEquals("myschema.", lowered.topic());
	}
}
