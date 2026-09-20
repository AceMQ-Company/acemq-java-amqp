/*
 * Copyright 2026 AceMQ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acemq.amqp.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import org.acemq.amqp.codec.avro.AvroCodec;
import org.acemq.amqp.codec.avro.InMemorySchemaRegistry;
import org.acemq.amqp.test.avro.ResolutionOrder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Holds this library to both columns of {@code avro-resolution-fixtures.json}.
 *
 * <p>Both are reachable from here without asking for either, which is why the fixture is asserted
 * from here twice rather than once. A reader schema taken from the generated class resolves, so the
 * field the writer never sent arrives carrying the reader's default; a {@code GenericRecord} asked
 * for through a plain registry codec has no reader schema of its own, so the writer's record comes
 * back as it was written and the field is simply not there.
 *
 * <p>The other four libraries assert both columns too, and reach the second one deliberately: Go by
 * leaving {@code avro.ReaderSchema(...)} off, Python and Ruby by naming the writer's schema as the
 * reader schema, .NET by calling {@code WithoutReaderSchema()}. What is only true here is that
 * {@code registered(registry)} is handed no schema at all, so the writer's serves as the reader's
 * without anybody having chosen it.
 *
 * <p>Neither of those is a bug and neither is being changed. The rule underneath is the same one
 * in all five languages — a library resolves when it has a reader schema to resolve onto — and the
 * fixture exists so that the sentence has bytes behind it.
 *
 * <p>The file is read rather than regenerated here. {@link FixtureDriftTest} proves the committed
 * bytes are what the generator produces; this test proves the committed bytes are what the codec
 * obeys. Asserting against the generator instead would only prove that one method agrees with
 * another method in the same process.
 */
@DisplayName("what the avro resolution fixtures say Java must do")
class AvroResolutionTest {

    private static JsonNode fixtures;

    @BeforeAll
    static void load() throws IOException {
        try (InputStream in = AvroResolutionTest.class.getResourceAsStream("/fixtures/avro-resolution-fixtures.json")) {
            assertThat(in).as("avro-resolution-fixtures.json is on the test classpath").isNotNull();
            fixtures = new ObjectMapper().readTree(in);
        }
        assertThat(fixtures.get("generatedBy").asText()).isEqualTo("acemq-java-amqp AvroResolutionFixtures");
    }

    private static JsonNode fixtureCase(String name) {
        for (JsonNode candidate : fixtures.get("cases")) {
            if (name.equals(candidate.get("case").asText())) {
                return candidate;
            }
        }
        throw new AssertionError("no case named " + name + " in the avro resolution fixtures");
    }

    /** The registry a reader of this file would build: the writer's schema, under the framed id. */
    private static InMemorySchemaRegistry registryFor(JsonNode entry) {
        Schema writerSchema = new Schema.Parser().parse(entry.get("writerSchema").asText());
        return new InMemorySchemaRegistry().register(entry.get("schemaId").asInt(),
                AvroCodec.definitionOf(writerSchema));
    }

    private static byte[] body(JsonNode entry) {
        return Base64.getDecoder().decode(entry.get("bodyBase64").asText());
    }

    /**
     * Asserts a decoded record against one of the fixture's two columns.
     *
     * <p>Field by field, and then the field count, because the whole difference between the two
     * columns in the first case is a field that is present in one and absent in the other. A
     * comparison that only walked the expected keys would pass either way.
     */
    private static void assertMatches(GenericRecord decoded, JsonNode expected, String column) {
        List<String> names = new ArrayList<>();
        Iterator<String> keys = expected.fieldNames();
        while (keys.hasNext()) {
            names.add(keys.next());
        }

        for (String name : names) {
            JsonNode value = expected.get(name);
            // Asked of the schema first: GenericRecord.get throws for a field it has never heard
            // of, and an exception out of a comparison reads as a broken test rather than as the
            // wrong column.
            assertThat(decoded.getSchema().getField(name))
                    .as("%s.%s is a field of the decoded record", column, name)
                    .isNotNull();
            Object actual = decoded.get(name);
            assertThat(actual).as("%s.%s is present", column, name).isNotNull();
            if (value.isNumber()) {
                assertThat(actual).as("%s.%s", column, name).isEqualTo(value.asInt());
            } else {
                assertThat(String.valueOf(actual)).as("%s.%s", column, name).isEqualTo(value.asText());
            }
        }

        List<String> actualNames = new ArrayList<>();
        for (Schema.Field field : decoded.getSchema().getFields()) {
            actualNames.add(field.name());
        }
        assertThat(actualNames).as("the fields of the %s column, and no others", column).isEqualTo(names);
    }

    @Test
    @DisplayName("a reader schema taken from the generated class resolves: the writer's missing field "
            + "arrives as the reader's default")
    void the_specific_readers_schema_fills_in_a_field_the_writer_never_sent() {
        JsonNode entry = fixtureCase("field-removed-by-the-writer");

        // The reader schema is the one the generated class carries, and the fixture carries the
        // same text. If the class and the file ever part company the rest of this test would be
        // asserting something else, so it is checked before anything is decoded.
        assertThat(entry.get("readerSchema").asText())
                .as("the reader schema in the fixture is the one ResolutionOrder declares")
                .isEqualTo(ResolutionOrder.SCHEMA$.toString());

        GenericRecord decoded = AvroCodec.registered(registryFor(entry), ResolutionOrder.SCHEMA$)
                .decode(body(entry), GenericRecord.class);

        // The value that can only have come from resolution. "GBP" appears in no message and in
        // no writer schema; it exists solely as the reader schema's default, which is why the
        // fixture uses it instead of an empty string that an unresolved decode could produce by
        // accident.
        assertThat(decoded.get("currency"))
                .as("the field the writer does not have, filled in from the reader's default")
                .hasToString("GBP");

        assertMatches(decoded, entry.get("resolved"), "resolved");
    }

    @Test
    @DisplayName("a GenericRecord through a plain registry codec does not resolve: the writer's "
            + "record arrives as it was written")
    void a_generic_record_comes_back_in_the_writers_shape() {
        JsonNode entry = fixtureCase("field-removed-by-the-writer");

        GenericRecord decoded = AvroCodec.registered(registryFor(entry)).decode(body(entry), GenericRecord.class);

        // Not null, not the default: absent. A GenericRecord asks for nothing in particular, so
        // the reader schema is the writer's and there is no declaration to fill anything in from.
        assertThat(decoded.getSchema().getField("currency"))
                .as("a field the writer never declared is not in the record at all")
                .isNull();

        assertMatches(decoded, entry.get("writerShape"), "writerShape");
    }

    @Test
    @DisplayName("a field the writer added is harmless both ways, which is the half people worry about")
    void an_unknown_field_is_skipped_by_one_and_carried_by_the_other() {
        JsonNode entry = fixtureCase("field-added-by-the-writer");

        GenericRecord resolved = AvroCodec.registered(registryFor(entry), ResolutionOrder.SCHEMA$)
                .decode(body(entry), GenericRecord.class);
        GenericRecord writerShape = AvroCodec.registered(registryFor(entry))
                .decode(body(entry), GenericRecord.class);

        assertMatches(resolved, entry.get("resolved"), "resolved");
        assertMatches(writerShape, entry.get("writerShape"), "writerShape");

        // The property that matters, stated once rather than inferred from the two columns: the
        // field the reader does not know about did not shift the ones it does. This is the failure
        // people expect from an added field and the reason it does not happen.
        assertThat(resolved.get("orderId")).hasToString(writerShape.get("orderId").toString());
        assertThat(resolved.get("total")).isEqualTo(writerShape.get("total"));
        assertThat(resolved.get("currency")).hasToString(writerShape.get("currency").toString());
    }

    @Test
    @DisplayName("the schema id in the frame is the one the fixture says it is")
    void the_framed_identifier_matches_the_fixture() {
        for (JsonNode entry : fixtures.get("cases")) {
            byte[] body = body(entry);
            int id = (body[1] & 0xFF) << 24 | (body[2] & 0xFF) << 16 | (body[3] & 0xFF) << 8 | body[4] & 0xFF;

            assertThat(body[0]).as("%s begins with the magic byte", entry.get("case").asText()).isZero();
            assertThat(id)
                    .as("the id framed into %s", entry.get("case").asText())
                    .isEqualTo(entry.get("schemaId").asInt());
        }
    }

    @Test
    @DisplayName("asking a registry codec for the generated class returns the generated class, "
            + "resolved against its own schema")
    void the_generated_class_can_be_the_decode_target() {
        // This threw until the codec stopped choosing its reader from how it was built.
        //
        // AvroCodec has always taken the reader schema from a SpecificRecord target, so the
        // resolution below always happened. What did not happen was reading into the class: a
        // registry codec built a GenericDatumReader whatever it was asked for, produced a
        // GenericData.Record, and failed the cast with a ClassCastException naming two types the
        // caller had never mentioned. The reader is now chosen from the target, which is the same
        // thing the reader schema is chosen from and the same thing the cast is against.
        //
        // Passing a generated class is the obvious thing to try, so it should be the thing that
        // works rather than the thing with a test explaining why it does not.
        JsonNode entry = fixtureCase("field-removed-by-the-writer");

        ResolutionOrder decoded = AvroCodec.registered(registryFor(entry))
                .decode(body(entry), ResolutionOrder.class);

        // The generated class itself came back, which is the whole point: before the fix this
        // line was a ClassCastException inside an AceMqException.
        assertThat(decoded).isInstanceOf(ResolutionOrder.class);

        // And it resolved: "GBP" is in no message and in no writer schema, and exists only as
        // the reader schema's default.
        assertThat(decoded.get("currency"))
                .as("the field the writer does not have, filled in from the generated class's default")
                .hasToString("GBP");
        assertMatches(decoded, entry.get("resolved"), "resolved");
    }

    @Test
    @DisplayName("the same class through the explicit reader-schema overload agrees with it")
    void the_explicit_reader_schema_overload_gives_the_same_answer() {
        // The two routes to the same place must not disagree. Before the fix only this one
        // worked, so nothing compared them, and a divergence would have gone unseen.
        JsonNode entry = fixtureCase("field-removed-by-the-writer");

        ResolutionOrder viaTarget = AvroCodec.registered(registryFor(entry))
                .decode(body(entry), ResolutionOrder.class);
        ResolutionOrder viaSchema = AvroCodec.registered(registryFor(entry), ResolutionOrder.SCHEMA$)
                .decode(body(entry), ResolutionOrder.class);

        assertThat(viaTarget)
                .as("the target's schema and the same schema passed explicitly decode alike")
                .isEqualTo(viaSchema);
    }
}
