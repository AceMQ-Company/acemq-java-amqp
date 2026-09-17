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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.acemq.amqp.codec.avro.AvroCodec;
import org.acemq.amqp.codec.avro.InMemorySchemaRegistry;
import org.acemq.amqp.core.Json;
import org.acemq.amqp.test.avro.ResolutionOrder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Writes what Avro schema resolution does, and what happens when it does not happen.
 *
 * <p>The five libraries do not resolve in the same circumstances, and that is not a bug anybody
 * is going to fix: resolution needs a reader schema, and each library resolves exactly when it
 * has one. Go has none unless the caller passes one, because a Go struct carries no schema. Java
 * has one when the target is a generated class or the codec was handed a reader schema, and none
 * when a {@code GenericRecord} is asked for through a registry codec. .NET, Python and Ruby
 * always have one, because their codec is constructed with it.
 *
 * <p>So this file does not pin one answer. It pins <em>both</em>, in two columns named
 * {@code resolved} and {@code writerShape}, and says which library lands on which. A library
 * reading it asserts the column its own documentation claims, and the fixture is then a test of
 * the documentation as much as of the code.
 *
 * <p>Every value below is produced by running the library rather than typed. The bodies are the
 * bytes {@code AvroCodec.registered(...)} actually writes; the {@code resolved} column is a real
 * decode against {@link ResolutionOrder#SCHEMA$}; the {@code writerShape} column is a real decode
 * with no reader schema at all. A transcribed expectation agrees with whatever was believed on
 * the day it was written, which is how the retry multiplier survived ten releases.
 */
final class AvroResolutionFixtures {

    /**
     * The writer that predates the field, and the case that actually bites.
     *
     * <p>It has no {@code currency}. A reader that resolves sees {@code "GBP"} — the reader
     * schema's default, a value that appears in no message and in no writer schema. A reader that
     * does not resolve sees a record with two fields, and asking it for a currency is a missing
     * key rather than a wrong answer.
     */
    private static final String WRITER_WITHOUT_CURRENCY = "{\"type\":\"record\",\"name\":\"ResolutionOrder\",\"namespace\":\"org.acemq.amqp.test.avro\","
            + "\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"total\",\"type\":\"int\"}]}";

    /**
     * The writer that has run ahead, and the case people are most afraid of for no reason.
     *
     * <p>It has a {@code channel} the reader has never heard of. Under resolution the field is
     * skipped; without resolution it is simply there. Either way the fields the reader does
     * declare come back correct, which is the property worth pinning: the unknown field does not
     * shift everything after it.
     */
    private static final String WRITER_WITH_CHANNEL = "{\"type\":\"record\",\"name\":\"ResolutionOrder\",\"namespace\":\"org.acemq.amqp.test.avro\","
            + "\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"total\",\"type\":\"int\"},"
            + "{\"name\":\"currency\",\"type\":\"string\",\"default\":\"GBP\"},"
            + "{\"name\":\"channel\",\"type\":\"string\"}]}";

    /**
     * Identifiers chosen rather than allocated.
     *
     * <p>{@link InMemorySchemaRegistry} hands out numbers in the order it first sees a schema, so
     * a fixture that let it choose would renumber itself the day a case is added in the middle.
     * Four repositories hold a byte-identical copy of this file.
     */
    private static final int WITHOUT_CURRENCY_ID = 101;

    private static final int WITH_CHANNEL_ID = 102;

    private AvroResolutionFixtures() {
        throw new AssertionError("AvroResolutionFixtures is a generator and must not be instantiated");
    }

    /**
     * Builds the whole fixture.
     *
     * @return the contents of {@code avro-resolution-fixtures.json}
     */
    static String generate() {
        Schema withoutCurrency = new Schema.Parser().parse(WRITER_WITHOUT_CURRENCY);
        Schema withChannel = new Schema.Parser().parse(WRITER_WITH_CHANNEL);
        InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                .register(WITHOUT_CURRENCY_ID, AvroCodec.definitionOf(withoutCurrency))
                .register(WITH_CHANNEL_ID, AvroCodec.definitionOf(withChannel));

        GenericData.Record removed = new GenericData.Record(withoutCurrency);
        removed.put("orderId", "o-1");
        removed.put("total", 1250);

        GenericData.Record added = new GenericData.Record(withChannel);
        added.put("orderId", "o-2");
        added.put("total", 990);
        added.put("currency", "EUR");
        added.put("channel", "mobile");

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedBy", "acemq-java-amqp AvroResolutionFixtures");
        root.put("contract", "when an AceMQ library resolves an Avro message onto a reader schema, and what a"
                + " message decodes to when it does not");
        root.put("rule", "resolution happens when the library has a reader schema to resolve onto, and not"
                + " otherwise. That is the whole rule, and it is the same rule in all five languages. The"
                + " libraries differ only in where a reader schema comes from, so they differ in how often"
                + " they have one -- which is a difference in what each language can know, not a"
                + " disagreement about Avro");
        root.put("columns", columns());
        root.put("libraries", libraries());
        root.put("bodyEncoding", "base64. Decoding it gives the bytes this library puts on the wire: one zero"
                + " byte, then four bytes of schema id big-endian, then the Avro body -- the framing"
                + " Confluent's clients use. schemaId is the number in those four bytes, and it is the id the"
                + " writerSchema is registered under");
        root.put("schemaEncoding", "writerSchema and readerSchema are the schema text, as a JSON string rather"
                + " than as a nested object. It is the exact text registered under schemaId: re-rendering a"
                + " parsed schema is not guaranteed to give the same bytes back, and the bytes are what the"
                + " identifier stands for");
        root.put("howToAssert", "register writerSchema under schemaId, decode body, and compare with the column"
                + " your library lands on in the table above. A library that can do both should assert both."
                + " The fields of resolved and writerShape are in the order their schema declares them; a"
                + " library whose decoded form is unordered should compare as a map");
        root.put("cases", Arrays.asList(
                theCase(
                        "field-removed-by-the-writer",
                        "the writer does not have a field the reader declares with a default. This is the case"
                                + " where the two columns differ in a value rather than in a spare key, and"
                                + " the case that leaves somebody in another service staring at a field they"
                                + " are certain they declared",
                        WITHOUT_CURRENCY_ID,
                        withoutCurrency,
                        removed,
                        registry),
                theCase(
                        "field-added-by-the-writer",
                        "the writer has a field the reader does not declare. Harmless either way, and pinned"
                                + " precisely because it is the case everybody assumes is the dangerous one:"
                                + " the fields the reader knows decode correctly in both columns, and the"
                                + " unknown field does not shift them",
                        WITH_CHANNEL_ID,
                        withChannel,
                        added,
                        registry)));
        root.put("notes", notes());
        return Json.render(root);
    }

    private static Map<String, Object> columns() {
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("resolved", "what a reader that holds a reader schema sees: the writer's bytes reconciled"
                + " with the reader's declaration. A field the writer omitted is filled in from the reader's"
                + " default; a field the writer added and the reader does not declare is skipped");
        columns.put("writerShape", "what a reader that holds no reader schema sees: the writer's record as it"
                + " was written. Nothing is filled in, because there is no declaration saying what to fill it"
                + " in with, and nothing is skipped");
        return columns;
    }

    private static List<Object> libraries() {
        List<Object> libraries = new ArrayList<>();
        libraries.add(library(
                "acemq-go-amqp",
                "writerShape",
                "a Go struct carries no schema, so by default the decoder has nothing to resolve onto. A"
                        + " caller who passes avro.ReaderSchema(...) has given it one, and that call moves"
                        + " Go to the resolved column"));
        libraries.add(library(
                "acemq-java-amqp",
                "both",
                "a GenericRecord asks for nothing in particular, so the reader schema is the writer's and"
                        + " nothing resolves; a generated SpecificRecord class carries a schema of its own,"
                        + " and registered(registry, readerSchema) is handed one, and either of those"
                        + " resolves. Java is the only one of the five that shows both columns"));
        libraries.add(library(
                "acemq-dotnet-amqp",
                "resolved",
                "the codec is constructed with a schema, so there is always one to resolve onto"));
        libraries.add(library(
                "acemq-python-amqp",
                "resolved",
                "the codec is constructed with a schema, so there is always one to resolve onto"));
        libraries.add(library(
                "acemq-ruby-amqp",
                "resolved",
                "the codec is constructed with a schema, so there is always one to resolve onto"));
        return libraries;
    }

    private static Map<String, Object> library(String name, String column, String why) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("library", name);
        entry.put("column", column);
        entry.put("why", why);
        return entry;
    }

    private static Map<String, Object> notes() {
        Map<String, Object> notes = new LinkedHashMap<>();
        notes.put(
                "whyTheDefaultIsNotAZeroValue",
                "the reader schema defaults currency to \"GBP\" and not to \"\". A default that is also the"
                        + " type's zero value passes whether resolution happened or not, because an absent"
                        + " field and a field defaulted to the zero value look identical at the assertion."
                        + " \"GBP\" can only have come from the reader schema");
        notes.put(
                "neitherColumnIsTheWrongOne",
                "a library that resolves and a library that does not are both right about the same bytes."
                        + " They are answering different questions, because only one of them was told what"
                        + " the reader expects");
        notes.put(
                "nothingHereChangesTheWire",
                "resolution is a reader-side decision. Both bodies were written by a plain registered codec,"
                        + " and a library that resolves reads exactly the same bytes as one that does not");
        notes.put(
                "whichToReachFor",
                "where a library can be asked for either behaviour, the one to reach for is resolved. It is"
                        + " what lets a producer be redeployed without its consumers, which is the entire"
                        + " reason the schema id is on the front of the message");
        return notes;
    }

    private static Map<String, Object> theCase(
            String name,
            String what,
            int schemaId,
            Schema writerSchema,
            GenericRecord payload,
            InMemorySchemaRegistry registry) {

        byte[] body = AvroCodec.registered(registry).encode(payload);

        // Both columns are decodes rather than transcriptions, run through the same two calls a
        // reader of this file would make. Nothing here asserts; the assertions live in
        // AvroResolutionTest, which reads the committed file back.
        GenericRecord resolved = AvroCodec.registered(registry, ResolutionOrder.SCHEMA$)
                .decode(body, GenericRecord.class);
        GenericRecord writerShape = AvroCodec.registered(registry).decode(body, GenericRecord.class);

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("case", name);
        entry.put("what", what);
        entry.put("schemaId", schemaId);
        entry.put("writerSchema", writerSchema.toString());
        entry.put("readerSchema", ResolutionOrder.SCHEMA$.toString());
        entry.put("bodyBase64", Base64.getEncoder().encodeToString(body));
        entry.put("resolved", fields(resolved));
        entry.put("writerShape", fields(writerShape));
        return entry;
    }

    /** A decoded record as a map, in schema order, with Avro's Utf8 rendered as a string. */
    private static Map<String, Object> fields(GenericRecord record) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            values.put(field.name(), value instanceof CharSequence ? value.toString() : value);
        }
        return values;
    }
}
