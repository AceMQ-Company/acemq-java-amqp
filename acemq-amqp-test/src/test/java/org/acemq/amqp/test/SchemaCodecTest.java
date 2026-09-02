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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.codec.avro.AvroCodec;
import org.acemq.amqp.codec.avro.InMemorySchemaRegistry;
import org.acemq.amqp.codec.protobuf.ProtobufCodec;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;

@DisplayName("the formats that need a schema")
class SchemaCodecTest {

    /** What a producer writes today. */
    private static final String V1 = "{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"acme\",\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"total\",\"type\":\"int\"}]}";

    /** The same record after somebody adds a field, with a default so old readers cope. */
    private static final String V2 = "{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"acme\",\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"total\",\"type\":\"int\"},"
            + "{\"name\":\"currency\",\"type\":\"string\",\"default\":\"GBP\"}]}";

    private static Schema schema(String json) {
        return new Schema.Parser().parse(json);
    }

    private static GenericRecord order(Schema schema, String id, int total) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("orderId", id);
        record.put("total", total);
        if (schema.getField("currency") != null) {
            record.put("currency", "EUR");
        }
        return record;
    }

    private AceMq mq;

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("why neither has a name")
    class NoNamedForm {

        @Test
        void asking_for_avro_by_name_explains_itself_instead_of_failing_blankly() {
            // Both modules are on this classpath. The error is not "missing artifact"; it is
            // that the format cannot be built without knowing the schema, which is a property of
            // the format rather than of the packaging.
            assertThatThrownBy(() -> Codecs.byName("avro"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("cannot be built without a schema")
                    .hasMessageContaining("AvroCodec.of(schema)");
        }

        @Test
        void and_so_does_protobuf() {
            assertThatThrownBy(() -> Codecs.byName("protobuf"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("cannot be built without a schema")
                    .hasMessageContaining("ProtobufCodec.of(YourMessage.parser())");
        }

        @Test
        void and_neither_answers_for_a_message_that_named_no_format() {
            // Avro and protobuf bytes are not recognisable, and both parse quietly into nonsense
            // more often than they fail. Volunteering would report rubbish as success.
            assertThat(AvroCodec.of(schema(V1)).canDecode(null)).isFalse();
            assertThat(ProtobufCodec.of(StringValue.parser()).canDecode(null)).isFalse();
        }
    }

    @Nested
    @DisplayName("Avro with a fixed schema")
    class FixedSchema {

        @Test
        void carries_a_record_out_and_back() {
            Codec codec = AvroCodec.of(schema(V1));

            byte[] encoded = codec.encode(order(schema(V1), "o-1", 4200));
            GenericRecord decoded = codec.decode(encoded, GenericRecord.class);

            assertThat(decoded.get("orderId")).hasToString("o-1");
            assertThat(decoded.get("total")).isEqualTo(4200);
        }

        @Test
        void is_smaller_than_the_json_for_the_same_record() {
            byte[] avro = AvroCodec.of(schema(V1)).encode(order(schema(V1), "o-1", 4200));

            // The reason to accept everything else about Avro. The field names are in the
            // schema, not in every copy of the message.
            assertThat(avro.length).isLessThan(20);
        }

        @Test
        void announces_a_content_type_that_says_the_schema_is_not_in_the_message() {
            assertThat(AvroCodec.of(schema(V1)).contentType()).isEqualTo("avro/binary");
        }

        @Test
        void refuses_bytes_written_by_a_registered_codec_rather_than_misreading_them() {
            // Regression, and a bad one. The two framings differ by five bytes at the front,
            // and Avro does not notice: reading a registered message with a fixed-schema codec
            // returned orderId="" and total=<garbage> with no exception at all. Every value in
            // the record was wrong and nothing said so.
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry().register(1,
                    AvroCodec.definitionOf(schema(V1)));
            byte[] registered = AvroCodec.registered(registry).encode(order(schema(V1), "o-1", 4200));

            assertThatThrownBy(() -> AvroCodec.of(schema(V1)).decode(registered, GenericRecord.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("schema identifier");
        }

        @Test
        void does_not_volunteer_for_the_other_framings_content_type() {
            // The check above only helps a caller who reached decode directly. A consumer picks
            // its codec by content type first, so that path has to refuse as well.
            assertThat(AvroCodec.of(schema(V1)).canDecode("application/vnd.acemq.avro")).isFalse();
            assertThat(AvroCodec.of(schema(V1)).canDecode("avro/binary")).isTrue();
        }
    }

    @Nested
    @DisplayName("Avro from a generated class")
    class Specific {

        @Test
        void carries_a_generated_record_out_and_back() {
            // AvroCodec.of(Class) had no test at all until this one. It is the same decode
            // path whose generic half returned an empty id and a total of 5.4e-67 before
            // 0.2.5, so leaving the specific half uncovered was the least comfortable gap
            // in this module.
            Codec codec = AvroCodec.of(org.acemq.amqp.test.avro.TestOrder.class);

            byte[] encoded = codec.encode(new org.acemq.amqp.test.avro.TestOrder("o-7", 4200));
            org.acemq.amqp.test.avro.TestOrder decoded = codec.decode(encoded,
                    org.acemq.amqp.test.avro.TestOrder.class);

            assertThat(decoded.id()).isEqualTo("o-7");
            assertThat(decoded.total()).isEqualTo(4200);
        }

        @Test
        void takes_its_schema_from_the_class_rather_than_being_told_one() {
            Codec fromClass = AvroCodec.of(org.acemq.amqp.test.avro.TestOrder.class);
            Codec fromSchema = AvroCodec.of(org.acemq.amqp.test.avro.TestOrder.SCHEMA$);

            // Same schema, so the bytes are identical: the class is only a carrier for it.
            assertThat(fromClass.encode(new org.acemq.amqp.test.avro.TestOrder("o-1", 1)))
                    .isEqualTo(fromSchema.encode(new org.acemq.amqp.test.avro.TestOrder("o-1", 1)));
            assertThat(fromClass.contentType()).isEqualTo("avro/binary");
        }

        @Test
        void a_class_that_is_not_a_generated_record_says_so() {
            // The failure a caller actually hits: passing something that looks close enough.
            assertThatThrownBy(() -> AvroCodec.of(NotGenerated.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("no-argument constructor");
        }

        @Test
        void a_generated_class_still_refuses_registered_bytes() {
            // The specific path shares the framing check with the generic one. Without this
            // it would be the only decode path where the 0.2.5 fix is unproven.
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                    .register(1, AvroCodec.definitionOf(org.acemq.amqp.test.avro.TestOrder.SCHEMA$));
            byte[] registered = AvroCodec.registered(registry)
                    .encode(new org.acemq.amqp.test.avro.TestOrder("o-2", 2));

            assertThatThrownBy(() -> AvroCodec.of(org.acemq.amqp.test.avro.TestOrder.class)
                    .decode(registered, org.acemq.amqp.test.avro.TestOrder.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("schema identifier");
        }
    }

    /** Deliberately not something the Avro compiler produced. */
    abstract static class NotGenerated implements org.apache.avro.specific.SpecificRecord {
    }

    @Nested
    @DisplayName("Avro with a registry")
    class Registered {

        @Test
        void accepts_its_own_framing_and_not_the_fixed_one() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry().register(1,
                    AvroCodec.definitionOf(schema(V1)));
            Codec codec = AvroCodec.registered(registry);

            assertThat(codec.canDecode("application/vnd.acemq.avro")).isTrue();
            // The mirror of the fixed codec's refusal: a registered codec reading unframed
            // bytes would take the first five bytes of the record as an identifier.
            assertThat(codec.canDecode("avro/binary")).isFalse();
        }

        @Test
        void puts_the_schema_identifier_on_the_front_where_confluent_clients_look_for_it() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry().register(7,
                    AvroCodec.definitionOf(schema(V1)));

            byte[] encoded = AvroCodec.registered(registry).encode(order(schema(V1), "o-1", 1));

            // Magic zero, then four bytes of identifier, big-endian. Wire-compatible with
            // Confluent's clients, which is what lets a Java producer here be read by a
            // consumer somebody else wrote years ago.
            assertThat(encoded[0]).isZero();
            assertThat(encoded[1]).isZero();
            assertThat(encoded[2]).isZero();
            assertThat(encoded[3]).isZero();
            assertThat(encoded[4]).isEqualTo((byte) 7);
        }

        @Test
        void a_consumer_keeps_working_when_a_producer_adds_a_field() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                    .register(1, AvroCodec.definitionOf(schema(V1))).register(2,
                            AvroCodec.definitionOf(schema(V2)));

            // Producer has been redeployed and writes the new record. The consumer has not
            // changed: it holds the same registered codec it always did.
            byte[] fromNewProducer = AvroCodec.registered(registry).encode(order(schema(V2), "o-9", 500));

            GenericRecord decoded = AvroCodec.registered(registry).decode(fromNewProducer, GenericRecord.class);

            // The fields it already knew are still right, which is the property that matters:
            // the added field did not shift everything after it.
            assertThat(decoded.get("orderId")).hasToString("o-9");
            assertThat(decoded.get("total")).isEqualTo(500);
            assertThat(decoded.get("currency")).hasToString("EUR");
        }

        @Test
        void a_reader_schema_drops_a_field_the_consumer_has_never_heard_of() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                    .register(1, AvroCodec.definitionOf(schema(V1)))
                    .register(2, AvroCodec.definitionOf(schema(V2)));

            // The producer has been redeployed and writes the field. This consumer was
            // written against V1 and has not been.
            byte[] fromNewProducer = AvroCodec.registered(registry).encode(order(schema(V2), "o-9", 500));

            GenericRecord decoded = AvroCodec.registered(registry, schema(V1))
                    .decode(fromNewProducer, GenericRecord.class);

            assertThat(decoded.get("orderId")).hasToString("o-9");
            assertThat(decoded.get("total")).isEqualTo(500);
            // Skipped, not shifted. The consumer sees the shape it was written against.
            assertThat(decoded.getSchema().getField("currency")).isNull();
        }

        @Test
        void a_reader_schema_fills_in_a_default_for_a_field_the_writer_did_not_send() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                    .register(1, AvroCodec.definitionOf(schema(V1)))
                    .register(2, AvroCodec.definitionOf(schema(V2)));

            // The other direction, and the one that lets consumers be deployed first: a
            // producer still on V1, read by a consumer already on V2.
            byte[] fromOldProducer = AvroCodec.registered(registry).encode(order(schema(V1), "o-3", 25));

            GenericRecord decoded = AvroCodec.registered(registry, schema(V2))
                    .decode(fromOldProducer, GenericRecord.class);

            assertThat(decoded.get("orderId")).hasToString("o-3");
            assertThat(decoded.get("currency")).hasToString("GBP");
        }

        @Test
        void a_generic_reader_adopts_the_writers_schema_rather_than_resolving_against_its_own() {
            // Worth stating, because it is the limit of what a GenericRecord can express. Avro's
            // reader-schema resolution needs a reader schema, and a GenericRecord asks for
            // nothing in particular -- so the writer's schema is used for both and a field the
            // "old" consumer does not know still arrives. Dropping it, or filling in a default
            // for a field the writer omitted, needs a generated SpecificRecord whose class
            // carries the reader schema.
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                    .register(2, AvroCodec.definitionOf(schema(V2)));
            byte[] written = AvroCodec.registered(registry).encode(order(schema(V2), "o-9", 500));

            GenericRecord decoded = AvroCodec.registered(registry).decode(written, GenericRecord.class);

            assertThat(decoded.getSchema().getField("currency")).isNotNull();
        }

        @Test
        void refuses_bytes_that_have_no_identifier_on_them() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry().register(1,
                    AvroCodec.definitionOf(schema(V1)));
            byte[] withoutHeader = AvroCodec.of(schema(V1)).encode(order(schema(V1), "o-1", 1));

            assertThatThrownBy(() -> AvroCodec.registered(registry).decode(withoutHeader, GenericRecord.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("no schema identifier");
        }

        @Test
        void says_so_when_it_has_never_seen_the_identifier_a_message_carries() {
            InMemorySchemaRegistry writer = new InMemorySchemaRegistry().register(42,
                    AvroCodec.definitionOf(schema(V1)));
            byte[] written = AvroCodec.registered(writer).encode(order(schema(V1), "o-1", 1));

            // A different process, which is exactly when an in-memory registry stops working.
            InMemorySchemaRegistry elsewhere = new InMemorySchemaRegistry();
            assertThatThrownBy(() -> AvroCodec.registered(elsewhere).decode(written, GenericRecord.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("does not know schema id 42");
        }

        @Test
        void will_not_quietly_reassign_an_identifier_to_a_different_schema() {
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry().register(1,
                    AvroCodec.definitionOf(schema(V1)));

            // Overwriting would make every message already written under this identifier decode
            // as something else, which is worse than refusing to start.
            assertThatThrownBy(() -> registry.register(1, AvroCodec.definitionOf(schema(
                    "{\"type\":\"record\",\"name\":\"Other\",\"fields\":[]}"))))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("already registered");
            assertThat(registry.size()).isEqualTo(1);
        }

        @Test
        @Timeout(20)
        void reaches_a_consumer_through_a_broker() {
            connect("avro-roundtrip");
            InMemorySchemaRegistry registry = new InMemorySchemaRegistry().register(1,
                    AvroCodec.definitionOf(schema(V1)));
            List<String> received = new CopyOnWriteArrayList<>();

            // The consumer has to be told, and this is the one place that is true. Avro bytes
            // say nothing about themselves, so nothing on the reading side could work it out.
            try (MessageConsumer consumer = mq.consume("orders.new", GenericRecord.class,
                    ConsumerOptions.defaults().as(AvroCodec.registered(registry)),
                    message -> received.add(message.payload().get("orderId").toString()))) {

                mq.publisher("orders", "order.placed", GenericRecord.class)
                        .as(AvroCodec.registered(registry))
                        .send(order(schema(V1), "o-avro", 77));

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received).containsExactly("o-avro");
                assertThat(consumer.rejected()).isZero();
            }
        }
    }

    @Nested
    @DisplayName("Protobuf")
    class Protobuf {

        @Test
        void carries_a_generated_message_out_and_back() {
            Codec codec = ProtobufCodec.of(StringValue.parser());

            byte[] encoded = codec.encode(StringValue.of("o-1"));

            assertThat(codec.decode(encoded, StringValue.class).getValue()).isEqualTo("o-1");
        }

        @Test
        void can_be_built_from_the_class_as_well_as_the_parser() {
            Codec codec = ProtobufCodec.of(Timestamp.class);
            Timestamp when = Timestamp.newBuilder().setSeconds(1767326645).build();

            assertThat(codec.decode(codec.encode(when), Timestamp.class).getSeconds()).isEqualTo(1767326645);
            assertThat(codec.toString()).contains("Timestamp");
        }

        @Test
        void refuses_an_ordinary_object_and_says_what_to_do_instead() {
            assertThatThrownBy(() -> ProtobufCodec.of(StringValue.parser()).encode("just a string"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("asJson()");
        }

        @Test
        void refuses_a_class_protoc_did_not_generate() {
            assertThatThrownBy(() -> ProtobufCodec.of(BadMessage.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("static parser()");
        }

        @Test
        void says_so_when_asked_for_a_type_it_does_not_parse() {
            Codec codec = ProtobufCodec.of(StringValue.parser());
            byte[] encoded = codec.encode(StringValue.of("o-1"));

            // Protobuf parses bytes meant for one message as another whenever the field numbers
            // line up, so this check is the difference between an error and a wrong answer.
            assertThatThrownBy(() -> codec.decode(encoded, Timestamp.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("oneof");
        }

        @Test
        @Timeout(20)
        void reaches_a_consumer_through_a_broker() {
            connect("protobuf-roundtrip");
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", StringValue.class,
                    ConsumerOptions.defaults().as(ProtobufCodec.of(StringValue.parser())),
                    message -> received.add(message.payload().getValue()))) {

                mq.publisher("orders", "order.placed", StringValue.class)
                        .as(ProtobufCodec.of(StringValue.parser()))
                        .send(StringValue.of("o-proto"));

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received).containsExactly("o-proto");
                assertThat(consumer.rejected()).isZero();
            }
        }
    }

    /** Deliberately not a protobuf message. */
    @SuppressWarnings("unused")
    abstract static class BadMessage implements com.google.protobuf.Message {
    }
}
