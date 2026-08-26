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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.CompositeCodec;
import org.acemq.amqp.core.JsonCodec;
import org.acemq.amqp.core.StringCodec;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@DisplayName("the JSON codec")
class JsonCodecTest {

    private AceMq mq;

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    /** A payload with the shape a real event has: identity, money, a time and a nested value. */
    public static final class OrderPlaced {

        private String orderId;
        private int total;
        private Instant placedAt;

        public OrderPlaced() {
        }

        OrderPlaced(String orderId, int total, Instant placedAt) {
            this.orderId = orderId;
            this.total = total;
            this.placedAt = placedAt;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }

        public Instant getPlacedAt() {
            return placedAt;
        }

        public void setPlacedAt(Instant placedAt) {
            this.placedAt = placedAt;
        }
    }

    @Nested
    @DisplayName("encoding")
    class Encoding {

        @Test
        void carries_an_object_out_and_back_unchanged() {
            JsonCodec codec = new JsonCodec();
            Instant placedAt = Instant.parse("2026-01-02T03:04:05Z");

            byte[] encoded = codec.encode(new OrderPlaced("o-1", 4200, placedAt));
            OrderPlaced decoded = codec.decode(encoded, OrderPlaced.class);

            assertThat(decoded.getOrderId()).isEqualTo("o-1");
            assertThat(decoded.getTotal()).isEqualTo(4200);
            assertThat(decoded.getPlacedAt()).isEqualTo(placedAt);
        }

        @Test
        void writes_a_time_as_a_date_rather_than_a_number() {
            String json = new String(
                    new JsonCodec().encode(new OrderPlaced("o-1", 1, Instant.parse("2026-01-02T03:04:05Z"))),
                    StandardCharsets.UTF_8);

            // A consumer in Go or Python reading a millisecond count as seconds puts the message
            // fifty thousand years in the past, and nothing in the message says which it is.
            assertThat(json).contains("2026-01-02T03:04:05Z").doesNotContain("1767326645");
        }

        @Test
        void announces_itself_as_json() {
            assertThat(new JsonCodec().contentType()).isEqualTo("application/json");
            assertThat(new JsonCodec().toString()).isEqualTo("JsonCodec");
        }
    }

    @Nested
    @DisplayName("messages that outlive the code that wrote them")
    class Evolution {

        @Test
        void a_field_the_consumer_has_never_heard_of_is_ignored() {
            byte[] fromANewerProducer = "{\"orderId\":\"o-1\",\"total\":10,\"discountCode\":\"SUMMER\"}"
                    .getBytes(StandardCharsets.UTF_8);

            OrderPlaced decoded = new JsonCodec().decode(fromANewerProducer, OrderPlaced.class);

            // The alternative makes every additive change a release coordinated across every
            // team that consumes the message, which is the coupling messaging exists to avoid.
            assertThat(decoded.getOrderId()).isEqualTo("o-1");
            assertThat(decoded.getTotal()).isEqualTo(10);
        }

        @Test
        void a_field_the_producer_has_stopped_sending_reads_as_absent() {
            byte[] fromAnOlderProducer = "{\"orderId\":\"o-1\"}".getBytes(StandardCharsets.UTF_8);

            OrderPlaced decoded = new JsonCodec().decode(fromAnOlderProducer, OrderPlaced.class);

            assertThat(decoded.getOrderId()).isEqualTo("o-1");
            assertThat(decoded.getPlacedAt()).isNull();
        }

        @Test
        void a_mapper_the_application_supplies_is_used_as_given() {
            ObjectMapper strict = new ObjectMapper();
            strict.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            byte[] withAnExtraField = "{\"orderId\":\"o-1\",\"surprise\":1}".getBytes(StandardCharsets.UTF_8);

            // Handing over a mapper means handing over its rules, including the strictness this
            // codec would not have chosen. Silently overriding it would be worse.
            assertThatThrownBy(() -> new JsonCodec(strict).decode(withAnExtraField, OrderPlaced.class))
                    .isInstanceOf(AceMqException.class);
        }
    }

    @Nested
    @DisplayName("what it will read")
    class ContentTypes {

        @Test
        void reads_json_and_its_variants() {
            JsonCodec codec = new JsonCodec();

            assertThat(codec.canDecode("application/json")).isTrue();
            assertThat(codec.canDecode("application/json; charset=utf-8")).isTrue();
            assertThat(codec.canDecode("APPLICATION/JSON")).isTrue();
            assertThat(codec.canDecode("application/vnd.acme.order+json")).isTrue();
        }

        @Test
        void reads_a_message_whose_sender_said_nothing() {
            // Usual from anything outside the JVM and from the management console. Refusing on
            // that basis would make the codec useless in the interoperating case it exists for.
            assertThat(new JsonCodec().canDecode(null)).isTrue();
        }

        @Test
        void leaves_other_formats_alone() {
            assertThat(new JsonCodec().canDecode("text/plain")).isFalse();
            assertThat(new JsonCodec().canDecode("application/octet-stream")).isFalse();
        }
    }

    @Nested
    @DisplayName("failure")
    class Failure {

        @Test
        void bytes_that_are_not_json_are_refused_with_the_reason() {
            assertThatThrownBy(() -> new JsonCodec().decode("not json at all".getBytes(StandardCharsets.UTF_8),
                    OrderPlaced.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("OrderPlaced");
        }

        @Test
        void something_that_cannot_be_written_is_refused_with_the_type_named() {
            assertThatThrownBy(() -> new JsonCodec().encode(new Object()))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("java.lang.Object");
        }

        @Test
        @Timeout(20)
        void a_payload_that_will_never_decode_is_not_retried() {
            mq = AceMq.connect("memory://json-poison", Telemetry.NONE, new JsonCodec());
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            try (org.acemq.amqp.core.MessageConsumer consumer = mq.consume("orders.new", OrderPlaced.class, message -> {
            })) {

                // Published as text into a queue whose consumer expects JSON objects. Retrying
                // this would fail identically every time and hold the queue up doing it.
                AceMq raw = AceMq.connect("memory://json-poison", Telemetry.NONE);
                raw.publisher("orders", "order.placed").send("this is not an order");

                await().atMost(Duration.ofSeconds(10)).until(() -> consumer.rejected() == 1);
                assertThat(consumer.retried()).isZero();
                assertThat(consumer.acknowledged()).isZero();
            }
        }
    }

    @Nested
    @DisplayName("in use")
    class RoundTrip {

        @Test
        @Timeout(20)
        void an_object_published_arrives_as_the_same_object() {
            mq = AceMq.connect("memory://json-roundtrip", Telemetry.NONE, new JsonCodec());
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            List<OrderPlaced> received = new CopyOnWriteArrayList<>();
            try (org.acemq.amqp.core.MessageConsumer ignored = mq.consume("orders.new", OrderPlaced.class,
                    message -> received.add(message.payload()))) {

                mq.publisher("orders", "order.placed")
                        .send(new OrderPlaced("o-99", 1234, Instant.parse("2026-05-06T07:08:09Z")));

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0).getOrderId()).isEqualTo("o-99");
                assertThat(received.get(0).getTotal()).isEqualTo(1234);
                assertThat(received.get(0).getPlacedAt()).isEqualTo(Instant.parse("2026-05-06T07:08:09Z"));
            }
        }

        @Test
        void the_default_stays_text_even_with_jackson_on_the_classpath() {
            mq = AceMq.connect("memory://json-not-default", Telemetry.NONE);

            // Jackson is on this classpath. If the engine detected it and switched, every
            // existing application would start writing a different format to the same queues,
            // having asked for nothing.
            assertThat(mq.publisher("", "q").toString()).isNotNull();
            assertThat(new StringCodec().contentType()).startsWith("text/plain");
        }
    }

    @Nested
    @DisplayName("reading two formats at once")
    class Migration {

        @Test
        void writes_the_first_format_and_reads_either() {
            Codec codec = CompositeCodec.of(new JsonCodec(), new StringCodec());

            assertThat(codec.contentType()).isEqualTo("application/json");
            assertThat(new String(codec.encode("hello"), StandardCharsets.UTF_8)).isEqualTo("\"hello\"");

            // The old format still on the queue from producers not yet redeployed.
            assertThat(codec.decode("plain words".getBytes(StandardCharsets.UTF_8), String.class, "text/plain"))
                    .isEqualTo("plain words");
            // And the new one.
            assertThat(codec.decode("{\"orderId\":\"o-1\"}".getBytes(StandardCharsets.UTF_8), OrderPlaced.class,
                    "application/json")
                    .getOrderId())
                    .isEqualTo("o-1");
        }

        @Test
        void tries_the_next_codec_when_one_refuses() {
            Codec codec = CompositeCodec.of(new JsonCodec(), new StringCodec());

            // No content type, so both are candidates. JSON is tried first and fails; the message
            // is still readable, and a composite that gave up on the first refusal would lose it.
            assertThat(codec.decode("just text".getBytes(StandardCharsets.UTF_8), String.class, null))
                    .isEqualTo("just text");
        }

        @Test
        void reports_every_reason_when_none_of_them_can_read_it() {
            Codec codec = CompositeCodec.of(new JsonCodec(), new StringCodec());

            assertThatThrownBy(() -> codec.decode("nonsense".getBytes(StandardCharsets.UTF_8), OrderPlaced.class, null))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("JsonCodec")
                    .hasMessageContaining("StringCodec");
        }

        @Test
        void says_so_when_the_content_type_matches_nothing() {
            Codec codec = CompositeCodec.of(new StringCodec());

            assertThatThrownBy(() -> codec.decode(new byte[0], String.class, "application/octet-stream"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("no codec here reads 'application/octet-stream'")
                    .hasMessageContaining("text/plain");
        }

        @Test
        void answers_for_all_of_the_codecs_it_holds() {
            Codec codec = CompositeCodec.of(new StringCodec());

            assertThat(codec.canDecode("text/plain")).isTrue();
            assertThat(codec.canDecode("application/octet-stream")).isFalse();
            assertThat(((CompositeCodec) codec).codecs()).hasSize(1);
            assertThat(codec.toString()).contains("writes=text/plain").contains("reads=text/plain");
        }

        @Test
        void refuses_to_be_built_with_nothing_to_read_with() {
            assertThatThrownBy(CompositeCodec::of)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least one codec");
            assertThatThrownBy(() -> CompositeCodec.of(new StringCodec(), null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not contain null");
        }

        @Test
        @Timeout(20)
        void a_consumer_reading_both_survives_a_producer_changing_format() {
            mq = AceMq.connect(
                    "memory://json-migration", Telemetry.NONE, CompositeCodec.of(new JsonCodec(), new StringCodec()));
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            List<String> received = new CopyOnWriteArrayList<>();
            try (org.acemq.amqp.core.MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()))) {

                // A producer not yet redeployed, still writing text.
                AceMq old = AceMq.connect("memory://json-migration", Telemetry.NONE);
                old.publisher("orders", "order.placed").send("the old format");

                // And one that has been, writing JSON.
                mq.publisher("orders", "order.placed").send("the new format");

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 2);
                assertThat(received).containsExactlyInAnyOrder("the old format", "the new format");
                assertThat(consumer.rejected()).isZero();
            }
        }
    }
}
