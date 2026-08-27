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
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.DefaultPublisher;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("choosing a format")
class CodecRegistryTest {

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

    /** Stands for an application's own event type. */
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
    @DisplayName("the registry")
    class Registry {

        @Test
        void finds_every_format_on_the_classpath() {
            // json and text and bytes come with the engine; xml and yaml are separate artifacts
            // that announce themselves. Nothing in the core names them.
            assertThat(Codecs.names()).contains("json", "xml", "yaml", "text", "bytes");
        }

        @Test
        void tries_them_in_a_deliberate_order() {
            List<String> order = Codecs.names();

            // JSON first because it is the default and its bytes are recognisable. YAML after
            // JSON because YAML parses JSON quite happily and would answer for it. Text second
            // to last because it claims any text/* at all, and bytes last because it claims
            // anything and returns a byte[] where an object was wanted.
            assertThat(order.indexOf("json")).isLessThan(order.indexOf("yaml"));
            assertThat(order.indexOf("yaml")).isLessThan(order.indexOf("text"));
            assertThat(order.indexOf("text")).isLessThan(order.indexOf("bytes"));
        }

        @Test
        void names_the_artifact_to_add_for_a_format_that_is_not_there() {
            assertThatThrownBy(() -> Codecs.byName("csv"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("no codec named 'csv'")
                    .hasMessageContaining("acemq-amqp-codec-csv")
                    .hasMessageContaining("json");
        }

        @Test
        void publishes_in_json_by_default() {
            assertThat(Codecs.forPublishing().contentType()).isEqualTo("application/json");
            assertThat(Codecs.DEFAULT_FORMAT).isEqualTo("json");
        }

        @Test
        void reads_with_everything_by_default() {
            Codec reading = Codecs.forConsuming();

            assertThat(reading.canDecode("application/json")).isTrue();
            assertThat(reading.canDecode("application/xml")).isTrue();
            assertThat(reading.canDecode("application/yaml")).isTrue();
            assertThat(reading.canDecode("text/plain")).isTrue();
        }
    }

    @Nested
    @DisplayName("publishing an object")
    class Defaults {

        @Test
        @Timeout(20)
        void an_object_goes_out_as_json_and_comes_back_an_object() {
            connect("codec-default");
            List<OrderPlaced> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", OrderPlaced.class,
                    message -> received.add(message.payload()))) {

                // No mention of a format anywhere. This is the case that has to be effortless.
                mq.publisher("orders", "order.placed", OrderPlaced.class)
                        .send(new OrderPlaced("o-1", 4200, Instant.parse("2026-01-02T03:04:05Z")));

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0).getOrderId()).isEqualTo("o-1");
                assertThat(received.get(0).getTotal()).isEqualTo(4200);
                assertThat(received.get(0).getPlacedAt()).isEqualTo(Instant.parse("2026-01-02T03:04:05Z"));
            }
        }

        @Test
        @Timeout(20)
        void a_string_still_travels_as_a_string() {
            connect("codec-default-string");
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()))) {

                mq.publisher("orders", "order.placed", String.class).send("just a string");

                // It goes on the wire as a JSON string, quotes and all, and arrives as what was
                // sent. Nobody publishing text has to know that happened.
                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0)).isEqualTo("just a string");
            }
        }
    }

    @Nested
    @DisplayName("the format an object used to be published as")
    class NoMoreSilentToString {

        @Test
        void the_text_codec_refuses_an_object_rather_than_writing_its_address() {
            Codec text = Codecs.byName("text");

            // This used to produce OrderPlaced@4b1210ee on the wire: published, confirmed, and
            // useless to whoever read it, with nothing anywhere reporting a problem.
            assertThatThrownBy(() -> text.encode(new OrderPlaced("o-1", 1, Instant.now())))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("OrderPlaced")
                    .hasMessageContaining("asJson()");
        }

        @Test
        void the_text_codec_still_writes_the_things_that_are_text() {
            Codec text = Codecs.byName("text");

            assertThat(new String(text.encode("hello"), StandardCharsets.UTF_8)).isEqualTo("hello");
            assertThat(new String(text.encode(42), StandardCharsets.UTF_8)).isEqualTo("42");
            assertThat(new String(text.encode(true), StandardCharsets.UTF_8)).isEqualTo("true");
        }
    }

    @Nested
    @DisplayName("asking for another format")
    class Fluent {

        @Test
        @Timeout(20)
        void a_publisher_told_to_write_xml_writes_xml_and_the_consumer_reads_it() {
            connect("codec-xml");
            List<OrderPlaced> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", OrderPlaced.class,
                    message -> received.add(message.payload()))) {

                mq.publisher("orders", "order.placed", OrderPlaced.class)
                        .asXml()
                        .send(new OrderPlaced("o-xml", 7, Instant.parse("2026-01-02T03:04:05Z")));

                // The consumer was told nothing about XML. It reads the content type and picks
                // the codec, which is why a producer can change format without a consumer change.
                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0).getOrderId()).isEqualTo("o-xml");
                assertThat(received.get(0).getTotal()).isEqualTo(7);
            }
        }

        @Test
        @Timeout(20)
        void a_publisher_told_to_write_yaml_writes_yaml_and_the_consumer_reads_it() {
            connect("codec-yaml");
            List<OrderPlaced> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", OrderPlaced.class,
                    message -> received.add(message.payload()))) {

                mq.publisher("orders", "order.placed", OrderPlaced.class)
                        .asYaml()
                        .send(new OrderPlaced("o-yaml", 9, Instant.parse("2026-01-02T03:04:05Z")));

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0).getOrderId()).isEqualTo("o-yaml");
            }
        }

        @Test
        void yaml_is_written_for_a_person_to_read() {
            String yaml = new String(Codecs.byName("yaml")
                    .encode(new OrderPlaced("o-1", 42, Instant.parse("2026-01-02T03:04:05Z"))), StandardCharsets.UTF_8);

            // Block style with unquoted scalars. Flow style would be JSON with extra steps, and
            // there would be no reason to have chosen YAML.
            assertThat(yaml).contains("orderId: o-1").contains("total: 42").doesNotStartWith("---");
        }

        @Test
        @Timeout(20)
        void three_producers_in_three_formats_reach_one_unchanged_consumer() {
            connect("codec-mixed");
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume(
                    "orders.new", OrderPlaced.class, message -> received.add(message.payload().getOrderId()))) {

                mq.publisher("orders", "order.placed", OrderPlaced.class)
                        .send(new OrderPlaced("from-json", 1, null));
                mq.publisher("orders", "order.placed", OrderPlaced.class)
                        .asXml()
                        .send(new OrderPlaced("from-xml", 2, null));
                mq.publisher("orders", "order.placed", OrderPlaced.class)
                        .asYaml()
                        .send(new OrderPlaced("from-yaml", 3, null));

                // The migration case, in one queue: producers cut over one at a time while the
                // consumer stays exactly as it was.
                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 3);
                assertThat(received).containsExactlyInAnyOrder("from-json", "from-xml", "from-yaml");
                assertThat(consumer.rejected()).isZero();
            }
        }

        @Test
        @Timeout(20)
        void bytes_go_through_untouched() {
            connect("codec-bytes");
            List<byte[]> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", byte[].class,
                    message -> received.add(message.payload()))) {

                byte[] alreadyEncoded = {1, 2, 3, 4, 5};
                mq.publisher("orders", "order.placed", byte[].class).asBytes().send(alreadyEncoded);

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0)).containsExactly(1, 2, 3, 4, 5);
            }
        }

        @Test
        void changing_format_leaves_the_original_publisher_alone() {
            connect("codec-immutable");
            DefaultPublisher<OrderPlaced> json = mq.publisher("orders", "order.placed", OrderPlaced.class);
            DefaultPublisher<OrderPlaced> xml = json.asXml();

            // A new publisher rather than a mutated one: a long-lived object shared between
            // threads that quietly changes what it writes is worse than one that does not.
            assertThat(json).isNotSameAs(xml);
            assertThat(json.codec().contentType()).isEqualTo("application/json");
            assertThat(xml.codec().contentType()).isEqualTo("application/xml");
        }

        @Test
        void a_format_that_is_not_installed_says_which_artifact_to_add() {
            connect("codec-missing");

            assertThatThrownBy(() -> mq.publisher("orders", "order.placed", OrderPlaced.class).as("csv"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("no codec named 'csv'")
                    .hasMessageContaining("acemq-amqp-codec-csv");
        }

        @Test
        @Timeout(20)
        void a_codec_of_the_caller_s_own_needs_no_registration() {
            connect("codec-custom");
            List<String> received = new CopyOnWriteArrayList<>();

            // How a format AceMQ does not ship is used. Avro and Protobuf arrive this way,
            // because neither can be built without a schema.
            Codec shouting = new Codec() {
                @Override
                public String contentType() {
                    return "text/x-shouting";
                }

                @Override
                public byte[] encode(Object payload) {
                    return payload.toString().toUpperCase(java.util.Locale.ROOT).getBytes(StandardCharsets.UTF_8);
                }

                @Override
                @SuppressWarnings("unchecked")
                public <R> R decode(byte[] body, Class<R> target) {
                    return (R) new String(body, StandardCharsets.UTF_8);
                }

                @Override
                public boolean canDecode(String contentType) {
                    return "text/x-shouting".equals(contentType);
                }
            };

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()))) {

                mq.publisher("orders", "order.placed", String.class).as(shouting).send("quietly");

                // Read back by the text codec, which claims text/*: the custom codec was never
                // registered, so nothing on the consumer side knows about it. The bytes are
                // still text, so the message is still readable.
                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received.get(0)).isEqualTo("QUIETLY");
            }
        }
    }
}
