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
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The optional claim an application sets to say where a payload is, when it stores it outside
 * the message.
 *
 * <p>Java reserved {@code x-acemq-claim} in {@link AceHeaders} and materialised nothing, so the
 * engine's reserved-prefix filter dropped it on the way in: a claim set by a Go, .NET, Python or
 * Ruby publisher arrived here and vanished, with nothing reporting the loss. These tests are the
 * evidence that it no longer does.
 *
 * <p><strong>Not the claim-check pattern.</strong> {@code ClaimCheckCodec} frames its reference
 * in the body and sets no header at all, because a header can be stripped by a shovel or a
 * federation link. That is tested by {@code ClaimCheckTest} and is deliberately untouched here.
 */
@DisplayName("the envelope's claim")
class EnvelopeClaimTest {

    private AceMq mq;

    private AceMq connect(String brokerName) {
        mq = AceMq.connect("memory://" + brokerName, Telemetry.NONE);
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @AfterEach
    void disconnect() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("on the wire")
    class OnTheWire {

        @Test
        @Timeout(10)
        @DisplayName("a claim is written to x-acemq-claim")
        void a_claim_is_written_to_the_reserved_header() {
            connect("claim-write");

            try (TransportConnection raw = new InMemoryTransport()
                    .connect(ConnectionConfig.url("memory://claim-write").build())) {

                mq.publisher("orders", "order.placed", String.class)
                        .send("{\"id\":\"o-1\"}",
                                Envelope.of("order.placed").claim("s3://payloads/o-1").build());

                assertThat(headersOf(raw))
                        .containsEntry(AceHeaders.CLAIM, "s3://payloads/o-1");
            }
        }

        @Test
        @Timeout(10)
        @DisplayName("no claim writes no header at all, rather than an empty one")
        void an_absent_claim_writes_nothing() {
            connect("claim-absent");

            try (TransportConnection raw = new InMemoryTransport()
                    .connect(ConnectionConfig.url("memory://claim-absent").build())) {

                mq.publisher("orders", "order.placed", String.class).send("{\"id\":\"o-1\"}");

                assertThat(headersOf(raw)).doesNotContainKey(AceHeaders.CLAIM);
            }
        }

        @Test
        @Timeout(10)
        @DisplayName("an empty claim is absent, not an empty header")
        void an_empty_claim_is_the_same_as_none() {
            connect("claim-empty");

            try (TransportConnection raw = new InMemoryTransport()
                    .connect(ConnectionConfig.url("memory://claim-empty").build())) {

                mq.publisher("orders", "order.placed", String.class)
                        .send("{\"id\":\"o-1\"}", Envelope.of("order.placed").claim("").build());

                // Absent rather than empty, which is what Go, .NET, Python and Ruby all write. A
                // header carrying "" is a header somebody has to special-case at the other end.
                assertThat(headersOf(raw)).doesNotContainKey(AceHeaders.CLAIM);
            }
        }

        private java.util.Map<String, Object> headersOf(TransportConnection raw) {
            InboundDelivery delivery = raw.receive("orders.new", Duration.ofSeconds(5))
                    .orElseThrow(() -> new IllegalStateException("nothing on the queue"))
                    .delivery();
            return delivery.headers();
        }
    }

    @Nested
    @DisplayName("on the way in")
    class OnTheWayIn {

        @Test
        @Timeout(10)
        @DisplayName("a claim set by another library reaches the handler")
        void a_foreign_publishers_claim_is_materialised() {
            connect("claim-foreign");
            List<Message<String>> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, received::add);
                    TransportConnection raw = new InMemoryTransport()
                            .connect(ConnectionConfig.url("memory://claim-foreign").build())) {

                // Raw rather than through a publisher: this is the message a Python or Ruby
                // service puts on the broker, and the whole point is that Java did not read it.
                // A JSON string, because the consumer decodes String.class -- the same bytes a
                // publisher of String.class puts on the wire.
                String id = UUID.randomUUID().toString();
                raw.send(OutboundMessage.body("\"o-1\"".getBytes(StandardCharsets.UTF_8))
                        .exchange("orders")
                        .routingKey("order.placed")
                        .messageId(id)
                        .contentType("application/json")
                        .header(AceHeaders.ID, id)
                        .header(AceHeaders.TYPE, "order.placed")
                        .header(AceHeaders.CORRELATION, id)
                        .header(AceHeaders.CLAIM, "s3://payloads/orders/o-1")
                        .build());

                await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());

                Envelope envelope = received.get(0).envelope();
                assertThat(envelope.claim())
                        .as("a claim written by another library must survive to the handler")
                        .contains("s3://payloads/orders/o-1");
                assertThat(envelope.headers())
                        .as("and must not also appear as an application header, which would let the"
                                + " two representations drift apart")
                        .doesNotContainKey(AceHeaders.CLAIM);
            }
        }

        @Test
        @Timeout(10)
        @DisplayName("a message without one reports no claim")
        void no_claim_reads_as_empty() {
            connect("claim-none");
            List<Message<String>> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, received::add)) {
                mq.publisher("orders", "order.placed", String.class).send("{\"id\":\"o-1\"}");

                await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());

                assertThat(received.get(0).envelope().claim()).isEmpty();
            }
        }

        @Test
        @Timeout(10)
        @DisplayName("a claim survives a publish and consume by this library")
        void a_claim_round_trips() {
            connect("claim-roundtrip");
            List<Message<String>> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, received::add)) {
                mq.publisher("orders", "order.placed", String.class)
                        .send("{\"id\":\"o-1\"}",
                                Envelope.of("order.placed").claim("s3://payloads/o-1").build());

                await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());

                assertThat(received.get(0).envelope().claim()).contains("s3://payloads/o-1");
            }
        }
    }
}
