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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The in-memory transport, exercised through the same public API a real broker is.
 *
 * <p>These assertions deliberately mirror the RabbitMQ integration test. Where they agree, the
 * fake is a fair substitute; where they cannot, the capability set says why.
 */
@DisplayName("the in-memory transport")
class InMemoryTransportTest {

    private AceMq mq;

    /**
     * Sets up the standard fixture.
     *
     * <p>The queue type is stated explicitly because {@link AceMq#declareQueue(String)}
     * defaults to quorum, which this transport does not provide and correctly refuses. That
     * refusal is the design working: a fake that quietly accepted a quorum declaration would
     * let code pass here and fail against a broker that means it.
     */
    private AceMq connect(String brokerName) {
        mq = AceMq.connect("memory://" + brokerName);
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

    @Test
    @Timeout(10)
    void is_selected_by_the_url_scheme() {
        connect("scheme");

        assertThat(mq.transportName()).isEqualTo("in-memory");
    }

    @Test
    @Timeout(10)
    void claims_only_what_it_actually_implements() {
        connect("capabilities");

        assertThat(mq.capabilities())
                .containsExactlyInAnyOrder(
                        Capability.EXCHANGE_ROUTING,
                        Capability.TOPIC_WILDCARDS,
                        Capability.PUBLISHER_CONFIRMS,
                        // Queue time-to-live with a dead-letter target is genuinely
                        // implemented, because the retry ladder is built on it.
                        Capability.DEAD_LETTER_NATIVE);
        // Overstating these is how a fake lets broken code pass. Anything depending on them
        // must fail here exactly as it would against a broker that lacks them.
        assertThat(mq.capabilities())
                .doesNotContain(Capability.QUORUM_QUEUES, Capability.DELAYED_DELIVERY, Capability.STREAMS);
    }

    @Test
    @Timeout(10)
    void refuses_a_quorum_queue_it_cannot_provide() {
        mq = AceMq.connect("memory://quorum");

        assertThatThrownBy(() -> mq.declareQueue("orders.new"))
                .isInstanceOf(org.acemq.amqp.api.AceMqException.class)
                .hasMessageContaining("does not support quorum queues");
    }

    @Nested
    @DisplayName("round trip")
    class RoundTrip {

        @Test
        @Timeout(10)
        void carries_the_envelope_from_publisher_to_handler() {
            connect("roundtrip");
            List<Message<String>> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, received::add)) {
                Envelope sent = Envelope.of("order.placed")
                        .correlationId("flow-1")
                        .header("tenant", "acme")
                        .build();

                mq.publisher("orders", "order.placed").send("{\"id\":\"o-1\"}", sent);

                await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());

                Message<String> message = received.get(0);
                assertThat(message.payload()).isEqualTo("{\"id\":\"o-1\"}");
                assertThat(message.queue()).isEqualTo("orders.new");
                assertThat(message.routingKey()).contains("order.placed");
                assertThat(message.attempt()).isEqualTo(1);
                assertThat(message.envelope().id()).isEqualTo(sent.id());
                assertThat(message.envelope().correlationId()).isEqualTo("flow-1");
                assertThat(message.envelope().headers()).containsEntry("tenant", "acme");
                assertThat(message.envelope().origin()).isPresent();

                await().atMost(Duration.ofSeconds(5)).until(() -> consumer.acknowledged() == 1);
                assertThat(consumer.rejected()).isZero();
            }
        }

        @Test
        @Timeout(10)
        void reports_an_unroutable_publish_as_a_failure() {
            connect("unroutable");

            assertThatThrownBy(() -> mq.publisher("orders", "nothing.bound").send("lost?"))
                    .isInstanceOf(PublishFailedException.class)
                    .hasMessageContaining("could not");
        }

        @Test
        @Timeout(10)
        void delivers_every_message_of_a_batch() {
            connect("batch");
            AtomicInteger handled = new AtomicInteger();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> handled.incrementAndGet())) {
                for (int i = 0; i < 100; i++) {
                    mq.publisher("orders", "order.placed").send("message-" + i);
                }

                await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 100);
                await().atMost(Duration.ofSeconds(5)).until(() -> consumer.acknowledged() == 100);
            }
        }
    }

    @Nested
    @DisplayName("routing")
    class Routing {

        @ParameterizedTest(name = "binding {0} against key {1} matches: {2}")
        @CsvSource({
                "order.*,      order.placed,          true",
                "order.*,      order.placed.eu,       false",
                "order.#,      order.placed.eu,       true",
                "order.#,      order,                 true",
                "#,            anything.at.all,       true",
                "*.placed,     order.placed,          true",
                "*.placed,     order.cancelled,       false",
                "order.placed, order.placed,          true",
        })
        @Timeout(10)
        void applies_amqp_topic_rules(String bindingKey, String routingKey, boolean shouldMatch) {
            mq = AceMq.connect("memory://topic-" + bindingKey.hashCode() + "-" + routingKey.hashCode());
            mq.declareExchange("topic-test", "topic");
            mq.declareQueue("bound", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("bound", "topic-test", bindingKey);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                mq.publisher("topic-test", routingKey).send("payload");
            } catch (Throwable t) {
                failure.set(t);
            }

            if (shouldMatch) {
                assertThat(failure.get()).as("binding '%s' should match '%s'", bindingKey, routingKey).isNull();
            } else {
                assertThat(failure.get())
                        .as("binding '%s' should not match '%s'", bindingKey, routingKey)
                        .isInstanceOf(PublishFailedException.class);
            }
        }

        @Test
        @Timeout(10)
        void a_fanout_exchange_reaches_every_bound_queue() {
            mq = AceMq.connect("memory://fanout");
            mq.declareExchange("events", "fanout");
            mq.declareQueue("audit", QueueType.CLASSIC, Collections.emptyMap());
            mq.declareQueue("search", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("audit", "events", "");
            mq.bind("search", "events", "");

            CountDownLatch both = new CountDownLatch(2);
            try (MessageConsumer auditor = mq.consume("audit", String.class, m -> both.countDown());
                    MessageConsumer indexer = mq.consume("search", String.class, m -> both.countDown())) {

                mq.publisher("events", "ignored.by.fanout").send("event");

                assertThat(awaitLatch(both)).as("both queues should receive the message").isTrue();
            }
        }

        @Test
        @Timeout(10)
        void refuses_to_redeclare_an_exchange_with_a_different_type() {
            mq = AceMq.connect("memory://redeclare");
            mq.declareExchange("orders", "topic");

            assertThatThrownBy(() -> mq.declareExchange("orders", "direct"))
                    .hasMessageContaining("cannot be redeclared");
        }
    }

    @Nested
    @DisplayName("settlement")
    class Settlement {

        @Test
        @Timeout(10)
        void rejects_a_message_whose_handler_fails() {
            connect("failing-handler");
            AtomicInteger attempts = new AtomicInteger();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, ConsumerOptions.prefetch(1), m -> {
                attempts.incrementAndGet();
                throw new IllegalStateException("downstream is unreachable");
            })) {
                mq.publisher("orders", "order.placed").send("payload");

                await().atMost(Duration.ofSeconds(5)).until(() -> consumer.rejected() == 1);
                assertThat(consumer.acknowledged()).isZero();
                assertThat(attempts).hasValue(1);
            }
        }

        @Test
        @Timeout(10)
        void rejects_a_payload_that_cannot_be_decoded_without_running_the_handler() {
            connect("undecodable");

            try (MessageConsumer consumer = mq.consume("orders.new", Integer.class, m -> {
                throw new AssertionError("the handler must not run when decoding fails");
            })) {
                mq.publisher("orders", "order.placed").send("not a number");

                await().atMost(Duration.ofSeconds(5)).until(() -> consumer.rejected() == 1);
            }
        }

        @Test
        @Timeout(10)
        void rejects_a_fatal_failure_without_requeue() {
            connect("fatal");

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, m -> {
                throw new AceFatalException("never valid for this handler");
            })) {
                mq.publisher("orders", "order.placed").send("payload");

                await().atMost(Duration.ofSeconds(5)).until(() -> consumer.rejected() == 1);
            }
        }

        @Test
        @Timeout(20)
        void honours_prefetch_by_holding_deliveries_until_earlier_ones_settle() {
            connect("prefetch");
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger highWaterMark = new AtomicInteger();
            CountDownLatch allHandled = new CountDownLatch(20);

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, ConsumerOptions.prefetch(1), m -> {
                int current = inFlight.incrementAndGet();
                highWaterMark.accumulateAndGet(current, Math::max);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                inFlight.decrementAndGet();
                allHandled.countDown();
            })) {
                for (int i = 0; i < 20; i++) {
                    mq.publisher("orders", "order.placed").send("message-" + i);
                }

                assertThat(awaitLatch(allHandled)).isTrue();
                // With a prefetch of one, a second delivery must not begin before the first
                // has been settled. This is the property the whole backpressure story rests on.
                assertThat(highWaterMark).hasValue(1);
            }
        }

        @Test
        @Timeout(10)
        void stops_delivering_once_the_connection_closes() {
            connect("shutdown");

            MessageConsumer consumer = mq.consume("orders.new", String.class, m -> {
            });
            assertThat(consumer.isRunning()).isTrue();

            mq.close();

            assertThat(consumer.isRunning()).isFalse();
            assertThat(mq.isOpen()).isFalse();
        }
    }

    private static boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
