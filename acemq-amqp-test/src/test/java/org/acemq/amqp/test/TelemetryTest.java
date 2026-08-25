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
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.MicrometerSupport;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * What the engine reports while it works.
 *
 * <p>Telemetry is treated as behaviour rather than decoration, so it is asserted the same way
 * everything else is. A metric nobody checks is a metric that quietly stops being emitted, and
 * the first time anyone notices is during an incident.
 */
@DisplayName("telemetry")
class TelemetryTest {

    private SimpleMeterRegistry registry;
    private AceMq mq;

    @BeforeEach
    void setUp() {
        // A registry of this test's own, passed in explicitly. Micrometer's global registry
        // back-fills its meters into every registry added to it, so tests that share it see
        // each other's measurements and read counts that were never theirs.
        registry = new SimpleMeterRegistry();
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        registry.close();
        InMemoryTransport.reset();
    }

    private AceMq connect(String brokerName) {
        mq = AceMq.connect("memory://" + brokerName, MicrometerSupport.telemetry(registry, "in-memory"));
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @Nested
    @DisplayName("publishing")
    class Publishing {

        @Test
        @Timeout(20)
        void records_a_timed_success() {
            connect("telemetry-publish");

            mq.publisher("orders", "order.placed").send("payload");

            Timer timer = registry.find(MetricNames.PUBLISH_DURATION)
                    .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_CONFIRMED)
                    .tag(MetricNames.TAG_EXCHANGE, "orders")
                    .tag(MetricNames.TAG_ROUTING_KEY, "order.placed")
                    .timer();

            assertThat(timer).isNotNull();
            assertThat(timer.count()).isEqualTo(1);
            assertThat(registry.find(MetricNames.PUBLISH_TOTAL)
                    .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_CONFIRMED)
                    .counter()
                    .count())
                    .isEqualTo(1.0);
        }

        @Test
        @Timeout(20)
        void distinguishes_an_unroutable_publish_from_a_failure() {
            connect("telemetry-unroutable");

            assertThatThrownBy(() -> mq.publisher("orders", "nothing.bound").send("lost?"))
                    .isInstanceOf(PublishFailedException.class);

            // Unroutable is its own outcome. Counting it as a generic failure would hide the
            // one problem an operator can actually fix, which is a missing binding.
            assertThat(registry.find(MetricNames.PUBLISH_TOTAL)
                    .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_UNROUTABLE)
                    .counter()
                    .count())
                    .isEqualTo(1.0);
            assertThat(registry.find(MetricNames.PUBLISH_TOTAL)
                    .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_FAILED)
                    .counter())
                    .isNull();
        }

        @Test
        @Timeout(20)
        void keeps_the_message_id_off_metrics() {
            connect("telemetry-cardinality");

            for (int i = 0; i < 25; i++) {
                mq.publisher("orders", "order.placed").send("payload-" + i);
            }

            // Twenty-five messages, each with a unique identifier, must still produce one time
            // series. A message id on a metric tag is how a metrics backend gets taken down.
            List<io.micrometer.core.instrument.Meter> published = registry.getMeters().stream()
                    .filter(meter -> meter.getId().getName().equals(MetricNames.PUBLISH_DURATION))
                    .collect(java.util.stream.Collectors.toList());

            assertThat(published).hasSize(1);
            assertThat(published.get(0).getId().getTags())
                    .extracting(io.micrometer.core.instrument.Tag::getKey)
                    .doesNotContain("message.id", "id");
        }
    }

    @Nested
    @DisplayName("when nobody is watching")
    class Disabled {

        @Test
        @Timeout(20)
        void works_normally_with_telemetry_switched_off() {
            // An application that wants no instrumentation must still work, and must pay
            // nothing for the instrumentation it declined.
            mq = AceMq.connect("memory://telemetry-off", org.acemq.amqp.api.Telemetry.NONE);
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            AtomicInteger handled = new AtomicInteger();
            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> handled.incrementAndGet())) {
                mq.publisher("orders", "order.placed").send("payload");
                await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 1);
            }

            assertThat(registry.getMeters()).isEmpty();
        }

        @Test
        @Timeout(20)
        void adds_no_propagation_headers_when_tracing_is_off() {
            mq = AceMq.connect("memory://telemetry-off-headers", org.acemq.amqp.api.Telemetry.NONE);
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            AtomicInteger checked = new AtomicInteger();
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
                assertThat(message.headers()).doesNotContainKey(org.acemq.amqp.api.AceHeaders.TRACEPARENT);
                checked.incrementAndGet();
            })) {
                mq.publisher("orders", "order.placed").send("payload");
                await().atMost(Duration.ofSeconds(10)).until(() -> checked.get() == 1);
            }
        }
    }

    @Nested
    @DisplayName("consuming")
    class Consuming {

        @Test
        @Timeout(20)
        void records_a_handled_delivery_and_which_attempt_it_was() {
            connect("telemetry-consume");
            AtomicInteger handled = new AtomicInteger();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> handled.incrementAndGet())) {
                mq.publisher("orders", "order.placed").send("payload");
                await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 1);

                await().atMost(Duration.ofSeconds(5)).until(() -> registry.find(MetricNames.CONSUME_TOTAL)
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_ACKED)
                        .counter() != null);

                assertThat(registry.find(MetricNames.CONSUME_TOTAL)
                        .tag(MetricNames.TAG_QUEUE, "orders.new")
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_ACKED)
                        .counter()
                        .count())
                        .isEqualTo(1.0);

                // The attempt distribution is what shows a dependency degrading: the mean
                // creeping above one long before anything reaches a dead-letter queue.
                assertThat(registry.find(MetricNames.CONSUME_ATTEMPTS)
                        .summary()
                        .max())
                        .isEqualTo(1.0);
            }
        }

        @Test
        @Timeout(30)
        void separates_retried_from_dead_lettered_deliveries() {
            connect("telemetry-retry");
            RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofMillis(50)).withJitter(0);

            try (MessageConsumer consumer = mq.consume(
                    "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                        throw new IllegalStateException("still failing");
                    })) {

                mq.publisher("orders", "order.placed").send("payload");
                await().atMost(Duration.ofSeconds(15)).until(() -> consumer.deadLettered() == 1);

                assertThat(registry.find(MetricNames.RETRIED_TOTAL)
                        .tag(MetricNames.TAG_QUEUE, "orders.new")
                        .counter()
                        .count())
                        .isEqualTo(2.0);
                assertThat(registry.find(MetricNames.DEAD_LETTERED_TOTAL)
                        .tag(MetricNames.TAG_QUEUE, "orders.new")
                        .counter()
                        .count())
                        .isEqualTo(1.0);

                // The attempt summary should have seen all three deliveries, peaking at three.
                assertThat(registry.find(MetricNames.CONSUME_ATTEMPTS).summary().max())
                        .isEqualTo(3.0);
            }
        }

        @Test
        @Timeout(20)
        void records_a_fatal_failure_as_dead_lettered_rather_than_retried() {
            connect("telemetry-fatal");
            RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(50)).withJitter(0);

            try (MessageConsumer consumer = mq.consume(
                    "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                        throw new AceFatalException("never valid");
                    })) {

                mq.publisher("orders", "order.placed").send("payload");
                await().atMost(Duration.ofSeconds(15)).until(() -> consumer.deadLettered() == 1);

                assertThat(registry.find(MetricNames.DEAD_LETTERED_TOTAL).counter().count())
                        .isEqualTo(1.0);
                assertThat(registry.find(MetricNames.RETRIED_TOTAL).counter()).isNull();
            }
        }

        @Test
        @Timeout(20)
        void returns_the_in_flight_gauge_to_zero_once_handlers_finish() {
            connect("telemetry-inflight");
            AtomicInteger handled = new AtomicInteger();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> handled.incrementAndGet())) {
                for (int i = 0; i < 10; i++) {
                    mq.publisher("orders", "order.placed").send("payload-" + i);
                }
                await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 10);

                // A gauge that only ever goes up is the most common instrumentation bug there
                // is, and it makes an alert on in-flight work useless.
                await().atMost(Duration.ofSeconds(5)).until(() -> registry.find(MetricNames.CONSUME_IN_FLIGHT)
                        .gauge()
                        .value() == 0.0);
            }
        }
    }
}
