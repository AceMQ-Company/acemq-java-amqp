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

import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MicrometerSupport;
import org.acemq.amqp.core.Pipeline;
import org.acemq.amqp.core.RequestTimedOutException;
import org.acemq.amqp.core.Requester;
import org.acemq.amqp.core.Responder;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The three things that counted their own work and never reported it.
 *
 * <p>Request/reply, the outbox relay and pipelines each kept counters readable only by calling
 * a getter on the object, which no dashboard can do. A number that exists and cannot be scraped
 * is a number nobody has during an incident, so each one is asserted here the way the older
 * metrics are.
 */
@DisplayName("telemetry for request/reply, the outbox and pipelines")
class TelemetryGapsTest {

    /** The relay publishes stored bytes unchanged, so the body is read as text, not parsed. */
    private static final org.acemq.amqp.core.ConsumerOptions RAW = org.acemq.amqp.core.ConsumerOptions.defaults()
            .as(org.acemq.amqp.core.Codecs.byName("text"));

    private SimpleMeterRegistry registry;
    private AceMq mq;

    @BeforeEach
    void setUp() {
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
        return mq;
    }

    @Nested
    @DisplayName("request and reply")
    class RequestReply {

        @Test
        @Timeout(30)
        @DisplayName("times the round trip the caller actually experienced")
        void timesTheRoundTrip() {
            connect("telemetry-request");
            mq.declareQueue("pricing", QueueType.CLASSIC, Collections.emptyMap());

            try (Responder responder = mq.respond("pricing", String.class, quote -> "42.00");
                    Requester requester = mq.requester()) {

                String answer = requester.request("", "pricing", "widget", String.class, Duration.ofSeconds(10));
                assertThat(answer).isEqualTo("42.00");

                // The publish and the reply's delivery were each already timed. Neither of them
                // is what the caller waited for, which is the number this adds.
                assertThat(registry.find(MetricNames.REQUEST_DURATION)
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_ANSWERED)
                        .timer())
                        .isNotNull()
                        .satisfies(timer -> assertThat(timer.count()).isEqualTo(1));
            }
        }

        @Test
        @Timeout(30)
        @DisplayName("a timeout is an outcome, not a missing measurement")
        void recordsTimeouts() {
            connect("telemetry-request-timeout");
            mq.declareQueue("pricing", QueueType.CLASSIC, Collections.emptyMap());

            try (Requester requester = mq.requester()) {
                // Nobody is answering. A round trip that never completes is exactly the case
                // worth graphing, so it must not simply be absent from the metric.
                assertThatThrownBy(() -> requester.request(
                        "", "pricing", "widget", String.class, Duration.ofMillis(300)))
                        .isInstanceOf(RequestTimedOutException.class);

                assertThat(registry.find(MetricNames.REQUEST_TOTAL)
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_TIMED_OUT)
                        .counter())
                        .isNotNull()
                        .satisfies(counter -> assertThat(counter.count()).isEqualTo(1.0));
            }
        }
    }

    @Nested
    @DisplayName("pipelines")
    class Pipelines {

        @Test
        @Timeout(30)
        @DisplayName("reports a completed run, and which step it finished at")
        void reportsCompletion() {
            connect("telemetry-pipeline");

            try (Pipeline<String> fulfilment = mq.pipeline("fulfilment", String.class)
                    .step("validate", String.class, m -> m.payload() + "|ok")
                    .step("dispatch", String.class, m -> null)
                    .build()) {

                fulfilment.send("o-1");

                await().atMost(Duration.ofSeconds(20)).until(() -> registry.find(MetricNames.PIPELINE_RUN_TOTAL)
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_COMPLETED)
                        .counter() != null);

                assertThat(registry.find(MetricNames.PIPELINE_RUN_TOTAL)
                        .tag(MetricNames.TAG_PIPELINE, "fulfilment")
                        .tag(MetricNames.TAG_STEP, "dispatch")
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_COMPLETED)
                        .counter())
                        .isNotNull();
            }
        }

        @Test
        @Timeout(30)
        @DisplayName("a run that stops early is a different outcome, at the step that stopped it")
        void reportsEndedEarly() {
            connect("telemetry-pipeline-early");

            try (Pipeline<String> fulfilment = mq.pipeline("fulfilment", String.class)
                    // Returning null before the last step is a decision, not a failure, and the
                    // step that made it is the thing worth knowing.
                    .step("validate", String.class, m -> null)
                    .step("dispatch", String.class, m -> null)
                    .build()) {

                fulfilment.send("o-1");

                await().atMost(Duration.ofSeconds(20)).until(() -> registry.find(MetricNames.PIPELINE_RUN_TOTAL)
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_ENDED_EARLY)
                        .counter() != null);

                assertThat(registry.find(MetricNames.PIPELINE_RUN_TOTAL)
                        .tag(MetricNames.TAG_STEP, "validate")
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_ENDED_EARLY)
                        .counter())
                        .isNotNull();
            }
        }
    }

    @Nested
    @DisplayName("the outbox relay")
    class Outbox {

        @Test
        @Timeout(30)
        @DisplayName("reports how long a record waited between being committed and published")
        void reportsLag() {
            connect("telemetry-outbox");
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            RecordingOutboxStore store = new RecordingOutboxStore();
            try (org.acemq.amqp.core.MessageConsumer consumer = mq.consume("orders.new", String.class, RAW, message -> {
            });
                    org.acemq.amqp.patterns.OutboxRelay relay = new org.acemq.amqp.patterns.OutboxRelay(mq, store)) {

                store.add(org.acemq.amqp.api.OutboxRecord.of(
                        "orders",
                        "order.placed",
                        org.acemq.amqp.api.Envelope.of("order.placed").id("o-1").build(),
                        "{\"order\":\"o-1\"}"));

                assertThat(relay.drainOnce()).isEqualTo(1);

                // A committed, unpublished row is a message that exists, is owed to somebody,
                // and shows up in no queue depth anywhere. This is the only number that sees it.
                assertThat(registry.find(MetricNames.OUTBOX_LAG).timer())
                        .isNotNull()
                        .satisfies(timer -> assertThat(timer.count()).isEqualTo(1));
                assertThat(registry.find(MetricNames.OUTBOX_TOTAL)
                        .tag(MetricNames.TAG_OUTCOME, MetricNames.OUTCOME_PUBLISHED)
                        .counter())
                        .isNotNull();
            }
        }
    }

    @Nested
    @DisplayName("the default methods")
    class Defaults {

        @Test
        @DisplayName("a sink written before these existed still compiles and still works")
        void olderSinksKeepWorking() {
            // The point of the defaults, asserted rather than assumed. This implements only the
            // four methods that existed before, exactly as an application's own sink would, and
            // it has to remain valid: adding an abstract method here would break every one of
            // them at compile time for a signal they never asked for.
            Telemetry older = new Telemetry() {

                @Override
                public Scope publishStarted(String exchange, String routingKey, org.acemq.amqp.api.Envelope e) {
                    return Scope.NONE;
                }

                @Override
                public Scope consumeStarted(String queue, org.acemq.amqp.api.Envelope e) {
                    return Scope.NONE;
                }

                @Override
                public void messageRetried(String queue, org.acemq.amqp.api.Envelope e, Duration delay) {
                    // no-op
                }

                @Override
                public void messageDeadLettered(String queue, org.acemq.amqp.api.Envelope e, String reason) {
                    // no-op
                }

                @Override
                public java.util.Map<String, String> propagationHeaders() {
                    return Collections.emptyMap();
                }
            };

            assertThat(older.requestStarted("pricing", org.acemq.amqp.api.Envelope.of("Q").build()))
                    .isSameAs(Telemetry.Scope.NONE);

            // And the void ones do nothing rather than throwing.
            older.outboxPublished("orders", "order.placed", Duration.ofSeconds(1));
            older.outboxFailed("orders", "order.placed", "broker refused it");
            older.pipelineRunFinished("fulfilment", "dispatch", MetricNames.OUTCOME_COMPLETED, Duration.ZERO);
        }
    }
}
