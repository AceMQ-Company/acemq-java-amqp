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
package org.acemq.amqp.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.DeliveryListener;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A retry or dead-letter hop that did not land must not be reported as one that did.
 *
 * <p>The consumer acknowledges the original delivery on the strength of the dispatcher having
 * republished it — "the message has already been republished elsewhere, so the original copy is
 * acknowledged rather than rejected into a requeue loop". That reasoning is right, and it depends
 * entirely on the republish having actually happened.
 *
 * <p>{@link TransportConnection#send} reports two failures as <em>return values</em> rather than
 * exceptions: {@link ConfirmResult#unroutable} when nothing was bound to receive the message, and
 * {@link ConfirmResult#failed} when the broker refused it or never answered. The dispatcher
 * discarded that result, so both were indistinguishable from success — and the original was
 * acknowledged into nothing. A dead-letter queue deleted by hand, or a rung queue that was never
 * declared, silently ate every message routed to it.
 *
 * <p>The existing {@code SetAsideTest} covers the third failure, where the transport throws. It is
 * the two that come back as values that went unnoticed, because no test modelled a broker that
 * answers rather than one that breaks.
 */
@DisplayName("a republished message that did not land")
class RepublishLandsTest {

    private static final String SOURCE = "orders.new";

    private static final RetryPolicy POLICY = RetryPolicy.fixed(3, Duration.ofMillis(1)).withJitter(0);

    @Test
    @DisplayName("is not reported as retried when nothing was bound to receive it")
    void unroutable_retry_is_not_a_retry() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryDispatcher dispatcher = new RetryDispatcher(
                new AnsweringConnection(ConfirmResult.unroutable(Duration.ZERO, "NO_ROUTE")),
                RetryTopology.forQueue(SOURCE, POLICY),
                telemetry);

        RetryDispatcher.Outcome outcome = dispatcher.onFailure(delivery(), envelope(),
                new IllegalStateException("downstream is down"), false);

        assertThat(outcome)
                .as("the rung queue took nothing, so the original must not be acknowledged: "
                        + "reporting RETRIED here loses the message")
                .isEqualTo(RetryDispatcher.Outcome.NOT_REPUBLISHED);
    }

    @Test
    @DisplayName("is not reported as retried when the broker refused it")
    void refused_retry_is_not_a_retry() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryDispatcher dispatcher = new RetryDispatcher(
                new AnsweringConnection(ConfirmResult.failed(Duration.ZERO, "the broker rejected the message")),
                RetryTopology.forQueue(SOURCE, POLICY),
                telemetry);

        RetryDispatcher.Outcome outcome = dispatcher.onFailure(delivery(), envelope(),
                new IllegalStateException("downstream is down"), false);

        assertThat(outcome).isEqualTo(RetryDispatcher.Outcome.NOT_REPUBLISHED);
    }

    @Test
    @DisplayName("is not reported as dead-lettered when the dead-letter queue is not there")
    void unroutable_dead_letter_is_not_a_dead_letter() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryDispatcher dispatcher = new RetryDispatcher(
                new AnsweringConnection(ConfirmResult.unroutable(Duration.ZERO, "NO_ROUTE")),
                RetryTopology.forQueue(SOURCE, POLICY),
                telemetry);

        // Attempt 3 of a 3-attempt policy: the next stop is the dead-letter queue.
        Envelope exhausted = envelope().toBuilder().attempt(3).build();

        RetryDispatcher.Outcome outcome = dispatcher.onFailure(delivery(), exhausted,
                new IllegalStateException("downstream is down"), false);

        assertThat(outcome)
                .as("a dead letter that reached no queue is not a dead letter, and the counter "
                        + "that says otherwise is the one an operator trusts")
                .isEqualTo(RetryDispatcher.Outcome.NOT_REPUBLISHED);
        assertThat(telemetry.setAsideFailures)
                .as("the failure belongs on the series that exists to tell a working dead-letter "
                        + "queue from one that was never declared")
                .isNotEmpty();
    }

    @Test
    @DisplayName("is reported as retried when it did land")
    void a_landed_retry_is_still_a_retry() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryDispatcher dispatcher = new RetryDispatcher(
                new AnsweringConnection(ConfirmResult.confirmed(Duration.ZERO)),
                RetryTopology.forQueue(SOURCE, POLICY),
                telemetry);

        RetryDispatcher.Outcome outcome = dispatcher.onFailure(delivery(), envelope(),
                new IllegalStateException("downstream is down"), false);

        assertThat(outcome).isEqualTo(RetryDispatcher.Outcome.RETRIED);
        assertThat(telemetry.setAsideFailures).isEmpty();
    }

    private static InboundDelivery delivery() {
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("x-acemq-type", "OrderPlaced");
        return new InboundDelivery(
                SOURCE,
                "orders",
                "order.placed",
                "{}".getBytes(StandardCharsets.UTF_8),
                headers,
                "m-1",
                "application/json",
                false);
    }

    private static Envelope envelope() {
        return Envelope.of("OrderPlaced").id("m-1").attempt(1).build();
    }

    /** A broker that answers rather than one that breaks. */
    private static final class AnsweringConnection implements TransportConnection {

        private final ConfirmResult answer;

        AnsweringConnection(ConfirmResult answer) {
            this.answer = answer;
        }

        @Override
        public ConfirmResult send(OutboundMessage message) {
            return answer;
        }

        @Override
        public void declareExchange(String name, String type, boolean durable) {
        }

        @Override
        public void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments) {
        }

        @Override
        public void bindQueue(String queue, String exchange, String routingKey) {
        }

        @Override
        public Subscription subscribe(String queue, int prefetch, DeliveryListener listener) {
            throw new UnsupportedOperationException("nothing subscribes in these tests");
        }

        @Override
        public void deleteQueue(String name) {
            throw new UnsupportedOperationException("nothing is deleted in these tests");
        }

        @Override
        public boolean queueExists(String name) {
            return true;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    /** Records what it was told rather than measuring it. */
    private static final class RecordingTelemetry implements Telemetry {

        private final List<String> parked = new ArrayList<>();
        private final List<String> deadLettered = new ArrayList<>();
        private final List<String> setAsideFailures = new ArrayList<>();

        @Override
        public Scope publishStarted(String exchange, String routingKey, Envelope envelope) {
            return Scope.NONE;
        }

        @Override
        public Scope consumeStarted(String queue, Envelope envelope) {
            return Scope.NONE;
        }

        @Override
        public void messageRetried(String queue, Envelope envelope, Duration delay) {
            // not under test
        }

        @Override
        public void messageDeadLettered(String queue, Envelope envelope, String reason) {
            deadLettered.add(queue + " " + reason);
        }

        @Override
        public void messageParked(String queue, Envelope envelope, String reason) {
            parked.add(queue + " " + reason);
        }

        @Override
        public void setAsideFailed(String queue, String target, String reason) {
            setAsideFailures.add(queue + " -> " + target);
        }

        @Override
        public Map<String, String> propagationHeaders() {
            return Collections.emptyMap();
        }
    }
}
