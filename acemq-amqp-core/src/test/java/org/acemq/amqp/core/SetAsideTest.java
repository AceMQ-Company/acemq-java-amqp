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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * What is reported when a message is set aside, and when setting it aside fails.
 *
 * <p>The failure is the interesting half. A dead-letter queue that was never declared and a
 * dead-letter queue doing its job look identical from every other series in the estate — one
 * queue draining, in both cases — so the only thing that tells them apart is a counter raised on
 * the path where the republish threw. The Go, Python and Ruby libraries raise
 * {@code acemq.messages.set.aside.failed} in the same place, so an alert written once reads the
 * same against all four.
 */
@DisplayName("setting a message aside")
class SetAsideTest {

    private static final String SOURCE = "orders.new";

    private static final RetryPolicy POLICY = RetryPolicy.fixed(1, Duration.ofMillis(1)).withJitter(0);

    @Test
    @DisplayName("an undecodable message is reported as parked, not as dead-lettered")
    void parking_is_its_own_outcome() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryDispatcher dispatcher = new RetryDispatcher(
                new StubConnection(false), RetryTopology.forQueue(SOURCE, POLICY), telemetry);

        dispatcher.park(delivery(), new IllegalArgumentException("not a number"));

        // Parked rather than dead-lettered, because the two mean different things to whoever is
        // on call: dead-lettered is a dependency that will come back, parked is a payload that
        // nothing will ever read.
        assertThat(telemetry.parked).hasSize(1);
        assertThat(telemetry.parked.get(0)).startsWith(SOURCE + " could not be decoded: ");
        assertThat(telemetry.deadLettered).isEmpty();
        assertThat(telemetry.setAsideFailures).isEmpty();
    }

    @Test
    @DisplayName("a parking lot that is not there is counted and then rethrown")
    void a_failed_park_is_counted_against_its_target() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryTopology topology = RetryTopology.forQueue(SOURCE, POLICY);
        RetryDispatcher dispatcher = new RetryDispatcher(new StubConnection(true), topology, telemetry);

        // Rethrown, deliberately: counting is not a reason to change what happens to the
        // message. The caller settles the delivery only once the copy is safely elsewhere, and
        // swallowing this would acknowledge a message that went nowhere at all.
        assertThatThrownBy(() -> dispatcher.park(delivery(), new IllegalArgumentException("nope")))
                .isInstanceOf(IllegalStateException.class);

        assertThat(telemetry.setAsideFailures)
                .containsExactly(SOURCE + " -> " + topology.parkingLotQueue());
        assertThat(telemetry.parked)
                .as("nothing was parked, so nothing may be reported as parked")
                .isEmpty();
    }

    @Test
    @DisplayName("a dead-letter queue that is not there is counted against the dead-letter queue")
    void a_failed_dead_letter_is_counted_against_its_target() {
        RecordingTelemetry telemetry = new RecordingTelemetry();
        RetryTopology topology = RetryTopology.forQueue(SOURCE, POLICY);
        RetryDispatcher dispatcher = new RetryDispatcher(new StubConnection(true), topology, telemetry);

        Envelope envelope = Envelope.of("order.placed").build();

        assertThatThrownBy(() -> dispatcher.onFailure(
                delivery(), envelope, new IllegalStateException("downstream is down"), true))
                .isInstanceOf(IllegalStateException.class);

        assertThat(telemetry.setAsideFailures)
                .containsExactly(SOURCE + " -> " + topology.deadLetterQueue());
        assertThat(telemetry.deadLettered)
                .as("the copy never arrived, so nothing may be reported as dead-lettered")
                .isEmpty();
    }

    private static InboundDelivery delivery() {
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("x-acemq-type", "order.placed");
        return new InboundDelivery(
                SOURCE,
                "orders",
                "order.placed",
                "not a number".getBytes(StandardCharsets.UTF_8),
                headers,
                "message-1",
                "application/json",
                false);
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

    /** A connection that either accepts a publish or refuses every one of them. */
    private static final class StubConnection implements TransportConnection {

        private final boolean refuses;

        StubConnection(boolean refuses) {
            this.refuses = refuses;
        }

        @Override
        public void declareExchange(String name, String type, boolean durable) {
            // nothing is declared in these tests
        }

        @Override
        public void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments) {
            // nothing is declared in these tests
        }

        @Override
        public void bindQueue(String queue, String exchange, String routingKey) {
            // nothing is declared in these tests
        }

        @Override
        public ConfirmResult send(OutboundMessage message) {
            if (refuses) {
                // What a broker does when the queue named by the routing key is not there and
                // the publish is mandatory: nothing takes the message.
                throw new IllegalStateException("no queue named " + message.routingKey());
            }
            return ConfirmResult.confirmed(Duration.ZERO);
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
            return !refuses;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
            // Nothing was opened.
        }
    }
}
