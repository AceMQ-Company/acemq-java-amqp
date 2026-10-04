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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishingPausedException;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.ConnectionBlockedException;
import org.acemq.amqp.transport.DeliveryListener;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * A publish the library declined to send is {@code refused}, not {@code failed}.
 *
 * <p>{@code failed} means a message that may have been lost: the broker nacked it, never
 * confirmed it, or the write broke. A publish refused before a byte was written cannot have been
 * lost — the caller still holds it and nothing reached the broker. Counting both as
 * {@code failed} put deliberate back pressure on the same graph as data loss, so a drill (or an
 * alert) could not tell a cutover pause from a broker eating messages.
 */
@DisplayName("a refused publish")
class RefusedPublishTelemetryTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @AfterEach
    void tearDown() {
        registry.close();
    }

    private DefaultPublisher<String> publisher(TransportConnection connection, AtomicBoolean paused) {
        return new DefaultPublisher<>(connection, Codecs.byName("text"), "orders", "order.placed", "test",
                MicrometerSupport.telemetry(registry, "test"), closeable -> {
                }, paused::get);
    }

    private double published(String outcome) {
        Counter counter = registry.find(MetricNames.PUBLISH_TOTAL)
                .tag(MetricNames.TAG_OUTCOME, outcome)
                .counter();
        return counter == null ? 0 : counter.count();
    }

    @Test
    @DisplayName("while publishing is paused counts as refused, not failed")
    void paused_is_refused() {
        DefaultPublisher<String> publisher = publisher(
                new FakeConnection(m -> ConfirmResult.confirmed(Duration.ZERO)), new AtomicBoolean(true));

        assertThatThrownBy(() -> publisher.send("x")).isInstanceOf(PublishingPausedException.class);

        assertThat(published(MetricNames.OUTCOME_REFUSED)).isEqualTo(1.0);
        assertThat(published(MetricNames.OUTCOME_FAILED)).isZero();
    }

    @Test
    @DisplayName("while publishing is paused counts as refused on the asynchronous path too")
    void paused_async_is_refused() {
        DefaultPublisher<String> publisher = publisher(
                new FakeConnection(m -> ConfirmResult.confirmed(Duration.ZERO)), new AtomicBoolean(true));

        assertThatThrownBy(() -> publisher.sendAsync("x")).isInstanceOf(PublishingPausedException.class);

        assertThat(published(MetricNames.OUTCOME_REFUSED)).isEqualTo(1.0);
        assertThat(published(MetricNames.OUTCOME_FAILED)).isZero();
    }

    @Test
    @DisplayName("on a connection already known to be blocked counts as refused")
    void blocked_before_writing_is_refused() {
        DefaultPublisher<String> publisher = publisher(new FakeConnection(m -> {
            throw new ConnectionBlockedException("blocked, nothing sent", "low on memory");
        }), new AtomicBoolean(false));

        assertThatThrownBy(() -> publisher.send("x")).isInstanceOf(ConnectionBlockedException.class);
        assertThatThrownBy(() -> publisher.sendAsync("x")).isInstanceOf(ConnectionBlockedException.class);

        assertThat(published(MetricNames.OUTCOME_REFUSED)).isEqualTo(2.0);
        assertThat(published(MetricNames.OUTCOME_FAILED)).isZero();
    }

    @Test
    @DisplayName("blocked after the message was written stays failed: it may have been lost")
    void blocked_after_writing_is_failed() {
        DefaultPublisher<String> publisher = publisher(new FakeConnection(m -> {
            throw new ConnectionBlockedException("blocked, never confirmed", "low on memory", true);
        }), new AtomicBoolean(false));

        assertThatThrownBy(() -> publisher.send("x")).isInstanceOf(ConnectionBlockedException.class);

        assertThat(published(MetricNames.OUTCOME_FAILED)).isEqualTo(1.0);
        assertThat(published(MetricNames.OUTCOME_REFUSED)).isZero();
    }

    @Test
    @DisplayName("a nack is still failed")
    void nack_is_failed() {
        DefaultPublisher<String> publisher = publisher(
                new FakeConnection(m -> ConfirmResult.failed(Duration.ZERO, "nack")), new AtomicBoolean(false));

        assertThatThrownBy(() -> publisher.send("x")).isInstanceOf(PublishFailedException.class);

        assertThat(published(MetricNames.OUTCOME_FAILED)).isEqualTo(1.0);
        assertThat(published(MetricNames.OUTCOME_REFUSED)).isZero();
    }

    /** Answers every publish the way it was told to. */
    private static final class FakeConnection implements TransportConnection {

        private final Function<OutboundMessage, ConfirmResult> answer;

        FakeConnection(Function<OutboundMessage, ConfirmResult> answer) {
            this.answer = answer;
        }

        @Override
        public ConfirmResult send(OutboundMessage message) {
            return answer.apply(message);
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
}
