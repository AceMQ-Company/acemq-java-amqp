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
package org.acemq.amqp.transport.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The walking skeleton, proven against a real broker on a single node.
 *
 * <p>This is the test that says the layering works: the core engine reaches a real RabbitMQ
 * through the transport SPI, an envelope survives the round trip, publisher confirms are
 * awaited, and every delivery is settled. Cluster topologies of three, five and nine nodes
 * come later and reuse the same assertions.
 */
@Testcontainers
@DisplayName("publish and consume against a single-node RabbitMQ")
class RoundTripIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            BrokerImage.current());

    private AceMq mq;

    @BeforeEach
    void connect() {
        mq = AceMq.connect(BROKER.getAmqpUrl());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new");
        mq.bind("orders.new", "orders", "order.*");
    }

    @AfterEach
    void disconnect() {
        if (mq != null) {
            mq.deleteQueue("orders.new");
            mq.close();
        }
    }

    @Test
    @Timeout(60)
    void reports_what_the_broker_can_do() {
        assertThat(mq.transportName()).isEqualTo("rabbitmq");
        assertThat(mq.capabilities())
                .contains(Capability.EXCHANGE_ROUTING, Capability.PUBLISHER_CONFIRMS, Capability.QUORUM_QUEUES);
        // Not claimed, because it needs a plugin that may not be installed. The engine
        // generates a retry ladder instead of assuming this is available.
        assertThat(mq.capabilities()).doesNotContain(Capability.DELAYED_DELIVERY);
    }

    @Test
    @Timeout(60)
    void a_published_message_arrives_with_its_envelope_intact() {
        List<Message<String>> received = new CopyOnWriteArrayList<>();
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, received::add)) {

            Publisher<String> publisher = mq.publisher("orders", "order.placed");
            Envelope sent = Envelope.of("order.placed")
                    .correlationId("flow-1")
                    .header("tenant", "acme")
                    .build();

            PublishResult result = publisher.send("{\"id\":\"o-1\"}", sent);

            assertThat(result.routed()).isTrue();
            assertThat(result.latency()).isPositive();

            await().atMost(Duration.ofSeconds(20)).until(() -> !received.isEmpty());

            Message<String> message = received.get(0);
            assertThat(message.payload()).isEqualTo("{\"id\":\"o-1\"}");
            assertThat(message.queue()).isEqualTo("orders.new");
            assertThat(message.routingKey()).contains("order.placed");
            assertThat(message.attempt()).isEqualTo(1);
            assertThat(message.isFirstAttempt()).isTrue();

            Envelope envelope = message.envelope();
            assertThat(envelope.id()).isEqualTo(sent.id());
            assertThat(envelope.type()).isEqualTo("order.placed");
            assertThat(envelope.correlationId()).isEqualTo("flow-1");
            assertThat(envelope.headers()).containsEntry("tenant", "acme");
            assertThat(envelope.origin()).isPresent();

            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.acknowledged() == 1);
            assertThat(consumer.rejected()).isZero();
        }
    }

    @Test
    @Timeout(60)
    void an_unroutable_publish_is_an_error_rather_than_a_silent_drop() {
        Publisher<String> publisher = mq.publisher("orders", "nothing.is.bound.to.this");

        assertThatThrownBy(() -> publisher.send("lost?"))
                .isInstanceOf(PublishFailedException.class)
                .hasMessageContaining("could not")
                .hasMessageContaining("routed");
    }

    @Test
    @Timeout(60)
    void a_failing_handler_rejects_its_message_rather_than_leaving_it_unsettled() {
        AtomicInteger attempts = new AtomicInteger();
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, ConsumerOptions.prefetch(1), message -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("the inventory service is unreachable");
        })) {

            mq.publisher("orders", "order.placed").send("{\"id\":\"o-2\"}");

            await().atMost(Duration.ofSeconds(20)).until(() -> consumer.rejected() == 1);
            assertThat(consumer.acknowledged()).isZero();

            // Not requeued by default: a message that just failed would come straight back to
            // the same consumer and spin. It stays rejected until the retry ladder exists.
            assertThat(attempts).hasValue(1);
        }
    }

    @Test
    @Timeout(60)
    void an_undecodable_payload_is_rejected_without_retrying() {
        try (MessageConsumer consumer = mq.consume("orders.new", Integer.class, message -> {
            throw new AssertionError("the handler must not run when decoding fails");
        })) {

            mq.publisher("orders", "order.placed").send("not an integer");

            await().atMost(Duration.ofSeconds(20)).until(() -> consumer.rejected() == 1);
            assertThat(consumer.acknowledged()).isZero();
        }
    }

    @Test
    @Timeout(60)
    void a_fatal_handler_failure_is_rejected_without_requeue() {
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
            throw new AceFatalException("this payload will never be valid for this handler");
        })) {

            mq.publisher("orders", "order.placed").send("{\"id\":\"o-3\"}");

            await().atMost(Duration.ofSeconds(20)).until(() -> consumer.rejected() == 1);
        }
    }

    @Test
    @Timeout(60)
    void closing_the_connection_stops_the_consumer() {
        MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
        });
        assertThat(consumer.isRunning()).isTrue();

        mq.close();

        assertThat(consumer.isRunning()).isFalse();
        assertThat(mq.isOpen()).isFalse();
        mq = null; // already closed; stop the teardown from touching it
    }
}
