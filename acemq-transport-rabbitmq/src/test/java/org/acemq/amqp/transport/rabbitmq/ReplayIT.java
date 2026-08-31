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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.Replay;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Replaying real dead-lettered messages on a real broker.
 *
 * <p>The in-memory tests seed the dead-letter queue directly, because doing otherwise would make
 * them a test of the retry ladder. This one refuses that shortcut for the main case: a handler
 * actually fails until its attempts run out, RabbitMQ actually expires the message through the
 * ladder, and only then is it replayed. That is the sequence an operator lives through, and it is
 * the only way to prove the two halves agree about which queue a dead message ends up in.
 *
 * <p>It also exercises what the fake cannot: {@code basic.get} and {@code messageCount} against a
 * real channel, including the detail that a pulled message must be acknowledged on the same
 * channel that delivered it.
 */
@Testcontainers
@DisplayName("replay against a real RabbitMQ")
class ReplayIT {

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
        if (mq != null && mq.isOpen()) {
            for (String queue : new String[]{"orders.new", "orders.new.dlq", "orders.new.parked",
                    "orders.new.retry.1s"}) {
                try {
                    mq.deleteQueue(queue);
                } catch (RuntimeException e) {
                    // Best effort: not every test creates every queue.
                }
            }
            mq.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("a message that exhausted its retries is replayed and then succeeds")
    void anExhaustedMessageIsReplayedAndSucceeds() {
        RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofSeconds(1)).withJitter(0);
        AtomicBoolean serviceIsDown = new AtomicBoolean(true);
        List<String> succeeded = new CopyOnWriteArrayList<>();
        AtomicInteger deliveries = new AtomicInteger();

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    deliveries.incrementAndGet();
                    if (serviceIsDown.get()) {
                        throw new IllegalStateException("the payment service is unreachable");
                    }
                    succeeded.add(message.payload());
                })) {

            mq.publisher("orders", "order.placed", String.class).send("order-42");

            // Fails until its attempts run out, and RabbitMQ puts it in the dead-letter queue.
            await().atMost(Duration.ofSeconds(60)).until(() -> consumer.deadLettered() == 1);
            await().atMost(Duration.ofSeconds(30)).until(() -> mq.messageCount("orders.new.dlq") == 1L);

            // The operator fixes the thing that was broken.
            serviceIsDown.set(false);

            Replay replay = mq.replay("orders.new");
            assertThat(replay.pending()).isEqualTo(1L);
            assertThat(replay.replayAll()).isEqualTo(1);

            await().atMost(Duration.ofSeconds(30)).until(() -> !succeeded.isEmpty());
            assertThat(succeeded).containsExactly("order-42");
            assertThat(mq.messageCount("orders.new.dlq"))
                    .as("a replayed message must not be left behind in the dead-letter queue as well")
                    .isZero();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("the replayed message carries its provenance and a reset attempt count")
    void replayedMessagesCarryProvenance() {
        RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofSeconds(1)).withJitter(0);
        AtomicBoolean serviceIsDown = new AtomicBoolean(true);
        List<Envelope> handled = new CopyOnWriteArrayList<>();

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    if (serviceIsDown.get()) {
                        throw new IllegalStateException("the payment service is unreachable");
                    }
                    handled.add(message.envelope());
                })) {

            mq.publisher("orders", "order.placed", String.class).send("order-7");
            await().atMost(Duration.ofSeconds(60)).until(() -> consumer.deadLettered() == 1);
            await().atMost(Duration.ofSeconds(30)).until(() -> mq.messageCount("orders.new.dlq") == 1L);

            serviceIsDown.set(false);
            mq.replay("orders.new").replayAll();

            await().atMost(Duration.ofSeconds(30)).until(() -> !handled.isEmpty());
            Envelope envelope = handled.get(0);
            assertThat(envelope.replayedFrom()).contains("orders.new.dlq");
            assertThat(envelope.replayedAt()).isPresent();
            assertThat(envelope.replayCount()).isEqualTo(1);
            assertThat(envelope.attempt())
                    .as("a replayed message must get the whole ladder again, not arrive exhausted")
                    .isEqualTo(1);
            assertThat(envelope.error())
                    .as("the reason it was dead-lettered is the context that explains this message")
                    .isPresent();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("a bounded replay leaves the rest in the dead-letter queue")
    void boundedReplayLeavesTheRest() {
        RetryPolicy policy = RetryPolicy.fixed(1, Duration.ofSeconds(1)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    throw new IllegalStateException("nothing works today");
                })) {

            for (int i = 0; i < 4; i++) {
                mq.publisher("orders", "order.placed", String.class).send("order-" + i);
            }
            await().atMost(Duration.ofSeconds(90)).until(() -> mq.messageCount("orders.new.dlq") == 4L);
        }

        // Consumer closed first, so replayed messages stay in the queue to be counted rather
        // than being swallowed by the handler that is still failing.
        int moved = mq.replay("orders.new").replay(2);

        assertThat(moved).isEqualTo(2);
        assertThat(mq.messageCount("orders.new.dlq")).isEqualTo(2L);
        await().atMost(Duration.ofSeconds(30)).until(() -> mq.messageCount("orders.new") == 2L);
    }

    @Test
    @Timeout(120)
    @DisplayName("replaying an empty dead-letter queue moves nothing and does not hang")
    void emptyQueueReplaysNothing() {
        // Declared by consuming once, so the queue exists and is genuinely empty.
        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class,
                ConsumerOptions.prefetch(1).withRetry(RetryPolicy.fixed(1, Duration.ofSeconds(1))),
                message -> {
                })) {
            await().atMost(Duration.ofSeconds(30)).until(() -> mq.messageCount("orders.new.dlq") == 0L);
        }

        assertThat(mq.replay("orders.new").replayAll()).isZero();
    }
}
