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
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.RetryPolicy;
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
 * The retry ladder against a real broker.
 *
 * <p>The in-memory suite covers the same behaviour far more quickly, so this exists to prove
 * one specific thing the fake cannot: that a real RabbitMQ expires a message out of a
 * time-to-live queue and dead-letters it back where AceMQ expects. If the two ever disagree,
 * the fake is wrong.
 */
@Testcontainers
@DisplayName("the retry ladder on RabbitMQ")
class RetryLadderIT {

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

    /**
     * Every queue any test here can create, rungs first.
     *
     * <p>The rungs matter more than the rest and are the reason this list is spelled out. A rung
     * left behind still holds a message with a live time-to-live, and when it expires the broker
     * routes it back to {@code orders.new} — which by then belongs to the next test. That
     * arrives as one extra delivery in a test that counted deliveries, minutes after the test
     * that produced it passed.
     *
     * <p>Rungs before the source queue, so nothing can expire into a queue that is about to be
     * left in place.
     */
    private static final String[] QUEUES = {
            "orders.new.retry.1s", "orders.new.retry.5s", "orders.new.retry.25s",
            "orders.new.dlq", "orders.new.parked", "orders.new",
    };

    @AfterEach
    void disconnect() {
        if (mq != null && mq.isOpen()) {
            for (String queue : QUEUES) {
                try {
                    mq.deleteQueue(queue);
                } catch (RuntimeException e) {
                    // The queue may not exist for a given test; tearing down is best effort.
                }
            }
            mq.close();
        }
    }

    @Test
    @Timeout(120)
    void the_broker_returns_an_expired_message_to_its_source_queue() {
        List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(1)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    attemptNumbers.add(message.attempt());
                    throw new IllegalStateException("inventory service is unreachable");
                })) {

            mq.publisher("orders", "order.placed").send("{\"id\":\"o-1\"}");

            // Waiting on the list itself, not on a counter beside it. A separate counter is
            // incremented either before or after the list is written, and whichever order it is
            // gives a window where the wait is satisfied and the list is one short.
            //
            // At least three rather than exactly three: an await that overshoots between polls
            // never sees an equality hold, and turns a wrong answer into a timeout that says
            // nothing about what went wrong.
            await().atMost(Duration.ofSeconds(60)).until(() -> attemptNumbers.size() >= 3);
            await().atMost(Duration.ofSeconds(30)).until(() -> consumer.deadLettered() >= 1);

            assertThat(attemptNumbers).containsExactly(1, 2, 3);
            // Also awaited: the counter is updated around the dead-lettering rather than with
            // it, so reading it the instant the message lands is a race the test loses rarely
            // enough to be blamed on the broker.
            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.retried() == 2);
            assertThat(consumer.deadLettered()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(120)
    void an_exhausted_message_is_readable_from_the_dead_letter_queue() {
        RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofSeconds(1)).withJitter(0);

        try (MessageConsumer failing = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    throw new IllegalStateException("payment gateway timed out");
                })) {
            mq.publisher("orders", "order.placed").send("{\"id\":\"o-2\"}");
            await().atMost(Duration.ofSeconds(60)).until(() -> failing.deadLettered() >= 1);
        }

        List<Message<String>> dead = new CopyOnWriteArrayList<>();
        try (MessageConsumer reader = mq.consume("orders.new.dlq", String.class, dead::add)) {
            await().atMost(Duration.ofSeconds(30)).until(() -> !dead.isEmpty());

            Message<String> message = dead.get(0);
            assertThat(message.payload()).isEqualTo("{\"id\":\"o-2\"}");
            assertThat(message.attempt()).isEqualTo(2);
            assertThat(message.envelope().error())
                    .hasValueSatisfying(reason -> assertThat(reason)
                            .contains("exhausted 2 attempts")
                            .contains("payment gateway timed out"));
        }
    }

    @Test
    @Timeout(120)
    void a_fatal_failure_skips_the_ladder_entirely() throws InterruptedException {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofSeconds(1)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    attempts.incrementAndGet();
                    throw new AceFatalException("this order can never be processed");
                })) {

            mq.publisher("orders", "order.placed").send("{\"id\":\"o-3\"}");

            await().atMost(Duration.ofSeconds(30)).until(() -> consumer.deadLettered() >= 1);
            assertThat(attempts).hasValue(1);
            assertThat(consumer.retried()).isZero();

            // A fatal failure skips the ladder, so nothing should arrive later. Waiting proves
            // that rather than assuming it: the assertion above holds a moment after the
            // dead-letter whether or not a rung is quietly holding a retry.
            Thread.sleep(2_000);
            assertThat(attempts).hasValue(1);
            assertThat(consumer.deadLettered()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(120)
    void a_waiting_message_does_not_hold_up_the_queue_behind_it() {
        AtomicInteger successes = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(5)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    if (message.payload().startsWith("poison")) {
                        throw new IllegalStateException("always fails");
                    }
                    successes.incrementAndGet();
                })) {

            mq.publisher("orders", "order.placed").send("poison");
            for (int i = 0; i < 10; i++) {
                mq.publisher("orders", "order.placed").send("good-" + i);
            }

            // With a five-second backoff and a prefetch of one, a sleeping handler would take
            // at least ten seconds to clear these. Parking the failure in the broker means the
            // rest are handled immediately.
            await().atMost(Duration.ofSeconds(20)).until(() -> successes.get() >= 10);
            assertThat(successes).hasValue(10);

            // The poison message is still going round the ladder, and its rung outlives this
            // test unless somebody deletes it. That is what the teardown list is for; without
            // the five-second rung in it, this message expires into the next test's queue.
        }
    }
}
