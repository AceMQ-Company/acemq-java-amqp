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
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.TransportConnection;
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
 * two specific things the fake cannot: that a real RabbitMQ expires a message out of a
 * time-to-live queue and dead-letters it back where AceMQ expects, and that a wait below the
 * threshold leaves no queue on a real broker to find. If the two ever disagree, the fake is
 * wrong.
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
     * A threshold low enough that a second-scale ladder reaches the broker.
     *
     * <p>The default is thirty seconds, and a test that waited thirty seconds three times over
     * would not be run. Lowering it is the only thing these tests change: everything below the
     * line is the same mechanism a real policy uses above it.
     */
    private static final Duration IN_BROKER_FROM = Duration.ofMillis(500);

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
            "orders.new.retry.1s", "orders.new.retry.2s", "orders.new.retry.5s", "orders.new.retry.25s",
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
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(1))
                .withJitter(0)
                .waitInBrokerFrom(IN_BROKER_FROM);

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
        RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofSeconds(1))
                .withJitter(0)
                .waitInBrokerFrom(IN_BROKER_FROM);

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
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofSeconds(1))
                .withJitter(0)
                .waitInBrokerFrom(IN_BROKER_FROM);

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
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(5))
                .withJitter(0)
                .waitInBrokerFrom(IN_BROKER_FROM);

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

    @Test
    @Timeout(120)
    @DisplayName("a short wait is held in the consumer and leaves no queue behind")
    void a_short_wait_creates_no_rung() {
        List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();
        // One second, against the default thirty-second threshold. The consumer holds this
        // itself, and the whole point of the threshold is that the broker is never told about
        // it: no queue is declared, so there is nothing to clean up and nothing for a second
        // service to disagree with.
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(1)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    attemptNumbers.add(message.attempt());
                    throw new IllegalStateException("inventory service is unreachable");
                })) {

            assertThat(policy.brokerRungs()).isEmpty();
            assertThat(exists("orders.new.retry.1s")).isFalse();

            mq.publisher("orders", "order.placed").send("{\"id\":\"o-4\"}");

            await().atMost(Duration.ofSeconds(60)).until(() -> attemptNumbers.size() >= 3);
            await().atMost(Duration.ofSeconds(30)).until(() -> consumer.deadLettered() >= 1);

            // The attempt still advances. It belongs to the message, not to whichever of the two
            // mechanisms happened to hold the wait.
            assertThat(attemptNumbers).containsExactly(1, 2, 3);
            assertThat(exists("orders.new.retry.1s")).isFalse();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("a long wait sits in its rung with nothing consuming, and comes back one attempt on")
    void a_long_wait_sits_in_its_rung_until_the_time_is_up() {
        // Five seconds, with the threshold lowered well below it so the broker takes the wait.
        // Everything below is what a thirty-second wait does under the default; only the number
        // is small enough to run.
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(5))
                .withJitter(0)
                .waitInBrokerFrom(IN_BROKER_FROM);
        List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        // One delivery, one failure, and then the consumer is closed. Nothing is attached to
        // orders.new for the rest of this test, which is what makes the next assertions mean
        // something: whatever brings the message back is the broker, not a thread in here.
        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    attemptNumbers.add(message.attempt());
                    throw new IllegalStateException("inventory service is unreachable");
                })) {

            mq.publisher("orders", "order.placed").send("{\"id\":\"o-5\"}");
            await().atMost(Duration.ofSeconds(30)).until(() -> !attemptNumbers.isEmpty());
        }

        assertThat(attemptNumbers).containsExactly(1);
        assertThat(exists("orders.new.retry.5s")).isTrue();

        // Sitting in the rung, and nowhere else. The source queue is empty and unconsumed: the
        // message exists only as an entry in a queue whose time-to-live has not run out.
        await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount("orders.new.retry.5s") == 1);
        assertThat(mq.messageCount("orders.new")).isZero();

        // And then the broker returns it, on its own, one attempt further on. Nothing in this
        // process did that: the time-to-live expired and the rung's dead-letter target is
        // orders.new.
        await().atMost(Duration.ofSeconds(30)).until(() -> mq.messageCount("orders.new") == 1);
        assertThat(mq.messageCount("orders.new.retry.5s")).isZero();

        List<Integer> returned = new CopyOnWriteArrayList<>();
        try (MessageConsumer reader = mq.consume("orders.new", String.class,
                message -> returned.add(message.attempt()))) {
            await().atMost(Duration.ofSeconds(30)).until(() -> !returned.isEmpty());
            assertThat(returned.get(0)).isEqualTo(2);
        }
    }

    /**
     * Whether a queue is on the broker, asked without creating it.
     *
     * <p>A passive declare on its own channel. Publishing a probe would answer a different
     * question: the default exchange drops an unroutable message without complaining, so a
     * successful publish proves nothing about whether a queue was there to receive it.
     */
    private boolean exists(String queue) {
        try (TransportConnection connection = new RabbitMqTransport()
                .connect(ConnectionConfig.url(BROKER.getAmqpUrl()).build())) {
            return connection.queueExists(queue);
        } catch (Exception e) {
            throw new IllegalStateException("could not check whether " + queue + " exists", e);
        }
    }
}
