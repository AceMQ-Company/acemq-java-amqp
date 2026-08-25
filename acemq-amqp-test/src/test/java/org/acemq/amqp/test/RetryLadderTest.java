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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The retry ladder, exercised end to end without a broker container.
 *
 * <p>Delays here are milliseconds rather than seconds. The mechanism is identical; only the
 * numbers change, which is what makes it possible to assert on retry behaviour in a suite that
 * finishes in seconds rather than minutes.
 */
@DisplayName("the retry ladder")
class RetryLadderTest {

    private AceMq mq;

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
    @Timeout(30)
    void redelivers_a_failing_message_until_the_attempts_run_out() {
        connect("retry-attempts");
        AtomicInteger attempts = new AtomicInteger();
        List<Integer> attemptNumbers = new CopyOnWriteArrayList<>();

        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new",
                String.class,
                ConsumerOptions.prefetch(1).withRetry(policy),
                message -> {
                    attempts.incrementAndGet();
                    attemptNumbers.add(message.attempt());
                    throw new IllegalStateException("downstream is unreachable");
                })) {

            mq.publisher("orders", "order.placed").send("payload");

            // Three deliveries in total: the original plus two retries.
            await().atMost(Duration.ofSeconds(10)).until(() -> attempts.get() == 3);
            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.deadLettered() == 1);

            assertThat(attemptNumbers).containsExactly(1, 2, 3);
            assertThat(consumer.retried()).isEqualTo(2);
            assertThat(consumer.acknowledged()).isZero();

            // Nothing further arrives once it has been dead-lettered.
            assertThat(attempts).hasValue(3);
        }
    }

    @Test
    @Timeout(30)
    void carries_the_attempt_counter_across_retries() {
        connect("retry-counter");
        List<Message<String>> seen = new CopyOnWriteArrayList<>();
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    seen.add(message);
                    throw new IllegalStateException("still failing");
                })) {

            mq.publisher("orders", "order.placed").send("payload");
            await().atMost(Duration.ofSeconds(10)).until(() -> seen.size() == 3);

            // Same message identity throughout: a retry is the same message, not a new one.
            String firstId = seen.get(0).envelope().id();
            assertThat(seen).allSatisfy(m -> assertThat(m.envelope().id()).isEqualTo(firstId));
            assertThat(seen.get(0).isFirstAttempt()).isTrue();
            assertThat(seen.get(2).isFirstAttempt()).isFalse();

            // The age is measured from the original publish, not from the last retry, which is
            // what makes an age-based give-up meaningful.
            assertThat(seen.get(2).envelope().firstSeen())
                    .isEqualTo(seen.get(0).envelope().firstSeen());
        }
    }

    @Test
    @Timeout(30)
    void succeeds_on_a_later_attempt_and_stops_retrying() {
        connect("retry-recovers");
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("not yet");
                    }
                })) {

            mq.publisher("orders", "order.placed").send("payload");

            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.acknowledged() == 1);
            assertThat(attempts).hasValue(3);
            assertThat(consumer.retried()).isEqualTo(2);
            assertThat(consumer.deadLettered()).isZero();
        }
    }

    @Test
    @Timeout(30)
    void waits_the_configured_delay_before_trying_again() {
        connect("retry-timing");
        List<Long> times = new CopyOnWriteArrayList<>();
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofMillis(300)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    times.add(System.currentTimeMillis());
                    throw new IllegalStateException("failing");
                })) {

            mq.publisher("orders", "order.placed").send("payload");
            await().atMost(Duration.ofSeconds(15)).until(() -> times.size() == 3);

            // The gap must reflect the configured delay. A handler that slept would produce
            // the same gap, so the accompanying evidence is that prefetch stayed free: see
            // does_not_block_other_messages_while_one_is_waiting.
            long firstGap = times.get(1) - times.get(0);
            assertThat(firstGap).isGreaterThanOrEqualTo(250L);
        }
    }

    @Test
    @Timeout(30)
    void does_not_block_other_messages_while_one_is_waiting() {
        connect("retry-nonblocking");
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger successes = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofMillis(500)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    if (message.payload().startsWith("poison")) {
                        failures.incrementAndGet();
                        throw new IllegalStateException("this one always fails");
                    }
                    successes.incrementAndGet();
                })) {

            mq.publisher("orders", "order.placed").send("poison");
            for (int i = 0; i < 10; i++) {
                mq.publisher("orders", "order.placed").send("good-" + i);
            }

            // This is the whole point of a broker-side ladder. With a sleeping handler and a
            // prefetch of one, the ten good messages would be stuck behind half a second of
            // sleep per failed attempt. Here they should sail past while the poison waits.
            await().atMost(Duration.ofSeconds(5)).until(() -> successes.get() == 10);
            assertThat(failures.get()).isLessThan(3);
        }
    }

    @Test
    @Timeout(30)
    void sends_a_fatal_failure_straight_to_the_dead_letter_queue() {
        connect("retry-fatal");
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    attempts.incrementAndGet();
                    throw new AceFatalException("this payload will never be valid");
                })) {

            mq.publisher("orders", "order.placed").send("payload");

            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.deadLettered() == 1);
            // Retrying cannot help, so none of the five attempts are spent.
            assertThat(attempts).hasValue(1);
            assertThat(consumer.retried()).isZero();
        }
    }

    @Test
    @Timeout(30)
    void parks_a_message_that_cannot_be_decoded_instead_of_retrying_it() {
        connect("retry-parked");
        RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", Integer.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    throw new AssertionError("the handler must not run when decoding fails");
                })) {

            mq.publisher("orders", "order.placed").send("not a number");

            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.rejected() == 1);
            // A payload that will not parse now will not parse later either.
            assertThat(consumer.retried()).isZero();
        }
    }

    @Test
    @Timeout(30)
    void a_dead_lettered_message_can_be_read_back_with_the_reason_attached() {
        connect("retry-dlq-contents");
        RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer failing = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    throw new IllegalStateException("inventory service is down");
                })) {

            mq.publisher("orders", "order.placed").send("payload");
            await().atMost(Duration.ofSeconds(10)).until(() -> failing.deadLettered() == 1);
        }

        // The dead-letter queue is an ordinary queue, so recovering from it is ordinary
        // consumption rather than a special tool.
        List<Message<String>> dead = new CopyOnWriteArrayList<>();
        try (MessageConsumer reader = mq.consume("orders.new.dlq", String.class, dead::add)) {
            await().atMost(Duration.ofSeconds(10)).until(() -> !dead.isEmpty());

            Message<String> message = dead.get(0);
            assertThat(message.payload()).isEqualTo("payload");
            assertThat(message.attempt()).isEqualTo(2);
            // The reason is an envelope field, not a raw header, so a consumer of the
            // dead-letter queue does not have to know the wire header name to read it.
            assertThat(message.envelope().error())
                    .hasValueSatisfying(reason -> assertThat(reason)
                            .contains("exhausted 2 attempts")
                            .contains("inventory service is down"));
        }
    }

    @Test
    @Timeout(30)
    void creates_one_retry_queue_per_distinct_delay_rather_than_per_attempt() {
        connect("retry-queues");
        // Ten attempts, but the exponential schedule reaches its ceiling quickly, so the
        // distinct delays are 50ms, 100ms and 200ms.
        RetryPolicy policy = RetryPolicy.exponential(10, Duration.ofMillis(50), 2.0, Duration.ofMillis(200))
                .withJitter(0);

        try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                })) {

            // Publishing into a queue that does not exist fails, so a successful publish to
            // each name is proof the rung was declared.
            assertThat(publishesSuccessfully("orders.new.retry.50ms")).isTrue();
            assertThat(publishesSuccessfully("orders.new.retry.100ms")).isTrue();
            assertThat(publishesSuccessfully("orders.new.retry.200ms")).isTrue();
            assertThat(publishesSuccessfully("orders.new.dlq")).isTrue();
            assertThat(publishesSuccessfully("orders.new.parked")).isTrue();

            // No rung was created for the seven attempts that all share the 200ms ceiling.
            assertThat(publishesSuccessfully("orders.new.retry.400ms")).isFalse();
        }
    }

    /** Publishes straight to a queue through the default exchange. */
    private boolean publishesSuccessfully(String queue) {
        try {
            mq.publisher("", queue).send("probe");
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
