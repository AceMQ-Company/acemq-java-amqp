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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.IdempotencyStore;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the idempotent consumer")
class IdempotentConsumerTest {

    private AceMq mq;

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("deduplication")
    class Deduplication {

        @Test
        @Timeout(20)
        void handles_the_same_message_once_however_often_it_is_delivered() {
            connect("idem-dedupe");
            AtomicInteger handled = new AtomicInteger();
            IdempotencyStore store = InMemoryIdempotencyStore.forOneDay();

            try (MessageConsumer consumer = mq.consume(
                    "orders.new",
                    String.class,
                    ConsumerOptions.prefetch(1).idempotent(store),
                    message -> handled.incrementAndGet())) {

                // The same envelope published five times is what a redelivery looks like from
                // the consumer's side: same identifier, five deliveries.
                Envelope envelope = Envelope.of("order.placed").build();
                for (int i = 0; i < 5; i++) {
                    mq.publisher("orders", "order.placed").send("payload", envelope);
                }

                await().atMost(Duration.ofSeconds(10))
                        .until(() -> consumer.acknowledged() + consumer.duplicates() == 5);

                assertThat(handled).hasValue(1);
                assertThat(consumer.duplicates()).isEqualTo(4);
                assertThat(consumer.rejected()).isZero();
            }
        }

        @Test
        @Timeout(20)
        void treats_different_messages_as_different_work() {
            connect("idem-distinct");
            AtomicInteger handled = new AtomicInteger();

            try (MessageConsumer consumer = mq.consume(
                    "orders.new",
                    String.class,
                    ConsumerOptions.prefetch(1).idempotent(InMemoryIdempotencyStore.forOneDay()),
                    message -> handled.incrementAndGet())) {

                for (int i = 0; i < 10; i++) {
                    mq.publisher("orders", "order.placed").send("payload-" + i);
                }

                await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 10);
                assertThat(consumer.duplicates()).isZero();
            }
        }

        @Test
        @Timeout(20)
        void counts_nothing_as_duplicate_without_a_store() {
            connect("idem-off");
            AtomicInteger handled = new AtomicInteger();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> handled.incrementAndGet())) {
                Envelope envelope = Envelope.of("order.placed").build();
                mq.publisher("orders", "order.placed").send("payload", envelope);
                mq.publisher("orders", "order.placed").send("payload", envelope);

                // Without a store the engine has no opinion: both deliveries are handled, which
                // is exactly the behaviour the pattern exists to change.
                await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 2);
                assertThat(consumer.duplicates()).isZero();
            }
        }
    }

    @Nested
    @DisplayName("failure")
    class Failure {

        @Test
        @Timeout(30)
        void a_failed_attempt_can_be_retried_rather_than_being_swallowed_as_a_duplicate() {
            connect("idem-retry");
            AtomicInteger attempts = new AtomicInteger();
            RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofMillis(50)).withJitter(0);

            try (MessageConsumer consumer = mq.consume(
                    "orders.new",
                    String.class,
                    ConsumerOptions.prefetch(1)
                            .withRetry(policy)
                            .idempotent(InMemoryIdempotencyStore.forOneDay()),
                    message -> {
                        // Fails twice, then succeeds. Were the claim kept on failure, attempts
                        // two and three would be discarded as duplicates and the message would
                        // be lost to a transient error.
                        if (attempts.incrementAndGet() < 3) {
                            throw new IllegalStateException("not yet");
                        }
                    })) {

                mq.publisher("orders", "order.placed").send("payload");

                await().atMost(Duration.ofSeconds(15)).until(() -> consumer.acknowledged() == 1);
                assertThat(attempts).hasValue(3);
                assertThat(consumer.deadLettered()).isZero();
            }
        }

        @Test
        @Timeout(20)
        void a_store_that_throws_does_not_stop_delivery() {
            connect("idem-broken-store");
            AtomicInteger handled = new AtomicInteger();

            IdempotencyStore broken = new IdempotencyStore() {
                @Override
                public boolean claim(String messageId) {
                    return true;
                }

                @Override
                public void confirm(String messageId) {
                    // silently fine
                }

                @Override
                public void release(String messageId) {
                    throw new IllegalStateException("the store is unavailable");
                }

                @Override
                public boolean isConfirmed(String messageId) {
                    return false;
                }
            };

            try (MessageConsumer consumer = mq.consume(
                    "orders.new", String.class, ConsumerOptions.prefetch(1).idempotent(broken), message -> {
                        handled.incrementAndGet();
                        throw new IllegalStateException("handler failed");
                    })) {

                mq.publisher("orders", "order.placed").send("payload");

                // The store failing while releasing must not leave the delivery unsettled: an
                // unsettled delivery holds a prefetch slot until the connection drops.
                await().atMost(Duration.ofSeconds(10)).until(() -> consumer.rejected() == 1);
                assertThat(handled).hasValue(1);
            }
        }
    }

    @Nested
    @DisplayName("the store itself")
    class Store {

        @Test
        void claims_once_and_refuses_the_second_caller() {
            InMemoryIdempotencyStore store = InMemoryIdempotencyStore.forOneDay();

            assertThat(store.claim("m-1")).isTrue();
            assertThat(store.claim("m-1")).isFalse();
        }

        @Test
        void a_released_claim_can_be_taken_again() {
            InMemoryIdempotencyStore store = InMemoryIdempotencyStore.forOneDay();

            store.claim("m-1");
            store.release("m-1");

            assertThat(store.claim("m-1")).isTrue();
            assertThat(store.isConfirmed("m-1")).isFalse();
        }

        @Test
        void a_confirmed_identifier_stays_refused() {
            InMemoryIdempotencyStore store = InMemoryIdempotencyStore.forOneDay();

            store.claim("m-1");
            store.confirm("m-1");

            assertThat(store.isConfirmed("m-1")).isTrue();
            assertThat(store.claim("m-1")).isFalse();
        }

        @Test
        void forgets_an_identifier_once_the_retention_has_passed() throws Exception {
            InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(Duration.ofMillis(50));

            store.claim("m-1");
            store.confirm("m-1");
            assertThat(store.isConfirmed("m-1")).isTrue();

            Thread.sleep(80);

            // Retention is what stops the store growing forever, and its consequence is that a
            // duplicate arriving long enough after the original is handled again.
            assertThat(store.isConfirmed("m-1")).isFalse();
            assertThat(store.claim("m-1")).isTrue();
        }

        @Test
        void stays_within_its_size_cap() {
            InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(Duration.ofHours(1), 50);

            for (int i = 0; i < 500; i++) {
                store.claim("m-" + i);
                store.confirm("m-" + i);
            }

            // An unbounded store is a memory leak with a business purpose.
            assertThat(store.size()).isLessThanOrEqualTo(50);
            assertThat(store.evictions()).isPositive();
        }

        @Test
        @Timeout(20)
        void gives_the_claim_to_exactly_one_of_many_racing_threads() throws Exception {
            InMemoryIdempotencyStore store = InMemoryIdempotencyStore.forOneDay();
            int threads = 32;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch startTogether = new CountDownLatch(1);
            List<Boolean> outcomes = new CopyOnWriteArrayList<>();

            try {
                for (int i = 0; i < threads; i++) {
                    pool.execute(() -> {
                        try {
                            startTogether.await();
                            outcomes.add(store.claim("contended"));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
                startTogether.countDown();
                pool.shutdown();
                assertThat(pool.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            } finally {
                pool.shutdownNow();
            }

            // The whole point of the store: under contention exactly one caller may proceed.
            // A check-then-act implementation passes every other test here and fails this one.
            assertThat(outcomes).hasSize(threads);
            assertThat(outcomes.stream().filter(Boolean::booleanValue).count()).isEqualTo(1);
        }
    }
}
