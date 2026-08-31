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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Partitioning;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.OrderedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("an ordered queue")
class OrderedQueueTest {

    private AceMq mq;

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        return mq;
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    /** {@code customer-3:7} — the key, then where it sits in that key's sequence. */
    private static String event(String customer, int sequence) {
        return customer + ":" + sequence;
    }

    private static String customerOf(String event) {
        return event.substring(0, event.indexOf(':'));
    }

    private static int sequenceOf(String event) {
        return Integer.parseInt(event.substring(event.indexOf(':') + 1));
    }

    @Nested
    @DisplayName("the partitioning")
    class Partitions {

        @Test
        void puts_a_key_in_the_same_place_every_time() {
            for (int i = 0; i < 200; i++) {
                String key = "customer-" + i;
                assertThat(Partitioning.partitionFor(key, 8)).isEqualTo(Partitioning.partitionFor(key, 8));
            }
        }

        @Test
        void spreads_keys_across_every_partition() {
            Map<Integer, Integer> counts = new HashMap<>();
            for (int i = 0; i < 2000; i++) {
                counts.merge(Partitioning.partitionFor("customer-" + i, 8), 1, Integer::sum);
            }

            assertThat(counts).hasSize(8);
            // No partition should be starved or swamped. FNV-1a on short keys is good enough
            // that a 2x deviation from the mean would mean something is wrong.
            assertThat(counts.values()).allSatisfy(count -> assertThat(count).isBetween(125, 500));
        }

        @Test
        void hashes_to_values_a_port_in_another_language_must_reproduce() {
            // Golden values. These are the wire contract: a Go publisher that computes something
            // else puts a customer's messages in two partitions and reorders them, and nothing
            // else in any test suite would notice.
            assertThat(Partitioning.hash("")).isEqualTo(0x811c9dc5);
            assertThat(Partitioning.hash("a")).isEqualTo(0xe40c292c);
            assertThat(Partitioning.hash("foobar")).isEqualTo(0xbf9cf968);
        }

        @Test
        void never_produces_a_negative_partition() {
            // The hash is signed, so a plain % would give a negative index for about half of
            // all keys, and the failure would look like a missing queue.
            for (int i = 0; i < 5000; i++) {
                assertThat(Partitioning.partitionFor("k-" + i, 8)).isBetween(0, 7);
            }
        }

        @Test
        void refuses_a_message_with_no_key() {
            assertThatThrownBy(() -> Partitioning.partitionFor(null, 4))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not be null");
            assertThatThrownBy(() -> Partitioning.partitionFor("k", 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("ordering")
    class Ordering {

        @Test
        @Timeout(60)
        void keeps_each_key_in_sequence_while_running_many_keys_at_once() {
            connect("ordered-sequence");
            Map<String, List<Integer>> seen = new ConcurrentHashMap<>();

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(4)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .declare()) {

                orders.consume(message -> {
                    String event = message.payload();
                    seen.computeIfAbsent(customerOf(event), k -> new CopyOnWriteArrayList<>())
                            .add(sequenceOf(event));
                });

                // Twelve keys interleaved, so any partition holds several of them and the
                // sequences are genuinely mixed on the way in.
                for (int sequence = 0; sequence < 25; sequence++) {
                    for (int customer = 0; customer < 12; customer++) {
                        orders.send(event("customer-" + customer, sequence));
                    }
                }

                await().atMost(Duration.ofSeconds(45)).until(() -> orders.handled() == 300);

                assertThat(seen).hasSize(12);
                seen.forEach((customer, sequences) -> {
                    List<Integer> expected = new ArrayList<>();
                    for (int i = 0; i < 25; i++) {
                        expected.add(i);
                    }
                    // The whole point: within one key, exactly the order it was sent in.
                    assertThat(sequences).as("sequence for %s", customer).containsExactlyElementsOf(expected);
                });
            }
        }

        @Test
        @Timeout(30)
        void sends_one_key_to_one_partition_and_says_which() {
            connect("ordered-routing");

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(8)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .declare()) {

                int first = orders.send(event("customer-7", 0));
                int second = orders.send(event("customer-7", 1));

                assertThat(first).isEqualTo(second);
                assertThat(orders.queues()).hasSize(8).contains("orders.p0", "orders.p7");
                assertThat(orders.queueFor(first)).isEqualTo("orders.p" + first);
            }
        }

        @Test
        @Timeout(30)
        void runs_different_keys_in_parallel() {
            connect("ordered-parallel");

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(8)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .declare()) {

                java.util.Set<Integer> used = new java.util.HashSet<>();
                for (int customer = 0; customer < 40; customer++) {
                    used.add(orders.send(event("customer-" + customer, 0)));
                }

                // Forty keys over eight partitions: parallelism across keys is the other half
                // of the trade, and a implementation that funnelled everything into one queue
                // would still pass the ordering test above.
                assertThat(used).hasSize(8);
            }
        }
    }

    @Nested
    @DisplayName("when a handler fails")
    class Failure {

        @Test
        @Timeout(60)
        void stopping_halts_only_the_partition_that_failed() {
            connect("ordered-stop");
            List<String> handled = new CopyOnWriteArrayList<>();

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(4)
                    .prefetch(1)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .declare()) {

                String poison = event("customer-0", 0);
                int poisonedPartition = Partitioning.partitionFor("customer-0", 4);

                orders.consume(message -> {
                    if (poison.equals(message.payload())) {
                        throw new IllegalStateException("this one is bad");
                    }
                    handled.add(message.payload());
                });

                orders.send(poison);
                // Keys that land elsewhere must keep flowing; a bad message stops a quarter of
                // the traffic, not all of it.
                List<String> elsewhere = new ArrayList<>();
                for (int customer = 1; customer < 40; customer++) {
                    if (Partitioning.partitionFor("customer-" + customer, 4) != poisonedPartition) {
                        String event = event("customer-" + customer, 0);
                        elsewhere.add(event);
                        orders.send(event);
                    }
                }

                await().atMost(Duration.ofSeconds(45)).until(() -> handled.size() == elsewhere.size());
                await().atMost(Duration.ofSeconds(20)).until(() -> !orders.haltedPartitions().isEmpty());

                assertThat(orders.haltedPartitions()).containsExactly(poisonedPartition);
                assertThat(orders.failed()).isPositive();
                assertThat(orders.skipped()).isZero();
            }
        }

        @Test
        @Timeout(60)
        void retrying_in_place_holds_the_partition_and_then_carries_on() {
            connect("ordered-retry");
            AtomicInteger attempts = new AtomicInteger();
            List<String> handled = new CopyOnWriteArrayList<>();

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(2)
                    .prefetch(1)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .onFailure(OrderedQueue.OnFailure.RETRY_IN_PLACE, 4, Duration.ofMillis(50))
                    .declare()) {

                orders.consume(message -> {
                    // Fails twice and then works, which is what a transient failure looks like.
                    if (message.payload().endsWith(":0") && attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("not yet");
                    }
                    handled.add(message.payload());
                });

                orders.send(event("customer-1", 0));
                orders.send(event("customer-1", 1));

                await().atMost(Duration.ofSeconds(45)).until(() -> handled.size() == 2);

                // Retrying in place is the only way message 0 precedes message 1 here. A retry
                // ladder would have parked 0 and delivered 1 first.
                assertThat(handled).containsExactly(event("customer-1", 0), event("customer-1", 1));
                assertThat(orders.haltedPartitions()).isEmpty();
                assertThat(orders.failed()).isEqualTo(2);
            }
        }

        @Test
        @Timeout(60)
        void skipping_leaves_a_gap_and_counts_it() {
            connect("ordered-skip");
            List<String> handled = new CopyOnWriteArrayList<>();

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(1)
                    .prefetch(1)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .onFailure(OrderedQueue.OnFailure.SKIP)
                    .declare()) {

                orders.consume(message -> {
                    if (message.payload().endsWith(":1")) {
                        throw new IllegalStateException("this one is bad");
                    }
                    handled.add(message.payload());
                });

                orders.send(event("customer-1", 0));
                orders.send(event("customer-1", 1));
                orders.send(event("customer-1", 2));

                await().atMost(Duration.ofSeconds(45)).until(() -> handled.size() == 2);

                // The order of what survives is intact, and one message has left the sequence
                // with nothing but this counter to say so.
                assertThat(handled).containsExactly(event("customer-1", 0), event("customer-1", 2));
                assertThat(orders.skipped()).isEqualTo(1);
                assertThat(orders.haltedPartitions()).isEmpty();
            }
        }
    }

    @Nested
    @DisplayName("building one")
    class Building {

        @Test
        @Timeout(30)
        void needs_a_key() {
            connect("ordered-nokey");

            assertThatThrownBy(() -> mq.ordered("orders", String.class).partitions(4).declare())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("keyedBy")
                    .hasMessageContaining("ordinary queue with extra steps");
        }

        @Test
        @Timeout(30)
        void refuses_settings_that_cannot_work() {
            connect("ordered-bad");

            assertThatThrownBy(() -> mq.ordered("orders", String.class).partitions(0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> mq.ordered("orders", String.class).prefetch(0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> mq.ordered("orders", String.class)
                    .onFailure(OrderedQueue.OnFailure.RETRY_IN_PLACE, 0, Duration.ofSeconds(1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @Timeout(30)
        void says_what_it_is_in_its_own_words() {
            connect("ordered-tostring");

            try (OrderedQueue<String> orders = mq.ordered("orders", String.class)
                    .partitions(3)
                    .keyedBy(OrderedQueueTest::customerOf)
                    .declare()) {

                assertThat(orders.name()).isEqualTo("orders");
                assertThat(orders.partitions()).isEqualTo(3);
                assertThat(orders.toString()).contains("orders").contains("partitions=3");
            }
        }
    }
}
