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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;
import org.acemq.amqp.patterns.OutboxRelay;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the outbox relay")
class OutboxRelayTest {

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

    private static OutboxRecord order(String id, String routingKey) {
        return OutboxRecord.of(
                "orders", routingKey, Envelope.of("order.placed").id(id).build(), "{\"order\":\"" + id + "\"}");
    }

    @Nested
    @DisplayName("draining")
    class Draining {

        @Test
        @Timeout(20)
        void publishes_what_the_outbox_holds_and_marks_it() {
            connect("outbox-drain");
            RecordingOutboxStore store = new RecordingOutboxStore();
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()));
                    OutboxRelay relay = new OutboxRelay(mq, store)) {

                store.add(order("o-1", "order.placed"));
                store.add(order("o-2", "order.placed"));

                assertThat(relay.drainOnce()).isEqualTo(2);

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 2);
                assertThat(store.published()).containsExactly("o-1", "o-2");
                assertThat(store.pendingCount()).isZero();
                assertThat(relay.published()).isEqualTo(2);
                assertThat(consumer.acknowledged()).isEqualTo(2);
            }
        }

        @Test
        @Timeout(20)
        void publishes_oldest_first() {
            connect("outbox-order");
            RecordingOutboxStore store = new RecordingOutboxStore();
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()));
                    OutboxRelay relay = new OutboxRelay(mq, store)) {

                for (int i = 0; i < 5; i++) {
                    store.add(order("o-" + i, "order.placed"));
                }
                relay.drain();

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 5);

                // A single relay preserves the order records were written in. Two relays would
                // not, which is a trade worth making knowingly rather than discovering.
                assertThat(store.published()).containsExactly("o-0", "o-1", "o-2", "o-3", "o-4");
            }
        }

        @Test
        @Timeout(20)
        void an_empty_outbox_is_not_an_error() {
            connect("outbox-empty");
            try (OutboxRelay relay = new OutboxRelay(mq, new RecordingOutboxStore())) {
                assertThat(relay.drainOnce()).isZero();
                assertThat(relay.drain()).isZero();
            }
        }

        @Test
        @Timeout(30)
        void drains_on_its_own_thread_once_started() {
            connect("outbox-background");
            RecordingOutboxStore store = new RecordingOutboxStore();
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()));
                    OutboxRelay relay = new OutboxRelay(mq, store, 10, Duration.ofMillis(50), Duration.ofSeconds(30))) {

                relay.start();
                relay.start(); // starting twice must not start a second thread
                assertThat(relay.isRunning()).isTrue();

                store.add(order("o-1", "order.placed"));

                // Nothing called drain: the point of the relay is that a request never waits for
                // the broker, so the publishing has to happen without anyone asking.
                await().atMost(Duration.ofSeconds(15)).until(() -> received.size() == 1);
                assertThat(store.published()).containsExactly("o-1");
            }
        }
    }

    @Nested
    @DisplayName("the identifier it publishes with")
    class Identity {

        @Test
        @Timeout(20)
        void keeps_the_record_identifier_so_a_duplicate_can_be_recognised() {
            connect("outbox-identity");
            RecordingOutboxStore store = new RecordingOutboxStore();
            List<String> handled = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume(
                    "orders.new",
                    String.class,
                    ConsumerOptions.prefetch(1).idempotent(InMemoryIdempotencyStore.forOneDay()),
                    message -> handled.add(message.envelope().id()));
                    OutboxRelay relay = new OutboxRelay(mq, store)) {

                store.add(order("o-42", "order.placed"));
                relay.drainOnce();
                await().atMost(Duration.ofSeconds(10)).until(() -> handled.size() == 1);

                // The relay publishing the same record twice is not hypothetical: it is what
                // happens whenever it dies between the broker's confirm and the row being marked.
                // Carrying the record's identifier through is what lets the consumer absorb it.
                store.add(order("o-42", "order.placed"));
                relay.drainOnce();

                await().atMost(Duration.ofSeconds(10)).until(() -> consumer.duplicates() == 1);
                assertThat(handled).containsExactly("o-42");
            }
        }

        @Test
        @Timeout(10)
        void carries_correlation_through_to_the_published_envelope() {
            Envelope source = Envelope.of("order.placed")
                    .id("o-7")
                    .correlationId("checkout-99")
                    .causationId("cmd-1")
                    .build();

            Envelope published = OutboxRecord.of("orders", "order.placed", source, "{}").envelope();

            assertThat(published.id()).isEqualTo("o-7");
            assertThat(published.correlationId()).isEqualTo("checkout-99");
            assertThat(published.causationId()).contains("cmd-1");
            assertThat(published.type()).isEqualTo("order.placed");
        }
    }

    @Nested
    @DisplayName("failure")
    class Failure {

        @Test
        @Timeout(20)
        void one_unpublishable_record_does_not_hold_up_the_rest() {
            connect("outbox-partial");
            RecordingOutboxStore store = new RecordingOutboxStore();
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()));
                    OutboxRelay relay = new OutboxRelay(mq, store)) {

                store.add(order("o-good-1", "order.placed"));
                store.add(order("o-bad", "nothing.bound.here"));
                store.add(order("o-good-2", "order.placed"));

                assertThat(relay.drainOnce()).isEqualTo(2);

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 2);
                assertThat(store.published()).containsExactly("o-good-1", "o-good-2");
                assertThat(relay.failed()).isEqualTo(1);
                assertThat(store.failures()).hasSize(1);
                assertThat(store.attemptsFor("o-bad")).isEqualTo(1);
            }
        }

        @Test
        @Timeout(20)
        void a_record_stops_being_claimed_once_it_has_failed_enough() {
            connect("outbox-exhausted");
            RecordingOutboxStore store = new RecordingOutboxStore(3);

            try (OutboxRelay relay = new OutboxRelay(mq, store)) {
                store.add(order("o-bad", "nothing.bound.here"));

                for (int attempt = 0; attempt < 5; attempt++) {
                    relay.drainOnce();
                }

                // Three attempts and then it stops. A message that cannot be published is a
                // problem to be seen, not one to be retried into the next outage.
                assertThat(relay.failed()).isEqualTo(3);
                assertThat(store.attemptsFor("o-bad")).isEqualTo(3);
                assertThat(store.pendingCount()).isEqualTo(1);
            }
        }

        @Test
        @Timeout(20)
        void a_store_that_cannot_mark_a_published_record_does_not_lose_the_batch() {
            connect("outbox-mark-fails");
            List<String> received = new CopyOnWriteArrayList<>();
            RecordingOutboxStore store = new RecordingOutboxStore() {
                @Override
                public synchronized void markPublished(String id) {
                    super.markPublished(id);
                    throw new IllegalStateException("the database is unavailable");
                }
            };

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()));
                    OutboxRelay relay = new OutboxRelay(mq, store)) {

                store.add(order("o-1", "order.placed"));
                store.add(order("o-2", "order.placed"));

                // The messages are already at the broker by the time marking fails, so the only
                // sane response is to carry on: the alternative abandons the rest of the batch to
                // avoid a duplicate that an idempotent consumer would have absorbed anyway.
                assertThat(relay.drainOnce()).isEqualTo(2);
                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 2);
            }
        }

        @Test
        @Timeout(30)
        void a_failing_pass_does_not_kill_the_background_thread() {
            connect("outbox-thread-survives");
            List<String> received = new CopyOnWriteArrayList<>();
            RecordingOutboxStore store = new RecordingOutboxStore() {
                private int claims;

                @Override
                public synchronized List<OutboxRecord> claimBatch(int batchSize, Duration lease) {
                    if (++claims <= 2) {
                        throw new IllegalStateException("the database is unavailable");
                    }
                    return super.claimBatch(batchSize, lease);
                }
            };

            try (MessageConsumer ignored = mq.consume("orders.new", String.class,
                    message -> received.add(message.payload()));
                    OutboxRelay relay = new OutboxRelay(mq, store, 10, Duration.ofMillis(50), Duration.ofSeconds(30))) {

                relay.start();
                store.add(order("o-1", "order.placed"));

                // scheduleWithFixedDelay cancels a task for good the first time it throws, and
                // says nothing. Without the catch inside the relay this test hangs: the relay is
                // dead, the outbox fills, and nothing reports why.
                await().atMost(Duration.ofSeconds(20)).until(() -> received.size() == 1);
            }
        }
    }

    @Nested
    @DisplayName("configuration")
    class Configuration {

        @Test
        void rejects_settings_that_cannot_work() {
            connect("outbox-config");
            RecordingOutboxStore store = new RecordingOutboxStore();

            assertThatThrownBy(() -> new OutboxRelay(mq, store, 0, Duration.ofSeconds(1), Duration.ofSeconds(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchSize");
            assertThatThrownBy(() -> new OutboxRelay(mq, store, 1, Duration.ZERO, Duration.ofSeconds(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("pollInterval");
            assertThatThrownBy(() -> new OutboxRelay(mq, store, 1, Duration.ofSeconds(1), Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("lease");
        }

        @Test
        @Timeout(10)
        void closing_stops_the_thread_and_can_be_done_twice() {
            connect("outbox-close");
            OutboxRelay relay = new OutboxRelay(mq, new RecordingOutboxStore());
            relay.start();
            relay.close();
            relay.close();

            assertThat(relay.isRunning()).isFalse();
            assertThat(relay.toString()).contains("OutboxRelay");
        }
    }
}
