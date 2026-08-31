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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.Replay;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Getting messages back out of a dead-letter queue.
 *
 * <p>Messages are seeded into {@code orders.new.dlq} by publishing to it directly, which is what
 * the retry dispatcher does when a message runs out of attempts. Driving a real handler to
 * exhaustion instead would be testing the retry ladder — which has its own tests — and would make
 * every assertion here depend on timing that has nothing to do with replay.
 */
@DisplayName("replay")
class ReplayTest {

    private AceMq mq;

    @BeforeEach
    void setUp() {
        mq = AceMq.connect("memory://replay-" + UUID.randomUUID());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.declareQueue("orders.new.dlq", QueueType.CLASSIC, Collections.emptyMap());
        mq.declareQueue("orders.new.parked", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
    }

    @AfterEach
    void tearDown() {
        if (mq != null) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    /** Publishes straight to a queue through the default exchange, as the dispatcher does. */
    private void seed(String queue, String payload, Envelope envelope) {
        mq.publisher("", queue, String.class).send(payload, envelope);
    }

    private void seedDead(String payload) {
        seed("orders.new.dlq", payload, Envelope.of("order.placed")
                .attempt(5)
                .error("the payment service was down")
                .build());
    }

    /** Consumes one message from a queue and returns its envelope. */
    private Envelope firstEnvelopeOn(String queue) throws Exception {
        java.util.concurrent.atomic.AtomicReference<Envelope> seen = new java.util.concurrent.atomic.AtomicReference<>();
        CountDownLatch received = new CountDownLatch(1);
        try (MessageConsumer consumer = mq.consume(queue, String.class, message -> {
            seen.compareAndSet(null, message.envelope());
            received.countDown();
        })) {
            assertThat(received.await(5, TimeUnit.SECONDS))
                    .as("expected a message on " + queue)
                    .isTrue();
        }
        return seen.get();
    }

    @Nested
    @DisplayName("moving messages")
    class Moving {

        @Test
        @DisplayName("counts what is waiting without moving it")
        void pendingMovesNothing() {
            seedDead("one");
            seedDead("two");

            Replay replay = mq.replay("orders.new");

            assertThat(replay.pending()).isEqualTo(2L);
            // Asked twice on purpose. A count that quietly drained the queue would be a
            // catastrophic thing to call before deciding whether to drain it.
            assertThat(replay.pending()).isEqualTo(2L);
            assertThat(mq.messageCount("orders.new")).isZero();
        }

        @Test
        @DisplayName("moves everything back to the queue it failed in")
        void replayAllMovesEverything() {
            seedDead("one");
            seedDead("two");
            seedDead("three");

            int moved = mq.replay("orders.new").replayAll();

            assertThat(moved).isEqualTo(3);
            assertThat(mq.messageCount("orders.new")).isEqualTo(3L);
            assertThat(mq.messageCount("orders.new.dlq")).isZero();
        }

        @Test
        @DisplayName("moves only as many as asked")
        void replayHonoursTheLimit() {
            for (int i = 0; i < 5; i++) {
                seedDead("message-" + i);
            }

            int moved = mq.replay("orders.new").replay(2);

            assertThat(moved).isEqualTo(2);
            assertThat(mq.messageCount("orders.new")).isEqualTo(2L);
            assertThat(mq.messageCount("orders.new.dlq"))
                    .as("the rest must stay put; a bounded replay that drains everything is not bounded")
                    .isEqualTo(3L);
        }

        @Test
        @DisplayName("stops when the queue runs out rather than waiting for more")
        void replayStopsOnAnEmptyQueue() {
            seedDead("only-one");

            assertThat(mq.replay("orders.new").replay(100)).isEqualTo(1);
        }

        @Test
        @DisplayName("moving nothing is not an error")
        void emptyQueueReplaysNothing() {
            assertThat(mq.replay("orders.new").replayAll()).isZero();
        }

        @Test
        @DisplayName("a replayed message is delivered to the consumer again")
        void replayedMessagesAreConsumable() throws Exception {
            seedDead("order-42");
            List<String> handled = new CopyOnWriteArrayList<>();
            CountDownLatch received = new CountDownLatch(1);

            mq.replay("orders.new").replayAll();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
                handled.add(message.payload());
                received.countDown();
            })) {
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(handled).containsExactly("order-42");
        }

        @Test
        @DisplayName("the body is returned unchanged")
        void bodyIsUntouched() throws Exception {
            // Matters most for the parking lot, where the body is by definition something this
            // library could not parse. Re-encoding it would bake the original bug into the copy.
            String awkward = "not json at all, {";
            seed("orders.new.parked", awkward, Envelope.of("order.placed").build());

            mq.replay("orders.new").parked().replayAll();

            List<String> handled = new CopyOnWriteArrayList<>();
            CountDownLatch received = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
                handled.add(message.payload());
                received.countDown();
            })) {
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(handled).containsExactly(awkward);
        }
    }

    @Nested
    @DisplayName("what it records")
    class Bookkeeping {

        @Test
        @DisplayName("resets the attempt counter")
        void attemptsAreReset() throws Exception {
            seedDead("one");

            mq.replay("orders.new").replayAll();

            // Left at 5, the message arrives already exhausted and the first failure
            // dead-letters it straight back.
            assertThat(firstEnvelopeOn("orders.new").attempt()).isEqualTo(1);
        }

        @Test
        @DisplayName("records where the message came from and when")
        void recordsProvenance() throws Exception {
            seedDead("one");

            mq.replay("orders.new").replayAll();

            Envelope envelope = firstEnvelopeOn("orders.new");
            assertThat(envelope.replayedFrom()).contains("orders.new.dlq");
            assertThat(envelope.replayedAt()).isPresent();
        }

        @Test
        @DisplayName("an ordinary message carries no replay provenance")
        void freshMessagesLookFresh() throws Exception {
            mq.publisher("orders", "order.created", String.class).send("brand-new");

            Envelope envelope = firstEnvelopeOn("orders.new");
            // The absence matters: provenance on a message that was never replayed would make
            // the field useless for spotting the ones that were.
            assertThat(envelope.replayedFrom()).isEmpty();
            assertThat(envelope.replayCount()).isZero();
        }

        @Test
        @DisplayName("keeps the error that put the message there")
        void keepsTheOriginalError() throws Exception {
            seedDead("one");

            mq.replay("orders.new").replayAll();

            // The one piece of context explaining why this message looks different from its
            // neighbours. Clearing it on the way back would throw it away.
            assertThat(firstEnvelopeOn("orders.new").error()).contains("the payment service was down");
        }

        @Test
        @DisplayName("counts how many times a message has been replayed")
        void countsRepeatedReplays() throws Exception {
            seedDead("stubborn");

            mq.replay("orders.new").replayAll();
            Envelope first = firstEnvelopeOn("orders.new");
            assertThat(first.replayCount()).isEqualTo(1);

            // Round two: the same message fails again and is replayed again.
            seed("orders.new.dlq", "stubborn", first);
            mq.replay("orders.new").replayAll();

            assertThat(firstEnvelopeOn("orders.new").replayCount())
                    .as("a message on its second trip through the dead-letter queue should say so")
                    .isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("the parking lot")
    class ParkingLot {

        @Test
        @DisplayName("is a different queue from the dead-letter queue")
        void parkedReadsTheParkingLot() {
            seedDead("dead");
            seed("orders.new.parked", "undecodable", Envelope.of("order.placed").build());

            Replay parked = mq.replay("orders.new").parked();

            assertThat(parked.from()).isEqualTo("orders.new.parked");
            assertThat(parked.to()).isEqualTo("orders.new");
            assertThat(parked.pending()).isEqualTo(1L);
            assertThat(parked.replayAll()).isEqualTo(1);
            assertThat(mq.messageCount("orders.new.dlq"))
                    .as("replaying the parking lot must leave the dead-letter queue alone")
                    .isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("filters")
    class Filters {

        @Test
        @DisplayName("moves what matches")
        void movesMatchingMessages() {
            seedDead("one");
            seedDead("two");

            int moved = mq.replay("orders.new")
                    .replay(10, delivery -> new String(delivery.body(), StandardCharsets.UTF_8).contains("one"));

            assertThat(moved).isEqualTo(1);
        }

        @Test
        @DisplayName("stops at the first message it rejects, leaving it in place")
        void stopsAtTheFirstRejection() {
            seedDead("keep-me");
            seedDead("and-me");

            int moved = mq.replay("orders.new").replay(10, delivery -> false);

            assertThat(moved).isZero();
            // Requeued rather than consumed. Holding every rejected message in order to look
            // past it would pull the whole queue into memory, so the drain stops instead.
            assertThat(mq.messageCount("orders.new.dlq")).isEqualTo(2L);
        }
    }

    @Nested
    @DisplayName("refusals")
    class Refusals {

        @Test
        @DisplayName("a non-positive batch size is rejected")
        void refusesNonPositiveMax() {
            assertThatThrownBy(() -> mq.replay("orders.new").replay(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("max");
        }
    }
}
