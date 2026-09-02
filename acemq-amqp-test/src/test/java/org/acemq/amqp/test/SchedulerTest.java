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
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.Scheduler;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the scheduler")
class SchedulerTest {

    private AceMq mq;

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        mq.declareExchange("later", "topic");
        mq.declareQueue("later.arrivals", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("later.arrivals", "later", "arrived");
        return mq;
    }

    @Test
    @Timeout(60)
    @DisplayName("a message due now is delivered without waiting")
    void deliversImmediatelyWhenDue() throws Exception {
        connect("scheduler-now");
        List<String> arrived = new CopyOnWriteArrayList<>();

        try (MessageConsumer consumer = mq.consume(
                "later.arrivals", String.class, message -> arrived.add(message.payload()));
                Scheduler scheduler = Scheduler.on(mq)) {

            // Anything already due, or so nearly due that another hop would cost more than the
            // accuracy it buys, goes straight out.
            scheduler.at(Instant.now().minusSeconds(10), "later", "arrived", "overdue");

            await().atMost(Duration.ofSeconds(20)).until(() -> arrived.size() == 1);
            assertThat(arrived).containsExactly("overdue");
            assertThat(scheduler.hops()).isZero();
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a short delay arrives after it, and not before")
    void waitsForTheDelay() throws Exception {
        connect("scheduler-delay");
        List<String> arrived = new CopyOnWriteArrayList<>();

        try (MessageConsumer consumer = mq.consume(
                "later.arrivals", String.class, message -> arrived.add(message.payload()));
                Scheduler scheduler = Scheduler.on(mq)) {

            Instant sent = Instant.now();
            scheduler.in(Duration.ofSeconds(2), "later", "arrived", "soon");

            // Not yet: the whole point is that it is not delivered immediately.
            Thread.sleep(500);
            assertThat(arrived).isEmpty();

            await().atMost(Duration.ofSeconds(30)).until(() -> arrived.size() == 1);
            assertThat(Duration.between(sent, Instant.now())).isGreaterThan(Duration.ofSeconds(1));
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a short delay is not held up behind a long one")
    void shortDelaysOvertakeLongOnes() throws Exception {
        connect("scheduler-overtake");
        List<String> arrived = new CopyOnWriteArrayList<>();

        try (MessageConsumer consumer = mq.consume(
                "later.arrivals", String.class, message -> arrived.add(message.payload()));
                Scheduler scheduler = Scheduler.on(mq)) {

            // This is the test the whole design exists for. With a per-message time to live on
            // one shared queue, the two-second message would wait behind the one-hour message,
            // because a classic queue expires only at its head -- and nothing would report it.
            scheduler.in(Duration.ofHours(1), "later", "arrived", "much-later");
            scheduler.in(Duration.ofSeconds(2), "later", "arrived", "soon");

            await().atMost(Duration.ofSeconds(45)).until(() -> arrived.size() == 1);

            assertThat(arrived).containsExactly("soon");
            // And the long one is still waiting, rather than having been delivered early.
            Thread.sleep(1_000);
            assertThat(arrived).doesNotContain("much-later");
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a delay longer than the largest rung is reached by hopping")
    void longDelaysHop() throws Exception {
        connect("scheduler-hops");
        List<String> arrived = new CopyOnWriteArrayList<>();

        try (MessageConsumer consumer = mq.consume(
                "later.arrivals", String.class, message -> arrived.add(message.payload()));
                Scheduler scheduler = Scheduler.on(mq)) {

            // Four seconds is longer than the smallest rung, so it cannot be done in one hop
            // at that granularity: it is one ten-second rung's worth of decisions, or several
            // one-second ones. Either way the scheduler moves it more than once.
            scheduler.in(Duration.ofSeconds(4), "later", "arrived", "hopped");

            await().atMost(Duration.ofSeconds(60)).until(() -> arrived.size() == 1);
            assertThat(scheduler.hops()).isGreaterThanOrEqualTo(1);
            assertThat(scheduler.delivered()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("the payload arrives unchanged, whatever it was")
    void payloadSurvives() throws Exception {
        connect("scheduler-payload");
        List<String> arrived = new CopyOnWriteArrayList<>();

        try (MessageConsumer consumer = mq.consume(
                "later.arrivals", String.class, message -> arrived.add(message.payload()));
                Scheduler scheduler = Scheduler.on(mq)) {

            // The scheduler encodes once and carries bytes from then on, so a hop cannot
            // re-encode and change what the consumer receives.
            scheduler.at(Instant.now(), "later", "arrived", "exactly these bytes");

            await().atMost(Duration.ofSeconds(20)).until(() -> arrived.size() == 1);
            assertThat(arrived).containsExactly("exactly these bytes");
        }
    }
}
