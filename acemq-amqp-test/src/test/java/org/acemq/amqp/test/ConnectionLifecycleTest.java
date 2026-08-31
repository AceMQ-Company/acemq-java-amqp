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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.PublishingPausedException;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("taking a connection out of rotation")
class ConnectionLifecycleTest {

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

    private void publish(int count) {
        for (int i = 0; i < count; i++) {
            mq.publisher("orders", "order.placed", String.class).send("order-" + i);
        }
    }

    @Nested
    @DisplayName("pausing publishing")
    class PausingPublishing {

        @Test
        @Timeout(20)
        void refuses_a_publish_and_says_it_sent_nothing() {
            connect("lifecycle-pause-publish");

            mq.pausePublishing();

            assertThat(mq.isPublishingPaused()).isTrue();
            assertThatThrownBy(() -> mq.publisher("orders", "order.placed", String.class).send("nope"))
                    .isInstanceOf(PublishingPausedException.class)
                    .hasMessageContaining("nothing was sent")
                    .hasMessageContaining("orders/order.placed");
        }

        @Test
        @Timeout(20)
        void is_its_own_exception_so_a_caller_can_tell_it_apart() {
            connect("lifecycle-pause-type");
            mq.pausePublishing();

            // Distinguishable from a broker rejection on purpose: this one means "not now",
            // and retrying shortly is the right response where retrying a rejected message
            // usually is not.
            assertThatThrownBy(() -> mq.publisher("orders", "order.placed", String.class).send("nope"))
                    .isInstanceOf(org.acemq.amqp.api.AceMqException.class)
                    .isInstanceOf(PublishingPausedException.class);
        }

        @Test
        @Timeout(20)
        void resuming_lets_messages_through_again() {
            connect("lifecycle-resume-publish");
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class, m -> received.add(m.payload()))) {

                mq.pausePublishing();
                assertThatThrownBy(() -> publish(1)).isInstanceOf(PublishingPausedException.class);

                mq.resumePublishing();
                assertThat(mq.isPublishingPaused()).isFalse();
                publish(3);

                await().atMost(Duration.ofSeconds(15)).until(() -> received.size() == 3);
            }
        }

        @Test
        @Timeout(20)
        void pausing_twice_is_harmless_and_so_is_resuming_twice() {
            connect("lifecycle-idempotent");

            mq.pausePublishing();
            mq.pausePublishing();
            assertThat(mq.isPublishingPaused()).isTrue();

            mq.resumePublishing();
            mq.resumePublishing();
            assertThat(mq.isPublishingPaused()).isFalse();
        }
    }

    @Nested
    @DisplayName("pausing consuming")
    class PausingConsuming {

        @Test
        @Timeout(30)
        void stops_delivery_and_starts_it_again() {
            connect("lifecycle-pause-consume");
            List<String> received = new CopyOnWriteArrayList<>();

            try (MessageConsumer consumer = mq.consume("orders.new", String.class, m -> received.add(m.payload()))) {

                mq.pauseConsuming();
                assertThat(consumer.isPaused()).isTrue();
                assertThat(mq.isConsumingPaused()).isTrue();

                publish(5);
                // Nothing should arrive while paused. Waiting is the only way to assert an
                // absence, so this sleeps rather than polls.
                sleep(500);
                assertThat(received).isEmpty();

                mq.resumeConsuming();
                assertThat(consumer.isPaused()).isFalse();

                // The messages were on the queue the whole time, not lost.
                await().atMost(Duration.ofSeconds(20)).until(() -> received.size() == 5);
            }
        }

        @Test
        @Timeout(30)
        void publishing_keeps_working_while_consuming_is_paused() {
            connect("lifecycle-asymmetry");

            try (MessageConsumer ignored = mq.consume("orders.new", String.class, m -> {
            })) {
                mq.pauseConsuming();

                // The asymmetry is the point: a service being taken out of rotation still has
                // requests to finish, and those requests still need to publish.
                publish(3);
                assertThat(mq.isPublishingPaused()).isFalse();
            }
        }
    }

    @Nested
    @DisplayName("draining")
    class Draining {

        @Test
        @Timeout(60)
        void waits_for_a_handler_that_is_still_running() throws Exception {
            connect("lifecycle-drain");
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger finished = new AtomicInteger();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class, m -> {
                started.countDown();
                release.await(30, TimeUnit.SECONDS);
                finished.incrementAndGet();
            })) {

                publish(1);
                assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();
                assertThat(mq.inFlight()).isEqualTo(1);

                java.util.concurrent.atomic.AtomicBoolean quiet = new java.util.concurrent.atomic.AtomicBoolean();
                Thread drainer = new Thread(() -> quiet.set(mq.drainConsumers(Duration.ofSeconds(30))));
                drainer.start();

                sleep(400);
                assertThat(finished.get()).isZero();
                assertThat(drainer.isAlive()).isTrue();

                release.countDown();
                drainer.join(30_000);

                assertThat(finished.get()).isEqualTo(1);
                assertThat(quiet).isTrue();
                assertThat(mq.inFlight()).isZero();
            }
        }

        @Test
        @Timeout(30)
        void reports_when_something_is_still_running_after_the_timeout() throws Exception {
            connect("lifecycle-drain-timeout");
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            try (MessageConsumer ignored = mq.consume("orders.new", String.class, m -> {
                started.countDown();
                release.await(30, TimeUnit.SECONDS);
            })) {

                publish(1);
                assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();

                // False rather than an exception: the caller now has a decision to make —
                // wait longer, or accept a redelivery — and only they can make it.
                assertThat(mq.drainConsumers(Duration.ofMillis(300))).isFalse();

                release.countDown();
            }
        }

        @Test
        @Timeout(30)
        void leaves_publishers_alone() {
            connect("lifecycle-drain-publishers");

            try (MessageConsumer ignored = mq.consume("orders.new", String.class, m -> {
            })) {
                assertThat(mq.drainConsumers(Duration.ofSeconds(5))).isTrue();

                // Stop consuming first, stop publishing second. That order is what makes a
                // cutover clean, so draining must not take the publishers with it.
                assertThat(mq.isPublishingPaused()).isFalse();
                publish(2);
            }
        }
    }

    @Nested
    @DisplayName("the cutover, in order")
    class Cutover {

        @Test
        @Timeout(60)
        void stop_consuming_finish_the_work_then_stop_publishing() {
            connect("lifecycle-cutover");
            List<String> handled = new CopyOnWriteArrayList<>();

            try (MessageConsumer ignored = mq.consume("orders.new", String.class, m -> handled.add(m.payload()))) {

                publish(4);
                await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 4);

                // 1. Stop taking new work and let what is in hand finish.
                assertThat(mq.drainConsumers(Duration.ofSeconds(20))).isTrue();
                assertThat(mq.inFlight()).isZero();

                // 2. In-flight requests can still publish their results.
                publish(1);

                // 3. Now refuse publishing too. The service is out of rotation.
                mq.pausePublishing();
                assertThatThrownBy(() -> publish(1)).isInstanceOf(PublishingPausedException.class);

                // Nothing new was consumed after the drain, and the message published in step
                // two is still on the queue for whoever takes over.
                assertThat(handled).hasSize(4);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
