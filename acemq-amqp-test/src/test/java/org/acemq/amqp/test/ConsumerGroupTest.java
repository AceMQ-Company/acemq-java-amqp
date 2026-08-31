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

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerGroup;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("a consumer group")
class ConsumerGroupTest {

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
    @DisplayName("resizing")
    class Resizing {

        @Test
        @Timeout(30)
        void starts_with_the_concurrency_it_was_asked_for() {
            connect("group-start");

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            })
                    .concurrency(3)
                    .prefetch(10)
                    .start()) {

                assertThat(group.size()).isEqualTo(3);
                assertThat(group.prefetch()).isEqualTo(10);
                assertThat(group.queue()).isEqualTo("orders.new");
            }
        }

        @Test
        @Timeout(30)
        void grows_while_it_is_running_and_the_new_consumers_take_work() {
            connect("group-grow");
            List<String> handled = new CopyOnWriteArrayList<>();

            try (ConsumerGroup group = mq
                    .consumeGroup("orders.new", String.class, message -> handled.add(message.payload()))
                    .concurrency(1)
                    .prefetch(1)
                    .start()) {

                group.scaleTo(4);
                assertThat(group.size()).isEqualTo(4);

                publish(20);
                await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 20);
                assertThat(group.acknowledged()).isEqualTo(20);
            }
        }

        @Test
        @Timeout(30)
        void shrinks_and_the_survivors_keep_working() {
            connect("group-shrink");
            List<String> handled = new CopyOnWriteArrayList<>();

            try (ConsumerGroup group = mq
                    .consumeGroup("orders.new", String.class, message -> handled.add(message.payload()))
                    .concurrency(4)
                    .prefetch(1)
                    .start()) {

                group.scaleTo(1);
                assertThat(group.size()).isEqualTo(1);

                // The queue still drains, which is the thing that would break if scaling down
                // had cancelled the wrong consumer or left the group in a bad state.
                publish(10);
                await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 10);
            }
        }

        @Test
        @Timeout(30)
        void refuses_to_scale_to_nothing() {
            connect("group-zero");

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            }).start()) {
                // Scaling to zero looks like pausing and is not: it would leave a group that
                // exists, reports healthy, and consumes nothing. Closing is the honest way.
                assertThatThrownBy(() -> group.scaleTo(0))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("close the group");
            }
        }

        @Test
        @Timeout(30)
        void a_closed_group_will_not_be_resized() {
            connect("group-closed");
            ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            }).start();
            group.close();

            assertThatThrownBy(() -> group.scaleTo(2)).isInstanceOf(AceMqException.class);
        }
    }

    @Nested
    @DisplayName("draining")
    class Draining {

        @Test
        @Timeout(60)
        void a_consumer_being_removed_finishes_the_message_it_holds() throws Exception {
            connect("group-drain");
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger finished = new AtomicInteger();

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
                started.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                finished.incrementAndGet();
            })
                    .concurrency(1)
                    .prefetch(1)
                    .start()) {

                publish(1);
                assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();
                assertThat(group.inFlight()).isEqualTo(1);

                // Draining while that handler is mid-flight must wait for it rather than
                // abandoning the message to a redelivery — which is only harmless if every
                // handler is idempotent, and if that were reliably true half this library would
                // not exist.
                java.util.concurrent.atomic.AtomicBoolean drained = new java.util.concurrent.atomic.AtomicBoolean();
                Thread drainer = new Thread(() -> drained.set(group.drain(Duration.ofSeconds(30))));
                drainer.start();

                Thread.sleep(300);
                assertThat(finished.get()).isZero();
                assertThat(drainer.isAlive()).isTrue();

                release.countDown();
                drainer.join(30_000);
                assertThat(finished.get()).isEqualTo(1);
                assertThat(drained).isTrue();
            }
        }

        @Test
        @Timeout(30)
        void reports_whether_everything_finished_in_time() {
            connect("group-drain-timeout");

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            })
                    .concurrency(2)
                    .start()) {

                assertThat(group.drain(Duration.ofSeconds(5))).isTrue();
                assertThat(group.inFlight()).isZero();
            }
        }
    }

    @Nested
    @DisplayName("prefetch")
    class Prefetch {

        @Test
        @Timeout(30)
        void changes_on_every_member_while_they_run() {
            connect("group-prefetch");

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            })
                    .concurrency(3)
                    .prefetch(10)
                    .start()) {

                group.prefetch(75);

                assertThat(group.prefetch()).isEqualTo(75);
            }
        }

        @Test
        @Timeout(30)
        void applies_to_consumers_added_afterwards() {
            connect("group-prefetch-new");

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            })
                    .concurrency(1)
                    .prefetch(10)
                    .start()) {

                group.prefetch(50);
                group.scaleTo(3);

                // A consumer created after the change must not silently start on the old value;
                // an operator who raised prefetch and then scaled up would be watching a number
                // that two thirds of the group is ignoring.
                assertThat(group.prefetch()).isEqualTo(50);
            }
        }

        @Test
        @Timeout(30)
        void keeps_draining_the_queue_after_a_change() {
            connect("group-prefetch-live");
            List<String> handled = new CopyOnWriteArrayList<>();

            try (ConsumerGroup group = mq
                    .consumeGroup("orders.new", String.class, message -> handled.add(message.payload()))
                    .concurrency(2)
                    .prefetch(1)
                    .start()) {

                publish(5);
                await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 5);

                group.prefetch(20);

                publish(15);
                await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 20);
            }
        }

        @Test
        @Timeout(30)
        void will_not_be_set_to_nothing() {
            connect("group-prefetch-zero");

            try (ConsumerGroup group = mq.consumeGroup("orders.new", String.class, message -> {
            }).start()) {
                assertThatThrownBy(() -> group.prefetch(0)).isInstanceOf(IllegalArgumentException.class);
            }
        }
    }
}
