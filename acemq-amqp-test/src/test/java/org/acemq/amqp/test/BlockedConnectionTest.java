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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionBlockedException;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Publishing to a broker that has stopped accepting messages.
 *
 * <p>The behaviour under test is a wait with a bound. Failing at once would turn a brief memory
 * alarm into an application outage; waiting without a bound is the hang this exists to remove.
 */
class BlockedConnectionTest {

    @AfterEach
    void tearDown() {
        InMemoryTransport.reset();
    }

    @Test
    @DisplayName("a publish waits for the broker to unblock, then succeeds")
    void publishWaitsForUnblock() throws Exception {
        String broker = "blocked-resumes";
        try (AceMq mq = AceMq
                .connect(ConnectionConfig.url("memory://" + broker).blockedTimeout(Duration.ofSeconds(10)).build())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, java.util.Map.of());
            mq.bind("orders.new", "orders", "order.*");

            InMemoryTransport.block(broker, "low on memory");
            assertThat(mq.isBlocked()).isTrue();
            assertThat(mq.blockedReason()).contains("low on memory");

            CountDownLatch publishing = new CountDownLatch(1);
            CountDownLatch published = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread publisher = new Thread(() -> {
                publishing.countDown();
                try {
                    mq.publisher("orders", "order.created", String.class).send("one");
                    published.countDown();
                } catch (Throwable t) {
                    failure.set(t);
                    published.countDown();
                }
            }, "blocked-publisher");
            publisher.start();

            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            // It must still be waiting: the whole point is that it did not fail on its own.
            assertThat(published.await(300, TimeUnit.MILLISECONDS))
                    .as("the publish should still be waiting while the broker is blocked")
                    .isFalse();

            InMemoryTransport.unblock(broker);

            assertThat(published.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            assertThat(mq.isBlocked()).isFalse();
            assertThat(mq.blockedReason()).isEmpty();
            publisher.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    @DisplayName("a publish gives up once the blocked timeout expires, and says why")
    void publishFailsOnceTheTimeoutExpires() {
        String broker = "blocked-forever";
        try (AceMq mq = AceMq
                .connect(ConnectionConfig.url("memory://" + broker).blockedTimeout(Duration.ofMillis(200)).build())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, java.util.Map.of());
            mq.bind("orders.new", "orders", "order.*");

            InMemoryTransport.block(broker, "low on disk");

            assertThatThrownBy(() -> mq.publisher("orders", "order.created", String.class).send("one"))
                    .isInstanceOf(ConnectionBlockedException.class)
                    // The operator's next action depends on which alarm it was, so it has to survive
                    // into the exception rather than being flattened into "publish failed".
                    .hasMessageContaining("low on disk")
                    .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.throwable(
                            ConnectionBlockedException.class))
                    .extracting(ConnectionBlockedException::reason)
                    .isEqualTo("low on disk");
        }
    }

    @Test
    @DisplayName("nothing is published when the wait times out")
    void nothingIsPublishedWhenTheWaitTimesOut() throws Exception {
        String broker = "blocked-no-send";
        try (AceMq mq = AceMq
                .connect(ConnectionConfig.url("memory://" + broker).blockedTimeout(Duration.ofMillis(100)).build())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, java.util.Map.of());
            mq.bind("orders.new", "orders", "order.*");

            InMemoryTransport.block(broker, "low on memory");
            assertThatThrownBy(() -> mq.publisher("orders", "order.created", String.class).send("lost"))
                    .isInstanceOf(ConnectionBlockedException.class);
            InMemoryTransport.unblock(broker);

            // Asserting the absence, because the failure mode worth catching is a message that
            // was queued while blocked and delivered later, after the caller was told it failed.
            CountDownLatch received = new CountDownLatch(1);
            try (var subscription = mq.consume("orders.new", String.class, message -> received.countDown())) {
                assertThat(received.await(500, TimeUnit.MILLISECONDS))
                        .as("a publish that threw must not have left a message behind")
                        .isFalse();
            }
        }
    }

    @Test
    @DisplayName("consuming carries on while publishing is blocked")
    void consumingCarriesOnWhileBlocked() throws Exception {
        String broker = "blocked-consume";
        try (AceMq mq = AceMq
                .connect(ConnectionConfig.url("memory://" + broker).blockedTimeout(Duration.ofSeconds(5)).build())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, java.util.Map.of());
            mq.bind("orders.new", "orders", "order.*");
            mq.publisher("orders", "order.created", String.class).send("before the alarm");

            InMemoryTransport.block(broker, "low on memory");

            // A resource alarm stops publishers, not consumers — draining the queue is how the
            // broker recovers, so a client that stopped consuming would prolong the outage.
            CountDownLatch received = new CountDownLatch(1);
            try (var subscription = mq.consume("orders.new", String.class, message -> received.countDown())) {
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
