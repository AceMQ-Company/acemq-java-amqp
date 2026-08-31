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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.PublishOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Publish options against a real broker.
 *
 * <p>The in-memory tests exercise our own expiry implementation, which proves the library does
 * what it says and nothing about AMQP. Expiry in particular is worth re-proving here: the protocol
 * carries a time-to-live as a string of milliseconds in the {@code expiration} property, and
 * RabbitMQ rejects a badly typed one rather than ignoring it — so a mistake in that mapping would
 * show up as a dead channel in production and as a passing test everywhere else.
 */
@Testcontainers
@DisplayName("publish options against a real RabbitMQ")
class PublishOptionsIT {

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

    @AfterEach
    void disconnect() {
        if (mq != null) {
            mq.deleteQueue("orders.new");
            mq.close();
        }
    }

    @Test
    @Timeout(90)
    @DisplayName("the broker discards a message that outlives its expiry")
    void expiredMessagesAreDropped() throws Exception {
        mq.publisher("orders", "order.created", String.class,
                PublishOptions.defaults().expiringAfter(Duration.ofMillis(500)))
                .send("stale");

        // Nobody is consuming, so it waits in the queue and runs out of time there.
        Thread.sleep(2_000);

        CountDownLatch received = new CountDownLatch(1);
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> received.countDown())) {
            assertThat(received.await(3, TimeUnit.SECONDS))
                    .as("a message past its expiry must not be delivered late")
                    .isFalse();
        }
    }

    @Test
    @Timeout(90)
    @DisplayName("a message consumed within its expiry arrives")
    void liveMessagesArrive() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> received.countDown())) {
            mq.publisher("orders", "order.created", String.class,
                    PublishOptions.defaults().expiringAfter(Duration.ofSeconds(30)))
                    .send("fresh");

            assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @Timeout(90)
    @DisplayName("allowUnroutable accepts what the default reports as a failure")
    void unroutableIsAcceptedOnlyWhenAsked() {
        assertThatThrownBy(() -> mq.publisher("orders", "nothing.listens", String.class).send("one"))
                .isInstanceOf(PublishFailedException.class);

        mq.publisher("orders", "nothing.listens", String.class, PublishOptions.defaults().allowUnroutable())
                .send("one");
    }

    @Test
    @Timeout(90)
    @DisplayName("a transient message is still delivered while the broker is up")
    void transientMessagesAreDelivered() throws Exception {
        // Transient is about surviving a restart, not about arriving. Worth asserting because the
        // delivery mode is the easiest property to set on the wrong message.
        CountDownLatch received = new CountDownLatch(1);
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> received.countDown())) {
            mq.publisher("orders", "order.created", String.class, PublishOptions.transientDelivery())
                    .send("quick");

            assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
