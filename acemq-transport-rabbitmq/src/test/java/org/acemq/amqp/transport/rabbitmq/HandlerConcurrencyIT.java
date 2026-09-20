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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * How many handlers really run at once, against the number that was asked for.
 *
 * <p>Concurrency is documented as the knob for handlers that spend their time waiting on a
 * database or an HTTP call, so the right number has nothing to do with how many cores the
 * machine has. The RabbitMQ client disagrees by default: left to itself it dispatches every
 * consumer on the connection from a fixed pool of {@code availableProcessors()} threads, and
 * the surplus consumers simply wait. Nothing reports it — the consumers exist, the broker has
 * delivered to them, and the handlers have not started.
 *
 * <p>Deliberately asks for more than the machine has, so the assertion means the same thing on
 * a laptop with sixteen cores and on a two-core runner. Pinned here rather than against the
 * in-memory transport because the cap lives in the client, which only the real transport uses.
 */
@Testcontainers
@DisplayName("running as many handlers at once as concurrency asked for")
class HandlerConcurrencyIT {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(BrokerImage.current());

    /**
     * More consumers than the machine has cores, which is the whole point.
     *
     * <p>Doubling and adding two rather than picking a constant: a constant large enough to
     * exceed a build agent is small enough to be met by a workstation, and the test would then
     * pass everywhere the bug existed.
     */
    private static final int CONCURRENCY = Runtime.getRuntime().availableProcessors() * 2 + 2;

    private final CountDownLatch release = new CountDownLatch(1);

    private AceMq mq;

    @AfterEach
    void tearDown() {
        // Always, so a held handler cannot outlive the test and wedge the next one.
        release.countDown();
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("every consumer handles a message at the same time, whatever the core count")
    void concurrencyIsNotCappedByTheNumberOfCores() {
        mq = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl()).build());
        mq.declareQueue("orders.concurrency");

        mq.consumeGroup("orders.concurrency", String.class, message -> hold())
                .concurrency(CONCURRENCY)
                .prefetch(1)
                .start();

        for (int i = 0; i < CONCURRENCY; i++) {
            mq.publisher("", "orders.concurrency", String.class).send("order-" + i);
        }

        // One message per consumer, and every consumer inside its handler at the same moment.
        // Capped at the core count this stops at availableProcessors() and never moves again,
        // so the failure is a timeout rather than a wrong number.
        await().atMost(Duration.ofSeconds(30)).until(() -> mq.inFlight() == CONCURRENCY);
        assertThat(mq.inFlight())
                .as("%d consumers were asked for on a machine reporting %d processors",
                        CONCURRENCY, Runtime.getRuntime().availableProcessors())
                .isEqualTo(CONCURRENCY);
    }

    /** Holds the message until the test lets go, so every consumer is busy at the same time. */
    private void hold() {
        try {
            release.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
