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
 * What closing a connection costs when several consumer groups are still holding work.
 *
 * <p>A shutdown budget is a number that has to fit inside something — usually Kubernetes'
 * thirty-second {@code terminationGracePeriodSeconds}. A budget spent once per group is not a
 * budget: three groups at thirty seconds each is ninety seconds, the pod is killed at thirty,
 * and every message still held is redelivered anyway. Draining exists to avoid exactly that
 * outcome, so arriving at it slowly is the worst of both.
 *
 * <p>Against a real broker rather than the in-memory transport on purpose. The test double
 * waits on its own dispatcher threads when a subscription closes, which would swamp the figure
 * this measures; RabbitMQ's transport cancels and closes the channel without waiting, so what
 * is timed here is the draining itself — the same thing a real shutdown spends.
 */
@Testcontainers
@DisplayName("closing a connection with several consumer groups still working")
class GracefulShutdownIT {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(BrokerImage.current());

    /** Generous, and per group: a connection that handed each its own would take three times it. */
    private static final Duration PER_GROUP = Duration.ofSeconds(5);

    /** The whole shutdown budget, as an orchestrator's grace period would supply it. */
    private static final Duration BUDGET = Duration.ofMillis(800);

    private static final int GROUPS = 3;
    private static final int PER_GROUP_CONSUMERS = 2;

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
    @DisplayName("the budget is spent once in total, not once per group")
    void closingSharesOneBudgetAcrossEveryGroup() {
        mq = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl()).build());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.shutdown");
        mq.bind("orders.shutdown", "orders", "order.*");

        for (int i = 0; i < GROUPS; i++) {
            mq.consumeGroup("orders.shutdown", String.class, message -> hold())
                    .concurrency(PER_GROUP_CONSUMERS)
                    .prefetch(1)
                    .start()
                    .drainTimeout(PER_GROUP);
        }

        int held = GROUPS * PER_GROUP_CONSUMERS;
        for (int i = 0; i < held; i++) {
            mq.publisher("orders", "order.created", String.class).send("order-" + i);
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> mq.inFlight() == held);

        long startedAt = System.nanoTime();
        mq.close(BUDGET);
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(took)
                .as("three groups given %s each would have taken %s", PER_GROUP, PER_GROUP.multipliedBy(GROUPS))
                .isLessThan(Duration.ofSeconds(3));
    }

    @Test
    @Timeout(120)
    @DisplayName("closing a consumer waits for the running handler, acknowledges it and hands out nothing new")
    void closingAConsumerWaitsForTheRunningHandler() {
        closeWhileHandling("orders.close-consumer", consumer -> consumer.close());
    }

    @Test
    @Timeout(120)
    @DisplayName("closing the connection waits for a plain consumer's running handler within the budget")
    void closingTheConnectionWaitsForAPlainConsumer() {
        closeWhileHandling("orders.close-connection", consumer -> mq.close(Duration.ofSeconds(10)));
    }

    /**
     * TCK CONSUME-002, in this library's own suite: m1's handler is running when close is called,
     * m2 waits behind prefetch 1. Up to 0.7.18 close returned at once and m1 was handled twice.
     */
    private void closeWhileHandling(String queue,
            java.util.function.Consumer<org.acemq.amqp.core.MessageConsumer> close) {
        mq = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl()).build());
        mq.declareQueue(queue);
        java.util.List<String> handled = new java.util.concurrent.CopyOnWriteArrayList<>();
        org.acemq.amqp.core.MessageConsumer consumer = mq.consume(queue, String.class,
                org.acemq.amqp.core.ConsumerOptions.prefetch(1), message -> {
                    handled.add(message.payload());
                    Thread.sleep(1500);
                });
        mq.publisher("", queue, String.class).send("m1");
        mq.publisher("", queue, String.class).send("m2");
        await().atMost(Duration.ofSeconds(30)).until(() -> handled.size() == 1);

        long startedAt = System.nanoTime();
        close.accept(consumer);
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(took).as("close must wait for the handler").isGreaterThan(Duration.ofMillis(700));
        assertThat(handled).containsExactly("m1");
        try (AceMq again = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl()).build())) {
            java.util.List<String> after = new java.util.concurrent.CopyOnWriteArrayList<>();
            again.consume(queue, String.class, message -> after.add(message.payload()));
            await().atMost(Duration.ofSeconds(30)).until(() -> !after.isEmpty());
            await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() -> true);
            assertThat(after).as("m1 was acknowledged, m2 never handed out").containsExactly("m2");
        }
    }

    /** Holds the message until the test lets go, so every consumer is still busy at close. */
    private void hold() {
        try {
            release.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
