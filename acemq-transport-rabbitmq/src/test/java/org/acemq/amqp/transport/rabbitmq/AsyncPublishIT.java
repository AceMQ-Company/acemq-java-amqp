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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.transport.ConnectionConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Asynchronous publishing against a real broker.
 *
 * <p>The in-memory transport completes its futures immediately, so it can only test the contract.
 * Everything interesting about this feature lives here: real publisher confirms arrive out of
 * order and in ranges, a single confirm can acknowledge every sequence number below it, and an
 * unroutable message is returned on a separate frame *before* its confirm. A correlation bug in
 * any of that shows up as a future that never completes or completes with another message's
 * answer, and neither is visible without a broker.
 */
@Testcontainers
@DisplayName("asynchronous publishing against a real RabbitMQ")
class AsyncPublishIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

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
        if (mq != null && mq.isOpen()) {
            try {
                mq.deleteQueue("orders.new");
            } catch (RuntimeException e) {
                // Best effort.
            }
            mq.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("every message in a large batch is confirmed and arrives")
    void everyMessageIsConfirmedAndArrives() {
        int count = 2_000;
        List<String> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add("order-" + i);
        }

        List<PublishResult> results = mq.publisher("orders", "order.placed", String.class).sendAll(payloads);

        assertThat(results).hasSize(count);
        // Distinct identifiers prove the confirms were matched to their own messages rather
        // than one answer being handed to every future.
        assertThat(results).extracting(PublishResult::messageId).doesNotHaveDuplicates();
        await().atMost(Duration.ofSeconds(60))
                .until(() -> mq.messageCount("orders.new") == (long) count);
    }

    @Test
    @Timeout(180)
    @DisplayName("it is materially faster than publishing one at a time")
    void itIsFasterThanSynchronous() {
        int count = 400;
        List<String> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add("order-" + i);
        }
        var publisher = mq.publisher("orders", "order.placed", String.class);

        long syncStart = System.nanoTime();
        for (String payload : payloads) {
            publisher.send(payload);
        }
        Duration synchronous = Duration.ofNanos(System.nanoTime() - syncStart);

        long asyncStart = System.nanoTime();
        publisher.sendAll(payloads);
        Duration asynchronous = Duration.ofNanos(System.nanoTime() - asyncStart);

        // The whole justification for the feature. Deliberately a loose bound -- CI machines are
        // noisy and the point is the order of magnitude, not a benchmark. If pipelining ever
        // stops working, this fails; it will not fail because a runner was busy.
        assertThat(asynchronous)
                .as("async %s should beat sync %s for %d messages", asynchronous, synchronous, count)
                .isLessThan(synchronous);
    }

    @Test
    @Timeout(180)
    @DisplayName("an unroutable message fails its own future, and only that one")
    void unroutableFailsOnlyItsOwnFuture() {
        var routable = mq.publisher("orders", "order.placed", String.class);
        var nowhere = mq.publisher("orders", "nothing.listens", String.class);

        List<CompletableFuture<PublishResult>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(routable.sendAsync("good-" + i));
            futures.add(nowhere.sendAsync("bad-" + i));
        }

        int failed = 0;
        int confirmed = 0;
        for (CompletableFuture<PublishResult> future : futures) {
            try {
                future.join();
                confirmed++;
            } catch (RuntimeException e) {
                failed++;
            }
        }

        // Returns arrive on a separate frame before the confirm, so correlating them by message
        // id is the only way to tell these apart. Getting it wrong fails all of them or none.
        assertThat(confirmed).isEqualTo(20);
        assertThat(failed).isEqualTo(20);
    }

    @Test
    @Timeout(180)
    @DisplayName("publishing blocks rather than buffering without limit")
    void publishingIsBounded() {
        // A tiny ceiling, so the semaphore is certain to be exhausted. Nothing should fail:
        // the publisher is expected to wait for room, which is the difference between
        // backpressure and a memory leak.
        try (AceMq bounded = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl())
                .maxOutstandingPublishes(4)
                .build())) {
            bounded.declareExchange("orders", "topic");

            List<String> payloads = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                payloads.add("bounded-" + i);
            }

            List<PublishResult> results = bounded
                    .publisher("orders", "order.placed", String.class, PublishOptions.defaults().allowUnroutable())
                    .sendAll(payloads);

            assertThat(results).hasSize(200);
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("a batch that fails says how many succeeded")
    void failedBatchReportsThePartial() {
        assertThatThrownBy(() -> mq.publisher("orders", "nothing.listens", String.class)
                .sendAll(List.of("a", "b", "c")))
                .isInstanceOf(PublishFailedException.class)
                .hasMessageContaining("3 of 3");
    }
}
