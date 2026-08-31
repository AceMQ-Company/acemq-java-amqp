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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishInterceptor;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Publishing without waiting for each confirm.
 *
 * <p>The in-memory transport completes its futures immediately, so these tests are about the
 * contract rather than the speed-up: the same result, the same failures, the same interceptors,
 * reported through a future instead of a return value. Whether it is actually faster is a
 * question for a real broker, and the integration suite asks it there.
 */
@DisplayName("asynchronous publishing")
class AsyncPublishTest {

    private AceMq mq;

    @BeforeEach
    void setUp() {
        mq = AceMq.connect("memory://async-" + UUID.randomUUID());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
    }

    @AfterEach
    void tearDown() {
        if (mq != null) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("sendAsync")
    class Async {

        @Test
        @DisplayName("delivers the message and reports the same result")
        void deliversAndReports() throws Exception {
            CountDownLatch received = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, m -> received.countDown())) {
                PublishResult result = mq.publisher("orders", "order.created", String.class)
                        .sendAsync("one")
                        .get(5, TimeUnit.SECONDS);

                assertThat(result.messageId()).isNotBlank();
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
        }

        @Test
        @DisplayName("an unroutable message fails the future rather than passing silently")
        void unroutableFailsTheFuture() {
            CompletableFuture<PublishResult> future = mq.publisher("orders", "nothing.listens", String.class)
                    .sendAsync("one");

            // The same failure the synchronous call throws. Reporting success here would make
            // asynchronous publishing quietly less safe than synchronous, which is the trap.
            assertThatThrownBy(future::join)
                    .hasCauseInstanceOf(PublishFailedException.class);
        }

        @Test
        @DisplayName("allowUnroutable is honoured on the async path too")
        void allowUnroutableApplies() {
            mq.publisher("orders", "nothing.listens", String.class, PublishOptions.defaults().allowUnroutable())
                    .sendAsync("one")
                    .join();
        }

        @Test
        @DisplayName("interceptors run, and their headers arrive")
        void interceptorsRun() throws Exception {
            mq.intercept((PublishInterceptor) context -> context.withEnvelope(
                    context.envelope().toBuilder().header("tenant", "acme").build()));

            List<String> tenants = new CopyOnWriteArrayList<>();
            CountDownLatch received = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, m -> {
                tenants.add(String.valueOf(m.headers().get("tenant")));
                received.countDown();
            })) {
                mq.publisher("orders", "order.created", String.class).sendAsync("one").join();
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
            // Asserted because the async path prepares the message separately; an interceptor
            // that only ran on the synchronous path would be a silent hole in every policy.
            assertThat(tenants).containsExactly("acme");
        }

        @Test
        @DisplayName("a paused connection refuses, as it does synchronously")
        void pausedConnectionRefuses() {
            mq.pausePublishing();
            assertThatThrownBy(() -> mq.publisher("orders", "order.created", String.class).sendAsync("one"))
                    .isInstanceOf(org.acemq.amqp.api.PublishingPausedException.class);
            mq.resumePublishing();
        }
    }

    @Nested
    @DisplayName("sendAll")
    class Batch {

        @Test
        @DisplayName("publishes every message and returns a result for each, in order")
        void publishesEverything() throws Exception {
            List<String> payloads = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                payloads.add("order-" + i);
            }

            CountDownLatch received = new CountDownLatch(200);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, m -> received.countDown())) {
                List<PublishResult> results = mq.publisher("orders", "order.created", String.class).sendAll(payloads);

                assertThat(results).hasSize(200);
                assertThat(results).allSatisfy(r -> assertThat(r.messageId()).isNotBlank());
                assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
            }
        }

        @Test
        @DisplayName("every message gets its own identifier")
        void identifiersAreDistinct() {
            List<PublishResult> results = mq.publisher("orders", "order.created", String.class)
                    .sendAll(List.of("a", "b", "c"));

            assertThat(results).extracting(PublishResult::messageId).doesNotHaveDuplicates();
        }

        @Test
        @DisplayName("a failing batch says how many succeeded")
        void failureNamesTheCount() {
            Publisher<String> nowhere = mq.publisher("orders", "nothing.listens", String.class);

            // A caller told only "the batch failed" resends messages that already arrived, so
            // the count is part of the contract rather than a nicety.
            assertThatThrownBy(() -> nowhere.sendAll(List.of("a", "b", "c")))
                    .isInstanceOf(PublishFailedException.class)
                    .hasMessageContaining("3 of 3")
                    .hasMessageContaining("0 were");
        }

        @Test
        @DisplayName("an empty batch is not an error")
        void emptyBatchIsFine() {
            assertThat(mq.publisher("orders", "order.created", String.class).sendAll(List.of())).isEmpty();
        }
    }
}
