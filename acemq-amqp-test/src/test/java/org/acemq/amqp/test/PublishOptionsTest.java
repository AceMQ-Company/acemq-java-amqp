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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.DefaultPublisher;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("publish options")
class PublishOptionsTest {

    private AceMq mq;

    @AfterEach
    void tearDown() {
        if (mq != null) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    private AceMq connect() {
        mq = AceMq.connect("memory://options-" + UUID.randomUUID());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @Nested
    @DisplayName("the defaults")
    class Defaults {

        @Test
        @DisplayName("are persistent and mandatory, with no expiry")
        void safeByDefault() {
            PublishOptions options = PublishOptions.defaults();

            assertThat(options.persistent()).isTrue();
            assertThat(options.mandatory()).isTrue();
            assertThat(options.expiration()).isEmpty();
        }

        @Test
        @DisplayName("report an unroutable message as a failure")
        void unroutableIsAFailureByDefault() {
            AceMq mq = connect();

            // Nothing is bound for this key. Silence here is how a routing-key typo loses every
            // message it touches while the logs stay clean.
            assertThatThrownBy(() -> mq.publisher("orders", "nothing.listens", String.class).send("one"))
                    .isInstanceOf(PublishFailedException.class);
        }
    }

    @Nested
    @DisplayName("allowUnroutable")
    class AllowUnroutable {

        @Test
        @DisplayName("accepts a message nothing is bound to receive")
        void unroutableIsAccepted() {
            AceMq mq = connect();

            mq.publisher("orders", "nothing.listens", String.class, PublishOptions.defaults().allowUnroutable())
                    .send("one");
        }

        @Test
        @DisplayName("does not stop a routable message from arriving")
        void routableStillArrives() throws Exception {
            AceMq mq = connect();
            CountDownLatch received = new CountDownLatch(1);

            try (var consumer = mq.consume("orders.new", String.class, message -> received.countDown())) {
                mq.publisher("orders", "order.created", String.class, PublishOptions.defaults().allowUnroutable())
                        .send("one");

                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Nested
    @DisplayName("expiry")
    class Expiry {

        @Test
        @DisplayName("a message that outlives its expiry is not delivered")
        void expiredMessagesAreDropped() throws Exception {
            AceMq mq = connect();

            mq.publisher("orders", "order.created", String.class,
                    PublishOptions.defaults().expiringAfter(Duration.ofMillis(100)))
                    .send("stale");

            // Nobody is consuming yet, so it sits in the queue and runs out of time there.
            Thread.sleep(400);

            CountDownLatch received = new CountDownLatch(1);
            try (var consumer = mq.consume("orders.new", String.class, message -> received.countDown())) {
                assertThat(received.await(500, TimeUnit.MILLISECONDS))
                        .as("a message past its expiry must not be delivered late")
                        .isFalse();
            }
        }

        @Test
        @DisplayName("a message consumed within its expiry arrives normally")
        void liveMessagesStillArrive() throws Exception {
            AceMq mq = connect();
            CountDownLatch received = new CountDownLatch(1);

            try (var consumer = mq.consume("orders.new", String.class, message -> received.countDown())) {
                mq.publisher("orders", "order.created", String.class,
                        PublishOptions.defaults().expiringAfter(Duration.ofSeconds(30)))
                        .send("fresh");

                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }
        }

        @Test
        @DisplayName("a non-positive expiry is refused")
        void refusesNonPositiveExpiry() {
            assertThatThrownBy(() -> PublishOptions.defaults().expiringAfter(Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expiration");
            assertThatThrownBy(() -> PublishOptions.defaults().expiringAfter(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("carrying options around")
    class Carrying {

        @Test
        @DisplayName("changing format keeps the options")
        void formatChangeKeepsOptions() {
            AceMq mq = connect();
            PublishOptions options = PublishOptions.transientDelivery()
                    .allowUnroutable()
                    .expiringAfter(Duration.ofMinutes(5));

            DefaultPublisher<String> xml = mq.publisher("orders", "order.created", String.class, options).asXml();

            // Asking for XML is not a request to start writing to disk again, and losing the
            // options here would do exactly that without saying so.
            assertThat(xml.options().persistent()).isFalse();
            assertThat(xml.options().mandatory()).isFalse();
            assertThat(xml.options().expiration()).contains(Duration.ofMinutes(5));
        }

        @Test
        @DisplayName("with() leaves the original publisher alone")
        void withDoesNotMutate() {
            AceMq mq = connect();
            DefaultPublisher<String> safe = mq.publisher("orders", "order.created", String.class);

            DefaultPublisher<String> loose = safe.with(PublishOptions.transientDelivery());

            assertThat(safe.options().persistent())
                    .as("a publisher another thread is using must not change underneath it")
                    .isTrue();
            assertThat(loose.options().persistent()).isFalse();
        }
    }
}
