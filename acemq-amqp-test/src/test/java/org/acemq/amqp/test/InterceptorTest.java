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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.api.Ack;
import org.acemq.amqp.api.ConsumeContext;
import org.acemq.amqp.api.ConsumeInterceptor;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.PublishContext;
import org.acemq.amqp.api.PublishInterceptor;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("interceptors")
class InterceptorTest {

    private AceMq mq;

    @BeforeEach
    void setUp() {
        mq = AceMq.connect("memory://interceptors-" + UUID.randomUUID());
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

    /** Consumes one message and returns what the handler saw. */
    private Envelope consumeOne() throws Exception {
        AtomicReference<Envelope> seen = new AtomicReference<>();
        CountDownLatch received = new CountDownLatch(1);
        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
            seen.compareAndSet(null, message.envelope());
            received.countDown();
        })) {
            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        }
        return seen.get();
    }

    @Nested
    @DisplayName("on publish")
    class OnPublish {

        @Test
        @DisplayName("a header added by an interceptor reaches the consumer")
        void addedHeadersArrive() throws Exception {
            mq.intercept((PublishInterceptor) context -> context.withEnvelope(
                    context.envelope().toBuilder().header("tenant", "acme").build()));

            mq.publisher("orders", "order.created", String.class).send("one");

            // The whole point: an interceptor that cannot change what is actually sent is a
            // logging hook with extra steps.
            assertThat(consumeOne().headers()).containsEntry("tenant", "acme");
        }

        @Test
        @DisplayName("runs before encoding, so it sees the payload as an object")
        void seesTheApplicationObject() {
            AtomicReference<Object> seen = new AtomicReference<>();
            mq.intercept((PublishInterceptor) context -> {
                seen.set(context.payload());
                return context;
            });

            mq.publisher("orders", "order.created", String.class).send("an-order");

            assertThat(seen.get()).isEqualTo("an-order");
        }

        @Test
        @DisplayName("throwing refuses the publish, and nothing is sent")
        void throwingRefusesThePublish() throws Exception {
            mq.intercept((PublishInterceptor) context -> {
                throw new IllegalStateException("no tenant on this thread");
            });

            assertThatThrownBy(() -> mq.publisher("orders", "order.created", String.class).send("one"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("no tenant on this thread");

            // Asserting the absence: a refusal that still delivered would be worse than no
            // interceptor at all, because it would look enforced.
            CountDownLatch received = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, m -> received.countDown())) {
                assertThat(received.await(400, TimeUnit.MILLISECONDS)).isFalse();
            }
        }

        @Test
        @DisplayName("afterConfirm is told what the broker said")
        void afterConfirmSeesTheResult() {
            AtomicReference<PublishResult> seen = new AtomicReference<>();
            mq.intercept(new PublishInterceptor() {
                @Override
                public PublishContext beforePublish(PublishContext context) {
                    return context;
                }

                @Override
                public void afterConfirm(PublishContext context, PublishResult result) {
                    seen.set(result);
                }
            });

            PublishResult result = mq.publisher("orders", "order.created", String.class).send("one");

            assertThat(seen.get()).isNotNull();
            assertThat(seen.get().messageId()).isEqualTo(result.messageId());
        }

        @Test
        @DisplayName("onError is told when a publish fails")
        void onErrorSeesFailures() {
            List<String> failures = new CopyOnWriteArrayList<>();
            mq.intercept(new PublishInterceptor() {
                @Override
                public PublishContext beforePublish(PublishContext context) {
                    return context;
                }

                @Override
                public void onError(PublishContext context, Throwable failure) {
                    failures.add(failure.getClass().getSimpleName());
                }
            });

            // Nothing is bound for this key, so the publish fails as unroutable.
            assertThatThrownBy(() -> mq.publisher("orders", "nothing.listens", String.class).send("one"))
                    .isInstanceOf(RuntimeException.class);

            assertThat(failures).containsExactly("PublishFailedException");
        }

        @Test
        @DisplayName("a failure in afterConfirm does not fail the publish")
        void afterConfirmFailuresAreContained() {
            mq.intercept(new PublishInterceptor() {
                @Override
                public PublishContext beforePublish(PublishContext context) {
                    return context;
                }

                @Override
                public void afterConfirm(PublishContext context, PublishResult result) {
                    throw new IllegalStateException("the metrics backend is down");
                }
            });

            // The message is already at the broker. Reporting it as failed would be a lie that
            // makes callers resend something that arrived.
            mq.publisher("orders", "order.created", String.class).send("one");
        }

        @Test
        @DisplayName("interceptors run in order, each seeing the one before")
        void interceptorsChainInOrder() throws Exception {
            mq.intercept(new OrderedStamp("second", 20));
            mq.intercept(new OrderedStamp("first", 10));

            mq.publisher("orders", "order.created", String.class).send("one");

            // Registered out of sequence on purpose: order() decides, not registration.
            assertThat(consumeOne().headers()).containsEntry("stamps", "first,second");
        }

        @Test
        @DisplayName("applies to publishers created before it was registered")
        void appliesToExistingPublishers() throws Exception {
            var publisher = mq.publisher("orders", "order.created", String.class);

            mq.intercept((PublishInterceptor) context -> context.withEnvelope(
                    context.envelope().toBuilder().header("late", "yes").build()));
            publisher.send("one");

            assertThat(consumeOne().headers()).containsEntry("late", "yes");
        }
    }

    @Nested
    @DisplayName("on consume")
    class OnConsume {

        @Test
        @DisplayName("wraps the handler, before and after")
        void wrapsTheHandler() throws Exception {
            List<String> events = new CopyOnWriteArrayList<>();
            mq.intercept(new RecordingConsumeInterceptor(events, "outer", 10));

            CountDownLatch received = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
                events.add("handler");
                received.countDown();
            })) {
                mq.publisher("orders", "order.created", String.class).send("one");
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }

            assertThat(events).containsExactly("outer.before", "handler", "outer.after");
        }

        @Test
        @DisplayName("teardown runs in reverse, so nested scopes close inside out")
        void teardownIsReversed() throws Exception {
            List<String> events = new CopyOnWriteArrayList<>();
            mq.intercept(new RecordingConsumeInterceptor(events, "outer", 10));
            mq.intercept(new RecordingConsumeInterceptor(events, "inner", 20));

            CountDownLatch received = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
                events.add("handler");
                received.countDown();
            })) {
                mq.publisher("orders", "order.created", String.class).send("one");
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            }

            // Reversed on the way out. Same order both ways would have the outer scope closing
            // while the inner one was still inside it.
            assertThat(events).containsExactly(
                    "outer.before", "inner.before", "handler", "inner.after", "outer.after");
        }

        @Test
        @DisplayName("a handler failure reaches onError, and teardown still runs")
        void failuresReachOnErrorAndStillTearDown() throws Exception {
            List<String> events = new CopyOnWriteArrayList<>();
            mq.intercept(new RecordingConsumeInterceptor(events, "outer", 10));

            CountDownLatch attempted = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
                attempted.countDown();
                throw new IllegalStateException("downstream is down");
            })) {
                mq.publisher("orders", "order.created", String.class).send("one");
                assertThat(attempted.await(5, TimeUnit.SECONDS)).isTrue();
                Thread.sleep(200);
            }

            // Teardown on the failure path matters most: an interceptor that only cleans up
            // after success leaks its state on exactly the messages worth investigating.
            assertThat(events).contains("outer.before", "outer.error", "outer.after");
        }

        @Test
        @DisplayName("an interceptor that refuses a message stops the handler running")
        void refusingStopsTheHandler() throws Exception {
            mq.intercept(new ConsumeInterceptor() {
                @Override
                public void beforeHandle(ConsumeContext context) {
                    throw new IllegalStateException("this tenant is not allowed here");
                }
            });

            List<String> handled = new CopyOnWriteArrayList<>();
            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> handled.add(message.payload()))) {
                mq.publisher("orders", "order.created", String.class).send("one");
                Thread.sleep(500);

                assertThat(handled)
                        .as("a refused message must not reach the handler")
                        .isEmpty();
                assertThat(consumer.rejected())
                        .as("and must be rejected rather than quietly acknowledged")
                        .isEqualTo(1L);
            }
        }
    }

    /** Appends its name to a comma-separated header, so ordering is visible in the result. */
    private static final class OrderedStamp implements PublishInterceptor {

        private final String name;
        private final int order;

        OrderedStamp(String name, int order) {
            this.name = name;
            this.order = order;
        }

        @Override
        public PublishContext beforePublish(PublishContext context) {
            Object existing = context.envelope().headers().get("stamps");
            String stamps = existing == null ? name : existing + "," + name;
            return context.withEnvelope(context.envelope().toBuilder().header("stamps", stamps).build());
        }

        @Override
        public int order() {
            return order;
        }
    }

    private static final class RecordingConsumeInterceptor implements ConsumeInterceptor {

        private final List<String> events;
        private final String name;
        private final int order;

        RecordingConsumeInterceptor(List<String> events, String name, int order) {
            this.events = events;
            this.name = name;
            this.order = order;
        }

        @Override
        public void beforeHandle(ConsumeContext context) {
            events.add(name + ".before");
        }

        @Override
        public void afterHandle(ConsumeContext context, Ack ack) {
            events.add(name + ".after");
        }

        @Override
        public void onError(ConsumeContext context, Throwable failure) {
            events.add(name + ".error");
        }

        @Override
        public int order() {
            return order;
        }
    }
}
