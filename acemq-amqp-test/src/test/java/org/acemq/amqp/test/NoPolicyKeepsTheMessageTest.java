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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A consumer with no retry policy must keep a message it could not handle.
 *
 * <p>This is the default path — {@link ConsumerOptions#defaults()}, no {@code withRetry} — and it
 * used to delete messages. With no policy there was no retry dispatcher, so a failed handler was
 * rejected without requeue and an undecodable body was too. A rejected message goes to the
 * broker's dead-letter exchange if the <em>queue</em> carries one, and nothing in this library
 * declares that on a queue it did not create: so out of the box, a handler that threw once
 * destroyed the message, silently, with {@code rejected} as the only trace.
 *
 * <p>Go, .NET, Python and Ruby all publish it to {@code {queue}.dlq} instead, and all four declare
 * that queue from the consumer so the publish cannot be unroutable. Java was the outlier, and the
 * divergence was invisible because no test asked what happens to a message when nobody configured
 * anything — every retry test configured a policy first.
 *
 * <p>What a policy still changes is how many attempts happen before the dead-letter queue. With
 * none there is one attempt, which is {@link org.acemq.amqp.api.RetryPolicy#none()} and matches
 * Go's {@code NoRetry()}.
 *
 * <p>These assertions also cover the declaration, without asserting it directly. A message set
 * aside is published to a queue <em>by name</em> and mandatory, so the in-memory broker reports it
 * unroutable if nothing declared the target — the message would then be requeued and the depth
 * asserted below would stay at zero. A dead-letter queue that arrives at depth one is a
 * dead-letter queue the consumer declared for itself.
 */
@DisplayName("a consumer with no retry policy")
class NoPolicyKeepsTheMessageTest {

    private AceMq mq;

    private AceMq connect(String brokerName) {
        mq = AceMq.connect("memory://" + brokerName);
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @AfterEach
    void disconnect() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Test
    @Timeout(30)
    @DisplayName("sends a message its handler could not process to the dead-letter queue")
    void a_failed_handler_dead_letters_rather_than_discarding() {
        connect("no-policy-dead-letter");
        AtomicInteger attempts = new AtomicInteger();

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.defaults(), message -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("downstream is unreachable");
                })) {

            mq.publisher("orders", "order.placed").send("payload");

            await().atMost(Duration.ofSeconds(10)).until(() -> consumer.deadLettered() == 1);

            assertThat(mq.messageCount("orders.new.dlq"))
                    .as("the message its handler could not process was thrown away: with no policy "
                            + "the consumer rejected it without requeue, and a queue with no "
                            + "dead-letter exchange of its own means the broker dropped it")
                    .isEqualTo(1);

            // One attempt, because there is no second one to have. A message that fails once with
            // nothing configured is set aside rather than retried, which is what a policy is for.
            assertThat(attempts).hasValue(1);
            assertThat(consumer.retried()).isZero();
            assertThat(consumer.acknowledged()).isZero();
        }
    }

    @Test
    @Timeout(30)
    @DisplayName("parks a body it could not decode instead of dropping it")
    void an_undecodable_body_is_parked_rather_than_discarded() {
        connect("no-policy-park");

        // A consumer expecting an Integer, given something that is not one. The bytes are the
        // evidence, and they are the one thing a decode failure leaves behind.
        try (MessageConsumer consumer = mq.consume(
                "orders.new", Integer.class, ConsumerOptions.defaults(), message -> {
                    throw new AssertionError("nothing should decode");
                })) {

            mq.publisher("orders", "order.placed").send("not-a-number");

            await().atMost(Duration.ofSeconds(10))
                    .until(() -> mq.messageCount("orders.new.parked") == 1);

            assertThat(mq.messageCount("orders.new.parked"))
                    .as("the bytes nobody could read were also the bytes nobody could look at")
                    .isEqualTo(1);
        }
    }

    @Test
    @Timeout(30)
    @DisplayName("still requeues when asked to, and then does not dead-letter")
    void requeue_on_failure_is_still_honoured() {
        connect("no-policy-requeue");
        AtomicInteger attempts = new AtomicInteger();

        // requeueOnFailure is the one case where putting the message straight back is what was
        // wanted -- shedding load onto another consumer -- so it has to survive a change whose
        // point is that an unconfigured failure stops being a discard.
        try (MessageConsumer consumer = mq.consume(
                "orders.new",
                String.class,
                ConsumerOptions.defaults().requeueOnFailure(),
                message -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("try somebody else");
                })) {

            mq.publisher("orders", "order.placed").send("payload");

            // Requeued, so it comes back. More than one delivery is the whole point of the option.
            await().atMost(Duration.ofSeconds(10)).until(() -> attempts.get() >= 2);

            assertThat(consumer.deadLettered())
                    .as("a requeued message has not been given up on, so it must not also be "
                            + "counted as dead-lettered")
                    .isZero();
            assertThat(mq.messageCount("orders.new.dlq")).isZero();
        }
    }

}
