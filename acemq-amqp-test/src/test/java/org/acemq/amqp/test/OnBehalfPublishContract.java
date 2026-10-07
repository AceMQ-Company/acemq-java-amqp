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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.Pipeline;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.core.Responder;
import org.acemq.amqp.patterns.OutboxRelay;
import org.acemq.amqp.patterns.Scheduler;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every place the library publishes on a caller's behalf, on a connection made
 * {@code withoutPublisherConfirms()}, sending somewhere that does not exist.
 *
 * <p>Each of these settles something on the answer to its publish: it acknowledges the message
 * it came from, or marks a record done. Without a confirm the answer was always "routed", so
 * each one deleted the last copy of a message that went nowhere. They are confirmed whatever
 * the connection was made with, and every test here fails if one is not. Run against the
 * in-memory broker and against RabbitMQ.
 */
abstract class OnBehalfPublishContract {

    /** @return a connection made without publisher confirms */
    abstract AceMq connect();

    private static String unique(String name) {
        return name + "." + UUID.randomUUID();
    }

    private static void failing(Object ignored) {
        throw new IllegalStateException("handler fails");
    }

    @Test
    @Timeout(60)
    void a_move_to_the_dead_letter_queue_that_goes_nowhere_keeps_the_message() {
        String queue = unique("onbehalf.dlq");
        try (AceMq mq = connect()) {
            mq.declareQueue(queue, QueueType.CLASSIC, Map.of());
            AtomicInteger attempts = new AtomicInteger();
            try (MessageConsumer consumer = mq.consume(queue, String.class, ConsumerOptions.prefetch(1), message -> {
                attempts.incrementAndGet();
                failing(message);
            })) {
                mq.deleteQueue(queue + ".dlq");
                mq.publisher("", queue, String.class).send("keep me");
                // Not acknowledged, so it comes back; before, it was acknowledged once and gone.
                await().atMost(Duration.ofSeconds(20)).until(() -> attempts.get() >= 2);
                assertThat(consumer.deadLettered()).isZero();
            }
        }
    }

    @Test
    @Timeout(60)
    void a_retry_hop_to_a_rung_that_goes_nowhere_keeps_the_message() {
        String queue = unique("onbehalf.rung");
        try (AceMq mq = connect()) {
            mq.declareQueue(queue, QueueType.CLASSIC, Map.of());
            AtomicInteger attempts = new AtomicInteger();
            RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(300)).withJitter(0)
                    .waitInBrokerFrom(Duration.ofMillis(10));
            try (MessageConsumer consumer = mq.consume(
                    queue, String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                        attempts.incrementAndGet();
                        failing(message);
                    })) {
                mq.deleteQueue(queue + ".retry.300ms");
                mq.publisher("", queue, String.class).send("keep me");
                await().atMost(Duration.ofSeconds(20)).until(() -> attempts.get() >= 2);
                assertThat(consumer.retried()).isZero();
            }
        }
    }

    @Test
    @Timeout(60)
    void parking_a_message_that_will_not_decode_where_the_parking_lot_is_gone_keeps_it() throws Exception {
        String queue = unique("onbehalf.parked");
        try (AceMq mq = connect()) {
            mq.declareQueue(queue, QueueType.CLASSIC, Map.of());
            try (MessageConsumer consumer = mq.consume(queue, Integer.class, ConsumerOptions.prefetch(1), message -> {
            })) {
                mq.deleteQueue(queue + ".parked");
                mq.publisher("", queue, String.class).asText().send("not a number");
                // Not acknowledged, so it goes round; before, it was acknowledged and gone.
                Thread.sleep(1_500);
                assertThat(consumer.acknowledged()).isZero();
            }
            await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount(queue) == 1L);
        }
    }

    @Test
    @Timeout(60)
    void a_replay_into_a_queue_that_is_gone_keeps_the_message() {
        String queue = unique("onbehalf.replay");
        try (AceMq mq = connect()) {
            mq.declareQueue(queue + ".dlq", QueueType.CLASSIC, Map.of());
            mq.publisher("", queue + ".dlq", String.class).send("keep me");
            await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount(queue + ".dlq") == 1L);

            assertThatThrownBy(() -> mq.replay(queue).replayAll()).isInstanceOf(AceMqException.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount(queue + ".dlq") == 1L);
        }
    }

    @Test
    @Timeout(60)
    void a_pipeline_hop_to_a_step_that_is_gone_keeps_the_message() {
        String name = unique("onbehalf.pipeline");
        AtomicInteger first = new AtomicInteger();
        try (AceMq mq = connect();
                Pipeline<String> pipeline = mq.pipeline(name, String.class)
                        .step("first", String.class, message -> {
                            first.incrementAndGet();
                            return message.payload();
                        })
                        .step("second", String.class, message -> null)
                        .build()) {
            mq.deleteQueue(name + ".second");
            pipeline.send("keep me");
            // The first step's consumer is told the hop failed, so the message goes round
            // again or to that step's dead-letter queue, rather than being acknowledged.
            await().atMost(Duration.ofSeconds(20))
                    .until(() -> first.get() >= 2 || mq.messageCount(name + ".first.dlq") == 1L);
        }
    }

    @Test
    @Timeout(60)
    void a_reply_to_a_queue_that_is_gone_keeps_the_request() {
        String queue = unique("onbehalf.respond");
        AtomicInteger asked = new AtomicInteger();
        try (AceMq mq = connect()) {
            mq.declareQueue(queue, QueueType.CLASSIC, Map.of());
            try (Responder responder = mq.respond(queue, String.class, request -> {
                asked.incrementAndGet();
                return request;
            })) {
                mq.publisher("", queue, String.class).replyingTo(unique("nobody")).send("question");
                await().atMost(Duration.ofSeconds(20))
                        .until(() -> asked.get() >= 2 || mq.messageCount(queue + ".dlq") == 1L);
            }
        }
    }

    @Test
    @Timeout(60)
    void an_outbox_record_whose_publish_goes_nowhere_is_not_marked_published() {
        RecordingOutboxStore store = new RecordingOutboxStore();
        try (AceMq mq = connect(); OutboxRelay relay = new OutboxRelay(mq, store)) {
            store.add(OutboxRecord.of("", unique("nowhere"), Envelope.of("order.placed").build(), "{}"));
            relay.drainOnce();
            assertThat(store.published()).isEmpty();
            assertThat(store.pendingCount()).isEqualTo(1L);
        }
    }

    @Test
    @Timeout(60)
    void a_scheduled_delivery_that_goes_nowhere_is_kept() throws Exception {
        try (AceMq mq = connect(); Scheduler scheduler = Scheduler.on(mq)) {
            long before = mq.messageCount("acemq.schedule.due.dlq");
            // Through a rung and back to the scheduler's own consumer, which then delivers.
            scheduler.in(Duration.ofSeconds(2), "", unique("nowhere"), "later");
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> mq.messageCount("acemq.schedule.due.dlq") == before + 1);
        }
    }

    @Test
    @Timeout(60)
    void a_callers_own_publisher_keeps_the_mode_it_chose() {
        try (AceMq mq = connect()) {
            // No confirm, so nothing to say it went nowhere: the caller asked for that.
            mq.publisher("", unique("nowhere"), String.class).send("fire and forget");
            assertThatThrownBy(() -> mq.publisher("", unique("nowhere"), String.class,
                    PublishOptions.defaults().alwaysConfirmed()).send("checked"))
                    .isInstanceOf(PublishFailedException.class);
        }
    }
}
