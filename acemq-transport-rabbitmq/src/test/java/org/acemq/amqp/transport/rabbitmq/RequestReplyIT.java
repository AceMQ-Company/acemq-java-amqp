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
import java.util.Collections;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.Requester;
import org.acemq.amqp.core.Responder;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Request and reply over a real broker.
 *
 * <p>Worth its own integration test rather than trusting the in-memory one: reply-to is an AMQP
 * property rather than an AceMQ header, so the only thing that proves it survives the wire is the
 * wire.
 */
@Testcontainers
@DisplayName("request and reply against RabbitMQ")
class RequestReplyIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(BrokerImage.current());

    private AceMq mq;

    @BeforeEach
    void connect() {
        mq = AceMq.connect(BROKER.getAmqpUrl());
        mq.declareQueue("pricing", QueueType.CLASSIC, Collections.emptyMap());
    }

    @AfterEach
    void disconnect() {
        if (mq != null && mq.isOpen()) {
            mq.deleteQueue("pricing");
            mq.close();
        }
    }

    @Test
    @Timeout(120)
    void an_answer_travels_back_over_the_wire() {
        try (Responder responder = mq.respond("pricing", String.class,
                ConsumerOptions.prefetch(10),
                sku -> sku + ":42");
                Requester requester = mq.requester()) {

            String price = requester.request("", "pricing", "WIDGET", String.class, Duration.ofSeconds(30));

            assertThat(price).isEqualTo("WIDGET:42");
            assertThat(requester.timedOut()).isZero();

            // Awaited rather than asserted outright: the responder increments this after the
            // reply is published, and the caller can be back from request(...) before that
            // line runs. Locally it always won the race; on CI it did not, which is exactly
            // the kind of assertion that turns into an intermittent failure nobody trusts.
            await().atMost(Duration.ofSeconds(10)).until(() -> responder.answered() == 1);
        }
    }

    @Test
    @Timeout(120)
    void the_reply_queue_expires_rather_than_outliving_the_process() {
        String replyQueue;
        try (Requester requester = mq.requester()) {
            replyQueue = requester.replyQueue();
            assertThat(mq.messageCount(replyQueue)).isZero();
        }

        // Deleted on close. The x-expires argument is the belt to this braces: a process
        // killed mid-request leaves the queue behind, and without expiry a service that
        // restarts often accumulates thousands of them.
        assertThatThrownBy(() -> mq.messageCount(replyQueue))
                .as("the reply queue is gone, not merely empty")
                .isInstanceOf(RuntimeException.class);
    }
}
