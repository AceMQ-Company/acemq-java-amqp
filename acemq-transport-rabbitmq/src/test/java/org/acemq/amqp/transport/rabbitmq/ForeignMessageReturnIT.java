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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A message from a publisher that set no message id, parked or replayed somewhere that is gone.
 *
 * <p>Returns were matched to their publish by message id, so one without an id was reported as
 * routed and the delivery it was copied from acknowledged: the last copy deleted.
 */
class ForeignMessageReturnIT {

    private static RabbitMQContainer broker;

    @BeforeAll
    static void startBroker() {
        broker = new RabbitMQContainer(BrokerImage.current());
        broker.start();
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.stop();
        }
    }

    /** Publishes as a client that has never heard of AceMQ: no message id, no envelope. */
    private static void publishForeign(String queue, String body) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(broker.getAmqpUrl());
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().contentType("text/plain").build(),
                    body.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    @Timeout(60)
    void parking_one_where_the_parking_lot_is_gone_keeps_it() throws Exception {
        String queue = "foreign.park." + UUID.randomUUID();
        try (AceMq mq = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE)) {
            mq.declareQueue(queue, QueueType.CLASSIC, Map.of());
            try (MessageConsumer consumer = mq.consume(queue, Integer.class, ConsumerOptions.prefetch(1), m -> {
            })) {
                mq.deleteQueue(queue + ".parked");
                publishForeign(queue, "not a number");
                Thread.sleep(1_500);
                assertThat(consumer.acknowledged()).isZero();
            }
            await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount(queue) == 1L);
        }
    }

    @Test
    @Timeout(60)
    void replaying_one_into_a_queue_that_is_gone_keeps_it() throws Exception {
        String queue = "foreign.replay." + UUID.randomUUID();
        try (AceMq mq = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE)) {
            mq.declareQueue(queue + ".parked", QueueType.CLASSIC, Map.of());
            publishForeign(queue + ".parked", "parked by hand");
            await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount(queue + ".parked") == 1L);

            assertThatThrownBy(() -> mq.replay(queue).parked().replayAll()).isInstanceOf(AceMqException.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> mq.messageCount(queue + ".parked") == 1L);
        }
    }
}
