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
package org.acemq.amqp.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportConnection;
import org.acemq.amqp.transport.TransportException;
import org.acemq.amqp.transport.rabbitmq.RabbitMqTransport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A retry hop whose publish throws must settle only the message it was for.
 *
 * <p>The throw used to escape the consumer's delivery callback, and the RabbitMQ client answers
 * an exception there by closing the channel. The failed message was not lost, but every other
 * delivery in flight on that channel came back with it, and the consumer stopped.
 */
@DisplayName("a retry hop that throws")
class RetryHopFailureIT {

    private static final String QUEUE = "hop.failure.work";

    private static RabbitMQContainer broker;

    @BeforeAll
    static void startBroker() {
        // The same override BrokerImage reads, which is package-private to the other test package.
        String image = System.getProperty("acemq.test.rabbitmq.image", System.getenv("ACEMQ_RABBITMQ_IMAGE"));
        broker = new RabbitMQContainer(DockerImageName
                .parse(image == null || image.isBlank() ? "rabbitmq:4-management" : image)
                .asCompatibleSubstituteFor("rabbitmq"));
        broker.start();
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("sends back only its own message and leaves the channel open")
    void onlyTheFailedMessageComesBack() {
        TransportConnection real = new RabbitMqTransport().connect(ConnectionConfig.url(broker.getAmqpUrl()).build());
        AtomicBoolean hopFails = new AtomicBoolean(true);
        // Every call goes to the real connection, except the first republish onto the source
        // queue, which fails the way a lost publishing channel does.
        TransportConnection failing = (TransportConnection) Proxy.newProxyInstance(
                TransportConnection.class.getClassLoader(),
                new Class<?>[]{TransportConnection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("send")
                            && QUEUE.equals(((OutboundMessage) args[0]).routingKey())
                            && hopFails.compareAndSet(true, false)) {
                        throw new TransportException("the publishing channel is gone");
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        try {
            real.declareQueue(QUEUE, QueueType.CLASSIC, true, Collections.emptyMap());
            for (String body : List.of("a", "b", "poison", "d", "e")) {
                real.send(OutboundMessage.body(body.getBytes(StandardCharsets.UTF_8))
                        .exchange("")
                        .routingKey(QUEUE)
                        .messageId(body)
                        .build());
            }

            List<String> handled = new CopyOnWriteArrayList<>();
            AtomicBoolean poisonFails = new AtomicBoolean(true);
            DefaultConsumer<byte[]> consumer = new DefaultConsumer<>(
                    failing,
                    Codecs.byName("bytes"),
                    QUEUE,
                    byte[].class,
                    ConsumerOptions.prefetch(10).withRetry(RetryPolicy.fixed(3, Duration.ofMillis(1)).withJitter(0)),
                    message -> {
                        String body = new String(message.payload(), StandardCharsets.UTF_8);
                        handled.add(body);
                        if (body.equals("poison") && poisonFails.compareAndSet(true, false)) {
                            throw new IllegalStateException("downstream is down");
                        }
                    },
                    Telemetry.NONE);
            consumer.start();
            try {
                await().atMost(Duration.ofSeconds(30))
                        .until(() -> handled.size() >= 6 && real.messageCount(QUEUE) == 0);
                assertThat(hopFails).as("the hop publish was made to fail").isFalse();
                assertThat(handled)
                        .as("the failed message comes back once; the others are handled exactly once")
                        .containsExactlyInAnyOrder("a", "b", "poison", "poison", "d", "e");
                assertThat(consumer.isRunning()).as("the consumer's channel is still open").isTrue();
                assertThat(consumer.acknowledged()).isEqualTo(5);
            } finally {
                consumer.close();
            }
        } finally {
            real.close();
        }
    }
}
