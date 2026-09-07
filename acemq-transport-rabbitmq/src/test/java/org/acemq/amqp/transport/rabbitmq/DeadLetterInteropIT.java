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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.core.AceMq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * The dead-letter topology as another language would find it.
 *
 * <p>The claim being tested is an interoperability one and cannot be checked by reading Java.
 * Two services in different languages consuming the same queue both declare it, and AMQP
 * compares the argument tables: one argument out of place answers the second service
 * {@code PRECONDITION_FAILED} and leaves it unable to consume at all. So the topology is
 * declared here through {@link Topology}, and then the same queue is declared again over a raw
 * second connection with the argument table the Python, Ruby and .NET libraries send. The
 * broker is the judge.
 *
 * <p>Both halves matter. That the matching declaration is accepted proves the tables agree;
 * that a different one is refused proves the broker was actually comparing them, rather than
 * being lenient about arguments in a way that would make the first half prove nothing.
 */
@Testcontainers
@DisplayName("dead-letter topology, as another language would declare it")
class DeadLetterInteropIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(BrokerImage.current());

    /** Namespaced so a run against a shared broker leaves nothing behind that another test uses. */
    private static final String SOURCE = "interop.orders";

    private AceMq mq;

    @BeforeEach
    void connect() {
        mq = AceMq.connect(BROKER.getAmqpUrl());
    }

    @AfterEach
    void disconnect() {
        if (mq != null) {
            for (String queue : new String[]{SOURCE, SOURCE + ".dlq", SOURCE + ".parked"}) {
                try {
                    mq.deleteQueue(queue);
                } catch (RuntimeException e) {
                    // A test may never have created it; the connection still has to close.
                }
            }
            mq.close();
        }
    }

    /**
     * The argument table the other four libraries put on a source queue.
     *
     * <p>Written out literally rather than read from {@link Topology}, because a constant shared
     * with the code under test would agree with it by construction and prove nothing. These are
     * the strings on the wire.
     */
    private static Map<String, Object> asAnotherLanguageDeclaresIt() {
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put("x-dead-letter-exchange", "acemq.dlx");
        arguments.put("x-dead-letter-routing-key", SOURCE + ".dlq");
        return arguments;
    }

    private static Topology topology() {
        // Classic rather than quorum, because the other four libraries default a source queue to
        // classic and x-queue-type is compared as strictly as anything else.
        return Topology.define().classicQueueWithDeadLetter(SOURCE, Collections.emptyMap()).build();
    }

    @Test
    @Timeout(120)
    @DisplayName("a second service declaring the same queue the way Python does is accepted")
    void anotherLanguageCanDeclareTheSameQueue() throws Exception {
        mq.topology().apply(topology(), ApplyMode.CREATE_ONLY);

        // A connection of its own, so this is the same thing a second service would do rather
        // than a redeclaration on a channel that already holds the queue.
        try (Connection second = secondService(); Channel channel = second.createChannel()) {
            channel.queueDeclare(SOURCE, true, false, false, asAnotherLanguageDeclaresIt());
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a different argument table is refused, so the agreement above is real")
    void aDifferentArgumentTableIsRefused() throws Exception {
        mq.topology().apply(topology(), ApplyMode.CREATE_ONLY);

        Map<String, Object> different = asAnotherLanguageDeclaresIt();
        different.put("x-dead-letter-routing-key", SOURCE + ".dead");

        try (Connection second = secondService()) {
            assertThatThrownBy(() -> {
                try (Channel channel = second.createChannel()) {
                    channel.queueDeclare(SOURCE, true, false, false, different);
                }
            })
                    .isInstanceOfAny(java.io.IOException.class, ShutdownSignalException.class)
                    .hasStackTraceContaining("PRECONDITION_FAILED")
                    .hasStackTraceContaining("x-dead-letter-routing-key");
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a queue declared without the arguments cannot be redeclared with them")
    void theUpgradeIsBreakingAndSaysSo() throws Exception {
        // The migration note in the CHANGELOG and the README says an existing queue has to be
        // drained and recreated. This is that claim, checked rather than asserted.
        try (Connection first = secondService(); Channel channel = first.createChannel()) {
            channel.queueDeclare(SOURCE, true, false, false, Collections.emptyMap());
        }

        assertThatThrownBy(() -> mq.topology().apply(topology(), ApplyMode.CREATE_ONLY))
                .hasStackTraceContaining("x-dead-letter-exchange");
    }

    @Test
    @Timeout(120)
    @DisplayName("the whole topology, printed so it can be compared with the other four by eye")
    void printsTheTopologyItDeclares() {
        Topology topology = topology();
        mq.topology().apply(topology, ApplyMode.CREATE_ONLY);

        List<String> lines = new ArrayList<>();
        lines.add("exchanges");
        for (Topology.ExchangeSpec exchange : topology.exchanges()) {
            lines.add("  " + exchange.name() + " (" + exchange.type() + ", durable=" + exchange.durable() + ")");
        }
        lines.add("queues");
        for (Topology.QueueSpec queue : topology.queues()) {
            lines.add("  " + queue.name() + " (" + (queue.quorum() ? "quorum" : "classic") + ", durable="
                    + queue.durable() + ")");
            if (queue.arguments().isEmpty()) {
                lines.add("      no arguments");
            }
            for (Map.Entry<String, Object> argument : queue.arguments().entrySet()) {
                lines.add("      " + argument.getKey() + " = " + argument.getValue());
            }
        }
        lines.add("bindings");
        for (Topology.BindingSpec binding : topology.bindings()) {
            lines.add("  " + binding.exchange() + " -> " + binding.queue() + " [" + binding.routingKey() + "]");
        }
        System.out.println(String.join(System.lineSeparator(), lines));

        // Printed for a human, asserted for the build: a print nobody checks is a print that
        // quietly stops printing the interesting part.
        assertThat(lines).contains(
                "  acemq.dlx (direct, durable=true)",
                "  interop.orders (classic, durable=true)",
                "      x-dead-letter-exchange = acemq.dlx",
                "      x-dead-letter-routing-key = interop.orders.dlq",
                "  interop.orders.dlq (classic, durable=true)",
                "  interop.orders.parked (classic, durable=true)",
                "  acemq.dlx -> interop.orders.dlq [interop.orders.dlq]",
                "  acemq.dlx -> interop.orders.parked [interop.orders.parked]");
    }

    private static Connection secondService() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(BROKER.getAmqpUrl());
        return factory.newConnection("acemq-interop-second-service");
    }
}
