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
import static org.assertj.core.api.Assertions.entry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.DeliveryListener;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The rung argument table, written out in full and printed.
 *
 * <p>This is the least clever test in the repository and that is the point of it. Every other
 * retry test asserts on behaviour, which means a change to the arguments a rung is declared with
 * can pass all of them and still break a deployment: two services consuming the same queue
 * declare the same rung by name, so if one of them declares it with a different time-to-live or
 * a different dead-letter target the second is refused with {@code PRECONDITION_FAILED} and
 * cannot consume at all. Nothing in this library can detect that; the operator finds out.
 *
 * <p>So the table is spelled out here — queue name, all three arguments, the exchange, and the
 * binding that brings an expired message home — and a change to any of it has to be a change to
 * this file, made on purpose. The same table is written out in the Go, .NET, Python and Ruby
 * libraries, and the point of printing it is that the five can be compared by eye.
 */
@DisplayName("the rung argument table")
class RungTableTest {

    private static final String SOURCE = "orders.new";

    /**
     * The policy the table is written for: 10s, 20s, 40s, 80s, 160s, of which the last three are
     * at or above the default thirty-second threshold.
     */
    private static final RetryPolicy POLICY = RetryPolicy.exponential(6, Duration.ofSeconds(10),
            Duration.ofMinutes(10));

    @Test
    void is_exactly_this() {
        RetryTopology topology = RetryTopology.forQueue(SOURCE, POLICY);

        // The short waits get no queue at all: the consumer holds those itself.
        assertThat(topology.rungQueues())
                .containsExactly("orders.new.retry.40s", "orders.new.retry.80s", "orders.new.retry.160s");

        assertThat(RetryTopology.rungArguments(SOURCE, Duration.ofSeconds(40)))
                .containsExactly(
                        entry("x-message-ttl", 40_000L),
                        entry("x-dead-letter-exchange", "acemq.retry"),
                        entry("x-dead-letter-routing-key", "orders.new"));
        assertThat(RetryTopology.rungArguments(SOURCE, Duration.ofSeconds(80)))
                .containsExactly(
                        entry("x-message-ttl", 80_000L),
                        entry("x-dead-letter-exchange", "acemq.retry"),
                        entry("x-dead-letter-routing-key", "orders.new"));
        assertThat(RetryTopology.rungArguments(SOURCE, Duration.ofSeconds(160)))
                .containsExactly(
                        entry("x-message-ttl", 160_000L),
                        entry("x-dead-letter-exchange", "acemq.retry"),
                        entry("x-dead-letter-routing-key", "orders.new"));

        // The two named exchanges and the queues reached through the dead-letter one.
        assertThat(RetryTopology.RETRY_EXCHANGE).isEqualTo("acemq.retry");
        assertThat(RetryTopology.DEAD_LETTER_EXCHANGE).isEqualTo("acemq.dlx");
        assertThat(topology.deadLetterQueue()).isEqualTo("orders.new.dlq");
        assertThat(topology.parkingLotQueue()).isEqualTo("orders.new.parked");
    }

    @Test
    void is_declared_exactly_as_it_is_written() {
        // Reading the table off the connection rather than off the class, so a declare() that
        // stopped using rungArguments() would be caught rather than quietly agreed with.
        RecordingConnection connection = new RecordingConnection();
        RetryTopology.forQueue(SOURCE, POLICY).declare(connection);

        assertThat(connection.exchanges).containsExactly("acemq.retry direct durable", "acemq.dlx direct durable");
        assertThat(connection.queues)
                .containsExactly(
                        "orders.new.retry.40s {x-message-ttl=40000, x-dead-letter-exchange=acemq.retry,"
                                + " x-dead-letter-routing-key=orders.new}",
                        "orders.new.retry.80s {x-message-ttl=80000, x-dead-letter-exchange=acemq.retry,"
                                + " x-dead-letter-routing-key=orders.new}",
                        "orders.new.retry.160s {x-message-ttl=160000, x-dead-letter-exchange=acemq.retry,"
                                + " x-dead-letter-routing-key=orders.new}",
                        "orders.new.dlq {}",
                        "orders.new.parked {}");
        assertThat(connection.bindings)
                .containsExactly(
                        // One binding brings every expired message back to the queue it came
                        // from, whichever rung it expired out of.
                        "orders.new -> acemq.retry -> orders.new",
                        "orders.new.dlq -> acemq.dlx -> orders.new.dlq",
                        "orders.new.parked -> acemq.dlx -> orders.new.parked");
    }

    @Test
    void a_policy_that_never_reaches_the_threshold_declares_no_rungs_and_no_binding() {
        RecordingConnection connection = new RecordingConnection();
        RetryTopology.forQueue(SOURCE, RetryPolicy.fixed(5, Duration.ofSeconds(2))).declare(connection);

        assertThat(connection.queues).containsExactly("orders.new.dlq {}", "orders.new.parked {}");
        assertThat(connection.bindings)
                .containsExactly("orders.new.dlq -> acemq.dlx -> orders.new.dlq",
                        "orders.new.parked -> acemq.dlx -> orders.new.parked");
    }

    /**
     * Prints the table, so the five libraries can be compared by eye.
     *
     * <p>Not an assertion. The assertions are above; this exists because reading three lines of
     * a table next to three lines of the Go one is how a disagreement is actually spotted, and
     * because a table nobody ever looks at is a table that drifts.
     */
    @Test
    void printed() {
        RetryTopology topology = RetryTopology.forQueue(SOURCE, POLICY);

        List<String> lines = new ArrayList<>();
        lines.add("source queue      : " + SOURCE);
        lines.add("policy            : " + POLICY);
        lines.add("schedule          : " + POLICY.schedule());
        lines.add("broker threshold  : " + POLICY.brokerWaitThreshold());
        lines.add("waits in consumer : " + belowThreshold());
        lines.add("rungs             : " + POLICY.brokerRungs());
        lines.add("");
        lines.add("retry exchange    : " + RetryTopology.RETRY_EXCHANGE + " (direct, durable)");
        lines.add("dead-letter exch. : " + RetryTopology.DEAD_LETTER_EXCHANGE + " (direct, durable)");
        lines.add("");
        for (Map.Entry<Duration, String> rung : topology.rungs().entrySet()) {
            Map<String, Object> arguments = RetryTopology.rungArguments(SOURCE, rung.getKey());
            lines.add("queue             : " + rung.getValue());
            lines.add("  x-message-ttl              : " + arguments.get("x-message-ttl"));
            lines.add("  x-dead-letter-exchange     : " + arguments.get("x-dead-letter-exchange"));
            lines.add("  x-dead-letter-routing-key  : " + arguments.get("x-dead-letter-routing-key"));
            lines.add("  binding                    : " + SOURCE + " -> " + RetryTopology.RETRY_EXCHANGE
                    + " -> " + SOURCE);
            lines.add("");
        }
        lines.add("queue             : " + topology.deadLetterQueue() + " (no arguments)");
        lines.add("  binding                    : " + topology.deadLetterQueue() + " -> "
                + RetryTopology.DEAD_LETTER_EXCHANGE + " -> " + topology.deadLetterQueue());
        lines.add("queue             : " + topology.parkingLotQueue() + " (no arguments)");
        lines.add("  binding                    : " + topology.parkingLotQueue() + " -> "
                + RetryTopology.DEAD_LETTER_EXCHANGE + " -> " + topology.parkingLotQueue());

        System.out.println("--- AceMQ rung table -------------------------------------------------");
        lines.forEach(System.out::println);
        System.out.println("----------------------------------------------------------------------");
    }

    private static List<Duration> belowThreshold() {
        List<Duration> waits = new ArrayList<>();
        for (Duration delay : POLICY.schedule()) {
            if (!POLICY.waitsInBroker(delay) && !waits.contains(delay)) {
                waits.add(delay);
            }
        }
        return waits;
    }

    /**
     * A connection that writes down what it was asked to declare and does nothing else.
     *
     * <p>Deliberately not the in-memory broker. That is a working broker, so a test using it
     * would assert on the effect of the declarations rather than on the declarations themselves,
     * and the effect is exactly what a second service with different arguments would agree with
     * right up to the moment it was refused.
     */
    private static final class RecordingConnection implements TransportConnection {

        private final List<String> exchanges = new ArrayList<>();
        private final List<String> queues = new ArrayList<>();
        private final List<String> bindings = new ArrayList<>();

        @Override
        public void declareExchange(String name, String type, boolean durable) {
            exchanges.add(name + " " + type + (durable ? " durable" : " transient"));
        }

        @Override
        public void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments) {
            StringBuilder rendered = new StringBuilder(name).append(" {");
            String separator = "";
            for (Map.Entry<String, Object> argument : arguments.entrySet()) {
                rendered.append(separator).append(argument.getKey()).append('=').append(argument.getValue());
                separator = ", ";
            }
            queues.add(rendered.append('}').toString());
        }

        @Override
        public void bindQueue(String queue, String exchange, String routingKey) {
            bindings.add(queue + " -> " + exchange + " -> " + routingKey);
        }

        @Override
        public ConfirmResult send(OutboundMessage message) {
            throw new UnsupportedOperationException("this connection only records declarations");
        }

        @Override
        public Subscription subscribe(String queue, int prefetch, DeliveryListener listener) {
            throw new UnsupportedOperationException("this connection only records declarations");
        }

        @Override
        public void deleteQueue(String name) {
            throw new UnsupportedOperationException("this connection only records declarations");
        }

        @Override
        public boolean queueExists(String name) {
            return queues.stream().anyMatch(declared -> declared.startsWith(name + " {"));
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
            // Nothing was opened.
        }
    }
}
