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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.Transport;
import org.acemq.amqp.transport.TransportConnection;

/**
 * A transport that runs entirely in the process, for tests that should not need Docker.
 *
 * <p>Connect with a {@code memory://} URL, where the host names the broker instance:
 *
 * <pre>{@code
 * try (AceMq mq = AceMq.connect("memory://orders")) {
 *     mq.declareExchange("orders", "topic");
 *     mq.declareQueue("orders.new");
 *     mq.bind("orders.new", "orders", "order.*");
 * }
 * }</pre>
 *
 * <p>Two connections to the same name share state; a different name is a different broker. A
 * test wanting isolation should use a unique name rather than resetting shared state.
 *
 * <p>This is a fake, not a simulator, and it is honest about the difference. It implements
 * routing, prefetch and settlement, because those are what most tests actually exercise. It
 * does not implement replication, persistence, delayed delivery or dead-lettering, and does
 * not claim those capabilities, so code that depends on them fails here for the same reason it
 * would fail against a broker that lacks them. The conformance suite will run against both
 * this and real brokers, which is what keeps the two from drifting apart.
 */
public final class InMemoryTransport implements Transport {

    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(
            Capability.EXCHANGE_ROUTING,
            Capability.TOPIC_WILDCARDS,
            Capability.PUBLISHER_CONFIRMS,
            // Queue-level time-to-live with a dead-letter target is implemented, which is what
            // the retry ladder is built from. Claimed only because it genuinely works here.
            Capability.DEAD_LETTER_NATIVE));

    /** Discards every in-memory broker and its contents. */
    public static void reset() {
        InMemoryBroker.reset();
    }

    /**
     * Makes a named broker refuse publishes, the way a real one does under a memory or disk alarm.
     *
     * <p>Exists because this failure is worth testing and is otherwise almost impossible to
     * reproduce: publishing to a blocked broker does not fail, it simply never returns, and a
     * service that has not been tested against it will hang in production instead of here.
     *
     * <pre>{@code
     * InMemoryTransport.block("orders", "low on memory");
     * assertThrows(ConnectionBlockedException.class, () -> publisher.send(order));
     * InMemoryTransport.unblock("orders");
     * }</pre>
     *
     * <p>Publishers wait up to {@code ConnectionConfig.blockedTimeout()} before throwing, so a
     * test that expects the exception should either shorten that timeout or not unblock.
     *
     * <p>One deliberate difference from RabbitMQ, worth knowing before writing a test around it:
     * here {@code isBlocked()} becomes true as soon as this is called, whereas RabbitMQ tells a
     * connection it is blocked only when that connection next publishes. Against a real broker the
     * first message into an alarm is already on the wire before anything is known, so it may or
     * may not arrive; here nothing is ever written, and
     * {@code ConnectionBlockedException.mayHaveBeenPublished()} is always {@code false}. A test
     * that needs the uncertain case needs a real broker.
     *
     * @param brokerName the host part of the {@code memory://} URL
     * @param reason what to report as the broker's own explanation, such as {@code low on memory}
     */
    public static void block(String brokerName, String reason) {
        InMemoryBroker.named(brokerName).block(reason);
    }

    /**
     * Makes a named broker accept publishes again, releasing every publisher waiting on it.
     *
     * @param brokerName the host part of the {@code memory://} URL
     */
    public static void unblock(String brokerName) {
        InMemoryBroker.named(brokerName).unblock();
    }

    @Override
    public String scheme() {
        return "memory";
    }

    @Override
    public String name() {
        return "in-memory";
    }

    @Override
    public Set<Capability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public TransportConnection connect(ConnectionConfig config) {
        return new InMemoryConnection(InMemoryBroker.named(brokerName(config.url())), config.blockedTimeout());
    }

    /** Extracts the broker name from {@code memory://name}, defaulting to {@code default}. */
    private static String brokerName(String url) {
        int start = url.indexOf("://");
        if (start < 0) {
            return "default";
        }
        String remainder = url.substring(start + 3);
        int end = remainder.indexOf('/');
        String name = end < 0 ? remainder : remainder.substring(0, end);
        return name.isEmpty() ? "default" : name;
    }
}
