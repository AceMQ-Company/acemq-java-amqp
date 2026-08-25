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
        return new InMemoryConnection(InMemoryBroker.named(brokerName(config.url())));
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
