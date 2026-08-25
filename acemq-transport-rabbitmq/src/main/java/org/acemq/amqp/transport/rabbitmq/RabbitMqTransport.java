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

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.Transport;
import org.acemq.amqp.transport.TransportConnection;
import org.acemq.amqp.transport.TransportException;

import com.rabbitmq.client.ConnectionFactory;

/**
 * AMQP 0-9-1 transport backed by the RabbitMQ Java client.
 *
 * <p>Discovered through {@link java.util.ServiceLoader}, so putting this module on the
 * classpath is all it takes for {@code amqp://} URLs to work.
 */
public final class RabbitMqTransport implements Transport {

    /**
     * What a RabbitMQ broker can do.
     *
     * <p>Two absences are deliberate. {@code DELAYED_DELIVERY} is missing because it needs a
     * plugin that may not be installed, so the core generates a time-to-live and dead-letter
     * ladder instead rather than assuming. {@code CONSISTENT_HASH_ROUTING} is missing for the
     * same reason.
     */
    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(
            Capability.EXCHANGE_ROUTING,
            Capability.TOPIC_WILDCARDS,
            Capability.HEADERS_ROUTING,
            Capability.PUBLISHER_CONFIRMS,
            Capability.TRANSACTIONS,
            Capability.DEAD_LETTER_NATIVE,
            Capability.TTL_PER_MESSAGE,
            Capability.PRIORITY,
            Capability.QUORUM_QUEUES,
            Capability.STREAMS,
            Capability.SINGLE_ACTIVE_CONSUMER));

    @Override
    public String scheme() {
        return "amqp";
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public Set<Capability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public TransportConnection connect(ConnectionConfig config) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(config.url());
        } catch (URISyntaxException | java.security.NoSuchAlgorithmException | java.security.KeyManagementException e) {
            throw new TransportException("invalid broker URL: " + config.url(), e);
        }

        if (config.username() != null) {
            factory.setUsername(config.username());
        }
        if (config.password() != null) {
            factory.setPassword(config.password());
        }
        if (config.virtualHost() != null) {
            factory.setVirtualHost(config.virtualHost());
        }
        factory.setConnectionTimeout((int) config.connectionTimeout().toMillis());

        // Topology recovery is what makes a reconnect transparent: without it the client
        // reopens the connection but the consumers and bindings are gone.
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(TimeUnit.SECONDS.toMillis(5));

        try {
            return new RabbitMqConnection(factory.newConnection(config.clientName()), config);
        } catch (Exception e) {
            throw new TransportException("could not connect to " + config.url(), e);
        }
    }
}
