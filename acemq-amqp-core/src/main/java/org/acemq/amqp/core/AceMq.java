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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Transport;
import org.acemq.amqp.transport.TransportConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point: one connection to one broker, and the publishers and consumers built on it.
 *
 * <pre>{@code
 * try (AceMq mq = AceMq.connect("amqp://localhost")) {
 *     mq.declareExchange("orders", "topic");
 *     mq.declareQueue("orders.new");
 *     mq.bind("orders.new", "orders", "order.placed");
 *
 *     Publisher<String> publisher = mq.publisher("orders", "order.placed");
 *     publisher.send("{\"id\":\"o-1\"}");
 *
 *     mq.consume("orders.new", String.class, message -> process(message.payload()));
 * }
 * }</pre>
 *
 * <p>Instances are thread safe and long lived. Closing one closes every consumer and publisher
 * created from it, so a try-with-resources block cannot leak a subscription.
 */
public final class AceMq implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AceMq.class);

    private final Transport transport;
    private final TransportConnection connection;
    private final Codec codec;
    private final String origin;
    private final CopyOnWriteArrayList<AutoCloseable> managed = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private AceMq(Transport transport, TransportConnection connection, Codec codec, String origin) {
        this.transport = transport;
        this.connection = connection;
        this.codec = codec;
        this.origin = origin;
    }

    /**
     * Connects to a broker, choosing the transport from the URL scheme.
     *
     * @param url broker URL, for example {@code amqp://localhost:5672}
     * @return an open connection
     * @throws org.acemq.amqp.api.AceMqException if no transport handles the scheme, or the
     *     broker cannot be reached
     */
    public static AceMq connect(String url) {
        return connect(ConnectionConfig.url(url).build());
    }

    /**
     * Connects to a broker with explicit settings.
     *
     * @param config connection settings
     * @return an open connection
     */
    public static AceMq connect(ConnectionConfig config) {
        Transport transport = Transports.forScheme(config.scheme());
        log.debug("connecting with the {} transport to {}", transport.name(), config);
        TransportConnection connection = transport.connect(config);
        return new AceMq(transport, connection, new StringCodec(), defaultOrigin(config));
    }

    /**
     * Reports what the connected broker supports.
     *
     * <p>Worth logging at startup. When a capability is missing the engine either applies a
     * documented alternative or refuses to start, so knowing the answer explains behaviour
     * that would otherwise look arbitrary.
     *
     * @return the capabilities of this broker
     */
    public Set<Capability> capabilities() {
        return transport.capabilities();
    }

    /** @return whether the broker supports a capability */
    public boolean supports(Capability capability) {
        return transport.capabilities().contains(capability);
    }

    /** @return the transport's short name, such as {@code rabbitmq} */
    public String transportName() {
        return transport.name();
    }

    /**
     * Declares a durable exchange.
     *
     * @param name exchange name
     * @param type {@code direct}, {@code topic}, {@code fanout} or {@code headers}
     * @return this instance, for chaining
     */
    public AceMq declareExchange(String name, String type) {
        connection.declareExchange(name, type, true);
        return this;
    }

    /**
     * Declares a durable quorum queue.
     *
     * <p>Quorum is the default because a queue that survives losing its node is what almost
     * everyone wants and almost nobody remembers to ask for.
     *
     * @param name queue name
     * @return this instance, for chaining
     */
    public AceMq declareQueue(String name) {
        return declareQueue(name, QueueType.QUORUM, Collections.emptyMap());
    }

    /**
     * Declares a queue of a specific type.
     *
     * @param name queue name
     * @param type queue implementation
     * @param arguments broker-specific arguments
     * @return this instance, for chaining
     */
    public AceMq declareQueue(String name, QueueType type, Map<String, Object> arguments) {
        if (type == QueueType.QUORUM && !supports(Capability.QUORUM_QUEUES)) {
            throw new org.acemq.amqp.api.AceMqException("the " + transport.name()
                    + " broker does not support quorum queues, so '" + name + "' cannot be declared as one."
                    + " Request QueueType.CLASSIC explicitly if that is acceptable for this queue.");
        }
        connection.declareQueue(name, type, true, arguments);
        return this;
    }

    /**
     * Binds a queue to an exchange.
     *
     * @param queue queue name
     * @param exchange exchange name
     * @param routingKey routing key or pattern
     * @return this instance, for chaining
     */
    public AceMq bind(String queue, String exchange, String routingKey) {
        connection.bindQueue(queue, exchange, routingKey);
        return this;
    }

    /**
     * Creates a publisher for one destination.
     *
     * @param exchange target exchange, or the empty string to publish straight to a queue
     * @param routingKey routing key, or the queue name when publishing without an exchange
     * @param <T> payload type
     * @return a publisher, closed automatically when this instance closes
     */
    public <T> Publisher<T> publisher(String exchange, String routingKey) {
        DefaultPublisher<T> publisher = new DefaultPublisher<>(connection, codec, exchange, routingKey, origin);
        managed.add(publisher);
        return publisher;
    }

    /**
     * Starts consuming a queue.
     *
     * @param queue queue to consume
     * @param payloadType type to decode payloads into
     * @param handler called for each message; returning normally acknowledges it
     * @param <T> payload type
     * @return a handle that stops consumption when closed
     */
    public <T> MessageConsumer consume(
            String queue, Class<T> payloadType, org.acemq.amqp.api.MessageHandler<T> handler) {
        return consume(queue, payloadType, ConsumerOptions.defaults(), handler);
    }

    /**
     * Starts consuming a queue with explicit options.
     *
     * @param queue queue to consume
     * @param payloadType type to decode payloads into
     * @param options prefetch and failure behaviour
     * @param handler called for each message
     * @param <T> payload type
     * @return a handle that stops consumption when closed
     */
    public <T> MessageConsumer consume(
            String queue,
            Class<T> payloadType,
            ConsumerOptions options,
            org.acemq.amqp.api.MessageHandler<T> handler) {
        DefaultConsumer<T> consumer = new DefaultConsumer<>(connection, codec, queue, payloadType, options, handler);
        managed.add(consumer);
        consumer.start();
        return consumer;
    }

    /**
     * Removes a queue and its contents.
     *
     * @param name queue to delete
     * @implNote intended for test fixtures and topology migration rather than application code.
     */
    public void deleteQueue(String name) {
        connection.deleteQueue(name);
    }

    /** @return whether the underlying connection is usable */
    public boolean isOpen() {
        return !closed.get() && connection.isOpen();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // Consumers stop before the connection goes, so in-flight deliveries settle rather
        // than becoming redeliveries on the next start-up.
        for (AutoCloseable closeable : managed) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.debug("ignoring error while closing {}", closeable, e);
            }
        }
        managed.clear();
        connection.close();
    }

    private static String defaultOrigin(ConnectionConfig config) {
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return config.clientName() + "@" + host;
    }
}
