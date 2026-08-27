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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Transport;
import org.acemq.amqp.transport.TransportConnection;
import org.jspecify.annotations.Nullable;
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
 *     Publisher<Order> publisher = mq.publisher("orders", "order.placed", Order.class);
 *     publisher.send(new Order("o-1", 42.00));
 *
 *     mq.consume("orders.new", Order.class, message -> process(message.payload()));
 * }
 * }</pre>
 *
 * <p>Payloads are JSON unless the publisher is told otherwise, and a consumer reads whatever
 * format arrives. Neither side has to say anything about serialisation for the common case;
 * {@link DefaultPublisher#asXml()} and its neighbours are there for the rest.
 *
 * <p>Instances are thread safe and long lived. Closing one closes every consumer and publisher
 * created from it, so a try-with-resources block cannot leak a subscription.
 */
public final class AceMq implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AceMq.class);

    private final Transport transport;
    private final TransportConnection connection;

    /**
     * What publishers write, and what consumers read, and deliberately not the same thing.
     *
     * <p>Publishing uses one format, because the bytes on a queue are a contract with services
     * that are not being redeployed, and a queue carrying two formats at once is one no consumer
     * can be written against. Consuming uses every format on the classpath, because a consumer
     * that refuses a message it could have read has turned somebody else's deployment into its
     * own outage. Write one, read all: that asymmetry is what makes changing format two ordinary
     * releases rather than a flag day.
     */
    private final Codec publishCodec;

    private final Codec consumeCodec;
    private final String origin;
    private final Telemetry telemetry;
    private final CopyOnWriteArrayList<AutoCloseable> managed = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private AceMq(
            Transport transport,
            TransportConnection connection,
            Codec publishCodec,
            Codec consumeCodec,
            String origin,
            Telemetry telemetry) {
        this.transport = transport;
        this.connection = connection;
        this.publishCodec = publishCodec;
        this.consumeCodec = consumeCodec;
        this.origin = origin;
        // Resolved once per connection rather than per message: the classpath does not change
        // while the process runs, and probing on a hot path would be its own overhead.
        this.telemetry = telemetry;
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
     * Connects to a broker with an explicit telemetry sink.
     *
     * @param url broker URL
     * @param telemetry where to report
     * @return an open connection
     */
    public static AceMq connect(String url, @Nullable Telemetry telemetry) {
        return connect(ConnectionConfig.url(url).build(), telemetry);
    }

    /**
     * Connects to a broker with explicit settings.
     *
     * @param config connection settings
     * @return an open connection
     */
    public static AceMq connect(ConnectionConfig config) {
        return connect(config, null);
    }

    /**
     * Connects with an explicit telemetry sink.
     *
     * <p>Preferable to auto-detection wherever the answer is known. Auto-detection reaches for
     * Micrometer's global registry and OpenTelemetry's global instance, both of which are
     * process-wide state; passing a sink lets two connections report separately and lets a
     * test observe only its own measurements.
     *
     * @param config connection settings
     * @param telemetry where to report, or {@code null} to detect what is on the classpath
     * @return an open connection
     */
    public static AceMq connect(ConnectionConfig config, @Nullable Telemetry telemetry) {
        return connect(config, telemetry, null);
    }

    /**
     * Connects to a broker, reading and writing payloads with the given codec.
     *
     * @param url broker URL
     * @param codec how payloads become bytes and back
     * @return an open connection
     */
    public static AceMq connect(String url, Codec codec) {
        return connect(ConnectionConfig.url(url).build(), null, codec);
    }

    /**
     * Connects to a broker with an explicit telemetry sink and codec.
     *
     * @param url broker URL
     * @param telemetry where to report, or {@code null} to detect what is on the classpath
     * @param codec how payloads become bytes and back
     * @return an open connection
     */
    public static AceMq connect(String url, @Nullable Telemetry telemetry, Codec codec) {
        return connect(ConnectionConfig.url(url).build(), telemetry, codec);
    }

    /**
     * Connects with an explicit telemetry sink and codec.
     *
     * <p>Passing a codec fixes the format in both directions. Without one, publishers write JSON
     * and consumers read every format on the classpath, which is what most applications want and
     * what makes a format migration two ordinary releases. An application that has named a codec
     * has said it wants one format, and should not find its consumers quietly accepting others.
     *
     * @param config connection settings
     * @param telemetry where to report, or {@code null} to detect what is on the classpath
     * @param codec the single format to read and write, or {@code null} for the defaults
     * @return an open connection
     */
    public static AceMq connect(ConnectionConfig config, @Nullable Telemetry telemetry, @Nullable Codec codec) {
        Transport transport = Transports.forScheme(config.scheme());
        log.debug("connecting with the {} transport to {}", transport.name(), config);
        TransportConnection connection = transport.connect(config);
        Telemetry sink = telemetry != null ? telemetry : Telemetries.autoDetect(transport.name());
        // A codec the caller named is used for both directions: having asked for one format, an
        // application should not find its consumers quietly accepting others.
        Codec publish = codec != null ? codec : Codecs.forPublishing();
        Codec consume = codec != null ? codec : Codecs.forConsuming();
        return new AceMq(transport, connection, publish, consume, defaultOrigin(config), sink);
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

    /**
     * @return where this connection reports what it is doing; a no-op sink when neither
     *     Micrometer nor OpenTelemetry is on the classpath
     */
    public Telemetry telemetry() {
        return telemetry;
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
    public <T> DefaultPublisher<T> publisher(String exchange, String routingKey) {
        DefaultPublisher<T> publisher = new DefaultPublisher<>(connection, publishCodec, exchange, routingKey, origin,
                telemetry, managed::add);
        managed.add(publisher);
        return publisher;
    }

    /**
     * Creates a publisher for one destination and one payload type.
     *
     * <p>Preferable to the two-argument form, which infers the payload type from whatever the
     * result is assigned to and so will happily produce a {@code Publisher<Anything>}. Naming the
     * type makes the mistake a compile error instead of a message no consumer can read.
     *
     * <p>Writes JSON. Call {@link DefaultPublisher#asXml()}, {@link DefaultPublisher#asText()} or
     * {@link DefaultPublisher#as(Codec)} on the result for anything else.
     *
     * @param exchange target exchange, or the empty string to publish straight to a queue
     * @param routingKey routing key, or the queue name when publishing without an exchange
     * @param payloadType type this publisher sends
     * @param <T> payload type
     * @return a publisher, closed automatically when this instance closes
     */
    public <T> DefaultPublisher<T> publisher(String exchange, String routingKey, Class<T> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType");
        return publisher(exchange, routingKey);
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
        // Options may name a format. They have to be able to: Avro and Protobuf cannot be
        // recognised from the bytes, so a consumer of those has to be told, where a consumer of
        // anything self-describing is better off not being told at all.
        Codec reading = options.codec().orElse(consumeCodec);
        DefaultConsumer<T> consumer = new DefaultConsumer<>(connection, reading, queue, payloadType, options,
                handler, telemetry);
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

    /**
     * Plans and applies a declared topology.
     *
     * <p>Preferable to the individual declare methods for anything beyond a single queue: it
     * works out what would change before changing it, and can be asked to report without
     * acting at all.
     *
     * @return a planner bound to this connection
     */
    public TopologyPlanner topology() {
        return new TopologyPlanner(connection, transport);
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
