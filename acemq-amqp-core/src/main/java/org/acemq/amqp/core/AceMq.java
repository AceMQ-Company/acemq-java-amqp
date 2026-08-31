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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
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
    private final Interceptors interceptors = new Interceptors();

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
    private final AtomicBoolean publishingPaused = new AtomicBoolean();

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

    /**
     * Stops every consumer taking new work and waits for what is in hand.
     *
     * <p>The consuming half of a cutover. Publishers are untouched, deliberately: a service
     * being taken out of rotation still has requests to finish, and those requests still need to
     * publish. Stop consuming first, stop publishing second, and the order is what makes the
     * cutover clean.
     *
     * <p>Stream readers are closed rather than drained, because a stream is resumable by
     * construction: the next reader starts from the offset this one recorded.
     *
     * @param timeout how long to wait for handlers still running
     * @return whether everything finished in time. False means something is still running, and
     *     the caller has a decision to make rather than a guarantee
     */
    public boolean drainConsumers(Duration timeout) {
        boolean quiet = true;
        for (AutoCloseable managedItem : managed) {
            if (managedItem instanceof MessageConsumer) {
                quiet &= ((MessageConsumer) managedItem).drain(timeout);
            } else if (managedItem instanceof StreamConsumer) {
                ((StreamConsumer) managedItem).close();
            }
        }
        log.info("drained the consumers on {}; everything finished: {}", transport.name(), quiet);
        return quiet;
    }

    /**
     * Stops every consumer taking new work, reversibly.
     *
     * <p>Unlike {@link #drainConsumers(Duration)} this does not wait and can be undone. For a
     * maintenance window, or for holding a service still while something downstream catches up.
     */
    public void pauseConsuming() {
        for (AutoCloseable managedItem : managed) {
            if (managedItem instanceof MessageConsumer) {
                ((MessageConsumer) managedItem).pause();
            }
        }
        log.info("paused consuming on {}", transport.name());
    }

    /** Starts every paused consumer again. */
    public void resumeConsuming() {
        for (AutoCloseable managedItem : managed) {
            if (managedItem instanceof MessageConsumer) {
                ((MessageConsumer) managedItem).resume();
            }
        }
        log.info("resumed consuming on {}", transport.name());
    }

    /**
     * Refuses further publishes on this connection.
     *
     * <p>The publishing half of a cutover, and the last thing to do before shutting a service
     * down. A publish attempted while paused throws
     * {@link org.acemq.amqp.api.PublishingPausedException} without sending anything, so there is
     * never a half-published message to reason about.
     *
     * <p>Publishes already in flight are not interrupted. They have been sent, and the broker
     * will confirm them or not on its own schedule.
     */
    public void pausePublishing() {
        if (publishingPaused.compareAndSet(false, true)) {
            log.info("paused publishing on {}", transport.name());
        }
    }

    /** Allows publishing again. */
    public void resumePublishing() {
        if (publishingPaused.compareAndSet(true, false)) {
            log.info("resumed publishing on {}", transport.name());
        }
    }

    /** @return whether publishing is currently refused */
    public boolean isPublishingPaused() {
        return publishingPaused.get();
    }

    /** @return whether every consumer on this connection is paused; false when there are none */
    public boolean isConsumingPaused() {
        boolean any = false;
        for (AutoCloseable managedItem : managed) {
            if (managedItem instanceof MessageConsumer) {
                any = true;
                if (!((MessageConsumer) managedItem).isPaused()) {
                    return false;
                }
            }
        }
        return any;
    }

    /** @return how many messages every consumer on this connection is handling right now */
    public long inFlight() {
        long total = 0;
        for (AutoCloseable managedItem : managed) {
            if (managedItem instanceof MessageConsumer) {
                total += ((MessageConsumer) managedItem).inFlight();
            }
        }
        return total;
    }

    /** @return the transport's short name, such as {@code rabbitmq} */
    public String transportName() {
        return transport.name();
    }

    /**
     * Whether the broker is currently refusing publishes.
     *
     * <p>RabbitMQ blocks publishing connections when it runs low on memory or disk. Publishing
     * while blocked does not fail — it waits, up to {@code blockedTimeout}, and then throws
     * {@link org.acemq.amqp.transport.ConnectionBlockedException}.
     *
     * <p>Useful in a readiness probe, and deliberately not in a liveness one. A blocked broker is
     * under pressure, not broken, and a service that reports itself dead every time an alarm
     * fires will be restarted by its orchestrator while the broker recovers on its own.
     *
     * @return whether the broker is refusing publishes right now
     */
    public boolean isBlocked() {
        return connection.isBlocked();
    }

    /**
     * @return the broker's own explanation for blocking, when it is blocked. RabbitMQ says
     *     {@code low on memory} or {@code low on disk}, which is the difference between an
     *     operator adding consumers and one adding disk
     */
    public java.util.Optional<String> blockedReason() {
        return connection.blockedReason();
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
                telemetry, managed::add, publishingPaused::get, PublishOptions.defaults(), interceptors);
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
     * Creates a publisher that publishes on the caller's terms rather than the safe defaults.
     *
     * <p>The defaults — written to disk, unroutable treated as a failure — are what most messages
     * want. This is for the ones that do not: telemetry that may be dropped, events with a
     * shelf life, fan-outs nobody is required to be listening to.
     *
     * @param exchange target exchange, or the empty string to publish straight to a queue
     * @param routingKey routing key, or the queue name when publishing without an exchange
     * @param payloadType type this publisher sends
     * @param options how these messages should be published
     * @param <T> payload type
     * @return a publisher, closed automatically when this instance closes
     */
    public <T> DefaultPublisher<T> publisher(
            String exchange, String routingKey, Class<T> payloadType, PublishOptions options) {
        Objects.requireNonNull(payloadType, "payloadType");
        Objects.requireNonNull(options, "options");
        DefaultPublisher<T> publisher = new DefaultPublisher<>(connection, publishCodec, exchange, routingKey, origin,
                telemetry, managed::add, publishingPaused::get, options, interceptors);
        managed.add(publisher);
        return publisher;
    }

    /**
     * Runs something around every publish on this connection.
     *
     * <p>For what every message in an organisation needs and no library can guess: a tenant
     * identifier, an authorisation token, a schema version, a size limit. Without this they get
     * copied into every call site, where one of them is always missing.
     *
     * <pre>{@code
     * mq.intercept((PublishInterceptor) context ->
     *         context.withEnvelope(context.envelope().toBuilder()
     *                 .header("tenant", TenantContext.current())
     *                 .build()));
     * }</pre>
     *
     * <p>Applies to publishers already created as well as later ones, so ordering against
     * start-up code is not a trap. Register during start-up all the same: a message already on
     * its way will not see an interceptor added mid-flight.
     *
     * @param interceptor what to run; throwing from it refuses the publish
     * @return this instance, for chaining
     */
    public AceMq intercept(org.acemq.amqp.api.PublishInterceptor interceptor) {
        interceptors.add(Objects.requireNonNull(interceptor, "interceptor"));
        return this;
    }

    /**
     * Runs something around every handler on this connection.
     *
     * <p>For the work that surrounds handling rather than being part of it: log context, tenant
     * adoption, opening and closing a unit of work.
     *
     * <p>Throwing from {@code beforeHandle} fails the delivery, which means it is retried and
     * eventually dead-lettered. That is the honest outcome for a refused message; acknowledging
     * one nothing processed is not.
     *
     * @param interceptor what to run around each handler
     * @return this instance, for chaining
     */
    public AceMq intercept(org.acemq.amqp.api.ConsumeInterceptor interceptor) {
        interceptors.add(Objects.requireNonNull(interceptor, "interceptor"));
        return this;
    }

    /**
     * Counts the messages waiting in a queue.
     *
     * <p>A snapshot, and useful as one: queue depth is the number an operator wants before
     * deciding whether to replay, scale up, or leave well alone. It is not a control value — a
     * live queue is a different depth by the time this returns, so a loop written around it is a
     * loop written around a number that has already changed.
     *
     * @param queue queue to measure
     * @return how many messages are waiting
     */
    public long messageCount(String queue) {
        Objects.requireNonNull(queue, "queue");
        return connection.messageCount(queue);
    }

    /**
     * Moves failed messages back to the queue they failed in.
     *
     * <p>The other half of dead-lettering. Messages that exhausted their retries are in
     * {@code <queue>.dlq} and messages that could not be decoded are in {@code <queue>.parked};
     * both are safe there and neither comes back on its own. This is how they do, once the bug is
     * fixed or the downstream service is up.
     *
     * <pre>{@code
     * Replay replay = mq.replay("orders.new");
     * long waiting = replay.pending();     // look first
     * replay.replay(50);                   // then move a bounded batch
     * }</pre>
     *
     * @param queue the queue that was being consumed, not the dead-letter queue itself
     * @return a replay reading {@code <queue>.dlq}; call {@link Replay#parked()} for the other one
     */
    public Replay replay(String queue) {
        Objects.requireNonNull(queue, "queue");
        return new Replay(connection, queue, queue + ".dlq");
    }

    /**
     * Declares a stream: an append-only log that keeps messages until retention removes them.
     *
     * <p>Not a queue with different settings. A stream is read without being emptied, every
     * consumer holds its own position, and nothing is ever dead-lettered — see
     * {@link StreamConsumer} for what that rules out.
     *
     * <p>Retention is not optional in practice. A stream with neither a size nor an age limit
     * grows until the disk is full, and the broker will not stop it.
     *
     * @param name stream name
     * @param maxAge how long a message is kept, or {@code null} for no age limit
     * @param maxLengthBytes how large the stream may grow, or {@code null} for no size limit
     * @return this instance, for chaining
     * @throws org.acemq.amqp.api.AceMqException if the broker does not support streams
     */
    public AceMq declareStream(String name, @Nullable Duration maxAge, @Nullable Long maxLengthBytes) {
        requireStreams(name);
        Map<String, Object> arguments = new LinkedHashMap<>();
        if (maxAge != null) {
            // The broker's own syntax. Seconds express every duration exactly, where rounding to
            // days would quietly change how much history is kept.
            arguments.put("x-max-age", maxAge.getSeconds() + "s");
        }
        if (maxLengthBytes != null) {
            arguments.put("x-max-length-bytes", maxLengthBytes);
        }
        if (arguments.isEmpty()) {
            log.warn("stream {} is declared with no retention. It will grow until the disk is full, and the broker"
                    + " will not stop it.", name);
        }
        connection.declareQueue(name, QueueType.STREAM, true, arguments);
        return this;
    }

    /**
     * Reads a stream, starting where the returned reader is told to.
     *
     * <pre>{@code
     * mq.stream("orders.log", OrderPlaced.class)
     *         .fromFirst()
     *         .consume(message -> projection.apply(message.payload()));
     * }</pre>
     *
     * <p>Nothing is consumed until {@code consume} is called.
     *
     * @param queue stream to read
     * @param payloadType type to decode payloads into
     * @param <T> payload type
     * @return a reader awaiting a starting position and a handler
     * @throws org.acemq.amqp.api.AceMqException if the broker does not support streams
     */
    public <T> StreamReader<T> stream(String queue, Class<T> payloadType) {
        requireStreams(queue);
        Objects.requireNonNull(payloadType, "payloadType");
        return new StreamReader<>(queue, payloadType, StreamOptions.defaults(), (reader, handler) -> {
            Codec reading = reader.options().codec().orElse(consumeCodec);
            DefaultStreamConsumer<T> consumer = new DefaultStreamConsumer<>(
                    connection, reading, reader.queue(), payloadType, reader.options(), handler, telemetry);
            managed.add(consumer);
            consumer.start();
            return consumer;
        });
    }

    private void requireStreams(String name) {
        if (!supports(Capability.STREAMS)) {
            // Declared, not assumed. Falling back to a classic queue would look like it worked
            // and would lose replay, retention and every consumer's independent position.
            throw new org.acemq.amqp.api.AceMqException("the " + transport.name() + " transport does not support"
                    + " streams, so '" + name + "' cannot be one. Streams need RabbitMQ 3.9 or later over the"
                    + " amqp:// transport.");
        }
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
     * A chain of steps, each with its own queue.
     *
     * <pre>{@code
     * try (Pipeline<Order> fulfilment = mq.pipeline("fulfilment", Order.class)
     *         .step("validate", Order.class, new ValidateOrder())
     *         .step("enrich", Enriched.class, new EnrichOrder())
     *             .withRetry(RetryPolicy.exponential(5, Duration.ofSeconds(2)))
     *             .concurrency(10)
     *         .step("dispatch", Void.class, new DispatchOrder())
     *         .build()) {
     *
     *     fulfilment.send(order);
     * }
     * }</pre>
     *
     * <p>Every hop goes through the broker, so a crash leaves the message where it was, a slow
     * step grows its own queue rather than blocking the chain, and the step needing ten
     * consumers gets ten while its neighbour keeps one. The cost is a round trip and a durable
     * write per hop.
     *
     * <p>Where a message is going travels with it, so nothing here coordinates.
     *
     * @param name pipeline name; also the exchange, with one queue per step beneath it
     * @param entryType type the pipeline is entered with
     * @param <T> entry type
     * @return a builder; nothing is declared until build() is called
     */
    public <T> PipelineBuilder<T, T> pipeline(String name, Class<T> entryType) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(entryType, "entryType");
        return new PipelineBuilder<>(this, name, entryType, new java.util.ArrayList<>());
    }

    /**
     * A queue where messages sharing a key are handled in the order they were sent.
     *
     * <pre>{@code
     * try (OrderedQueue<Order> orders = mq.ordered("orders", Order.class)
     *         .partitions(8)
     *         .keyedBy(Order::customerId)
     *         .declare()) {
     *
     *     orders.send(order);
     *     orders.consume(message -> ledger.post(message.payload()));
     * }
     * }</pre>
     *
     * <p>The key decides a partition, each partition is a queue, and each queue has exactly one
     * consumer. That gives ordering within a key and parallelism across keys, which is almost
     * always the ordering anyone actually wanted: two orders for one customer are sequenced, two
     * orders for different customers never needed to be.
     *
     * <p>The retry ladder is not available here, because republishing a failed message to come
     * back later is what breaks a sequence. See {@link OrderedQueue.OnFailure} for the three
     * things that can happen instead, all of which preserve order and none of which are free.
     *
     * @param name logical name; also the exchange, with one queue per partition beneath it
     * @param payloadType type to publish and decode
     * @param <T> payload type
     * @return a builder; nothing is declared until declare() is called
     */
    public <T> OrderedQueueBuilder<T> ordered(String name, Class<T> payloadType) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(payloadType, "payloadType");
        return new OrderedQueueBuilder<>(this, name, payloadType);
    }

    /**
     * Consumes a queue with several consumers, resizable while the application runs.
     *
     * <pre>{@code
     * ConsumerGroup orders = mq.consumeGroup("orders.new", Order.class, handler)
     *         .concurrency(4)
     *         .prefetch(50)
     *         .start();
     *
     * orders.scaleTo(8);
     * }</pre>
     *
     * <p>Prefetch and concurrency are the two numbers that decide how fast a queue drains, and
     * both are usually guessed once and frozen in a properties file because changing either has
     * meant a redeploy. Neither has to be.
     *
     * @param queue queue to consume
     * @param payloadType type to decode payloads into
     * @param handler called for each message
     * @param <T> payload type
     * @return a builder; nothing consumes until start() is called
     */
    public <T> ConsumerGroupBuilder<T> consumeGroup(
            String queue, Class<T> payloadType, org.acemq.amqp.api.MessageHandler<T> handler) {
        Objects.requireNonNull(payloadType, "payloadType");
        Objects.requireNonNull(handler, "handler");
        return new ConsumerGroupBuilder<>(queue, payloadType, ConsumerOptions.defaults(), 1, (builder, howMany) -> {
            ConsumerGroup group = new ConsumerGroup(
                    builder.queue(),
                    builder.options().prefetch(),
                    () -> consume(builder.queue(), builder.payloadType(), builder.options(), handler));
            group.scaleTo(howMany);
            managed.add(group);
            return group;
        });
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
                handler, telemetry, interceptors);
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
