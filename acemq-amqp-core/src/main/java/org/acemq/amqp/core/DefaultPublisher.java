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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.TransportConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes to one destination, waiting for the broker to confirm every message.
 *
 * <p>Three things are non-negotiable here, because each is a way messages get lost quietly:
 * the publish waits for a confirm, an unroutable message is an error rather than a shrug, and
 * the envelope is stamped onto the message so the consumer can tell what it received.
 *
 * <h2>Choosing a format</h2>
 *
 * <p>A publisher writes one format, chosen when it is built:
 *
 * <pre>{@code
 * Publisher<Order> orders = mq.publisher("orders", "order.placed", Order.class);          // JSON
 * Publisher<Order> legacy = mq.publisher("legacy", "order", Order.class).asXml();
 * Publisher<byte[]> files = mq.publisher("files", "file.new", byte[].class).asBytes();
 * Publisher<Order> avro   = mq.publisher("events", "order", Order.class).as(new AvroCodec(registry));
 * }</pre>
 *
 * <p>Chosen at the publisher rather than per message, and that is a decision rather than an
 * omission. A queue whose messages are sometimes JSON and sometimes XML is a queue nobody can
 * write a consumer against, so the useful place to decide is once, where the destination is
 * named. It also has to be here for a duller reason: by the time {@code send} has returned there
 * is nothing left to choose, because the message is already at the broker.
 *
 * @param <T> payload type
 */
public final class DefaultPublisher<T> implements Publisher<T> {

    private static final Logger log = LoggerFactory.getLogger(DefaultPublisher.class);

    private final TransportConnection connection;
    private final Codec codec;
    private final String exchange;
    private final String routingKey;
    private final String origin;
    private final Telemetry telemetry;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Consumer<AutoCloseable> registrar;
    private final java.util.function.BooleanSupplier publishingPaused;
    private final PublishOptions options;

    DefaultPublisher(
            TransportConnection connection,
            Codec codec,
            String exchange,
            String routingKey,
            String origin,
            Telemetry telemetry,
            Consumer<AutoCloseable> registrar,
            java.util.function.BooleanSupplier publishingPaused) {
        this(connection, codec, exchange, routingKey, origin, telemetry, registrar, publishingPaused,
                PublishOptions.defaults());
    }

    DefaultPublisher(
            TransportConnection connection,
            Codec codec,
            String exchange,
            String routingKey,
            String origin,
            Telemetry telemetry,
            Consumer<AutoCloseable> registrar,
            java.util.function.BooleanSupplier publishingPaused,
            PublishOptions options) {
        this.connection = connection;
        this.codec = codec;
        this.exchange = exchange == null ? "" : exchange;
        this.routingKey = routingKey == null ? "" : routingKey;
        this.origin = origin;
        this.telemetry = telemetry;
        this.registrar = registrar;
        this.publishingPaused = publishingPaused;
        this.options = options;
    }

    /**
     * The same destination, published differently.
     *
     * <p>A new publisher rather than a mutated one, for the same reason {@link #as(Codec)} is:
     * two threads sharing a publisher must not have its behaviour changed underneath them.
     *
     * @param options how these messages should be published
     * @return a publisher using those options; this one is left alone
     */
    public DefaultPublisher<T> with(PublishOptions options) {
        java.util.Objects.requireNonNull(options, "options");
        DefaultPublisher<T> derived = new DefaultPublisher<>(connection, codec, exchange, routingKey, origin,
                telemetry, registrar, publishingPaused, options);
        registrar.accept(derived);
        return derived;
    }

    /** @return how this publisher publishes */
    public PublishOptions options() {
        return options;
    }

    /**
     * The same destination, written in a named format.
     *
     * @param format short format name, such as {@code json} or {@code xml}
     * @return a publisher writing that format; this one is left alone
     * @throws AceMqException if no module on the classpath provides that format
     */
    public DefaultPublisher<T> as(String format) {
        return as(Codecs.byName(format));
    }

    /**
     * The same destination, written with a codec of the caller's own.
     *
     * <p>How a format AceMQ does not ship is used: implement {@link Codec} and pass it. Avro and
     * Protobuf belong here rather than behind a name of their own, because neither can be built
     * without a schema and a method taking no arguments would only be able to fail.
     *
     * @param format the codec to write with
     * @return a publisher writing that format; this one is left alone
     */
    public DefaultPublisher<T> as(Codec format) {
        // A new publisher rather than a mutated one. Two threads sharing a publisher while a
        // third changes its format is a race with no useful outcome, and a long-lived object
        // that quietly changes what it writes is worse than one that does not.
        // Options come along. Changing the format is not a request to start writing messages
        // transiently again, and losing them here would do exactly that, quietly.
        DefaultPublisher<T> switched = new DefaultPublisher<>(
                connection, Objects.requireNonNull(format, "format"), exchange, routingKey, origin, telemetry,
                registrar, publishingPaused, options);
        registrar.accept(switched);
        return switched;
    }

    /**
     * @return the same destination, written as JSON; what a publisher does anyway unless the
     *     connection was given a different codec
     */
    public DefaultPublisher<T> asJson() {
        return as("json");
    }

    /**
     * @return the same destination, written as XML
     * @throws AceMqException unless org.acemq:acemq-amqp-codec-xml is on the classpath
     */
    public DefaultPublisher<T> asXml() {
        return as("xml");
    }

    /**
     * @return the same destination, written as YAML, for messages a person will read as well as
     *     a program
     * @throws AceMqException unless org.acemq:acemq-amqp-codec-yaml is on the classpath
     */
    public DefaultPublisher<T> asYaml() {
        return as("yaml");
    }

    /**
     * @return the same destination, written as UTF-8 text; for payloads that are already strings
     */
    public DefaultPublisher<T> asText() {
        return as("text");
    }

    /**
     * @return the same destination, written as raw bytes, for payloads something else has
     *     already encoded
     */
    public DefaultPublisher<T> asBytes() {
        return as("bytes");
    }

    /** @return the format this publisher writes */
    public Codec codec() {
        return codec;
    }

    @Override
    public PublishResult send(T payload) {
        // The routing key doubles as the message type when none is stated. It is the most
        // useful default available: it is what an operator sees in the broker anyway.
        return send(payload, Envelope.of(routingKey.isEmpty() ? "message" : routingKey)
                .origin(origin)
                .build());
    }

    @Override
    public PublishResult send(T payload, Envelope envelope) {
        if (closed.get()) {
            throw new AceMqException("this publisher is closed");
        }
        if (publishingPaused.getAsBoolean()) {
            // Checked before anything is encoded or sent, so a paused connection costs nothing
            // and leaves no half-finished work to reason about.
            throw new org.acemq.amqp.api.PublishingPausedException("publishing is paused on this connection, so"
                    + " nothing was sent to " + exchange + "/" + routingKey + ". Resume it, or retry once the"
                    + " cutover is finished.");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }

        // Provenance is stamped by the engine, not by the caller. An application that builds
        // its own envelope to carry correlation should not also have to know its own hostname,
        // and an unattributed message is painful to trace back once it reaches a dead-letter
        // queue. An origin the caller did set is left alone.
        Envelope stamped = envelope.origin().isPresent() ? envelope : envelope.toBuilder().origin(origin).build();

        byte[] body = codec.encode(payload);

        try (Telemetry.Scope scope = telemetry.publishStarted(exchange, routingKey, stamped)) {
            // Trace context is gathered inside the scope, so the headers carry the span that is
            // being created here. That is what lets a consumer, in another process and possibly
            // much later, attach its work to this publish.
            Map<String, Object> headers = new LinkedHashMap<>(EnvelopeHeaders.toHeaders(stamped));
            headers.putAll(telemetry.propagationHeaders());

            OutboundMessage.Builder outbound = OutboundMessage.body(body)
                    .exchange(exchange)
                    .routingKey(routingKey)
                    .headers(headers)
                    .messageId(stamped.id())
                    .contentType(codec.contentType())
                    .expiration(options.expiration().orElse(null));
            if (!options.persistent()) {
                outbound.transientDelivery();
            }
            if (!options.mandatory()) {
                outbound.allowUnroutable();
            }
            OutboundMessage message = outbound.build();

            ConfirmResult result;
            try {
                result = connection.send(message);
            } catch (RuntimeException e) {
                scope.failed(e);
                throw e;
            }

            if (!result.isConfirmed()) {
                scope.outcome(MetricNames.OUTCOME_FAILED);
                throw new PublishFailedException("the broker did not confirm message " + stamped.id()
                        + " to exchange '" + exchange + "' with routing key '" + routingKey + "': "
                        + result.detail());
            }
            if (!result.isRouted()) {
                scope.outcome(MetricNames.OUTCOME_UNROUTABLE);
                throw new PublishFailedException("message " + stamped.id() + " was accepted by the broker but could"
                        + " not be routed: nothing is bound to exchange '" + exchange + "' for routing key '"
                        + routingKey + "'. The message has been discarded. Declare the binding, or publish with an"
                        + " explicit allowance for unroutable messages if that is intended.");
            }

            scope.outcome(MetricNames.OUTCOME_CONFIRMED);
            log.debug("published {} to {}/{} in {}", stamped.id(), exchange, routingKey, result.latency());
            return new PublishResult(stamped.id(), result.isRouted(), result.latency());
        }
    }

    @Override
    public void close() {
        // The connection is owned by AceMq, not by the publisher, so there is nothing to
        // release beyond refusing further sends.
        closed.set(true);
    }

    @Override
    public String toString() {
        return "Publisher{exchange=" + exchange + ", routingKey=" + routingKey + "}";
    }
}
