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
import org.acemq.amqp.api.PublishContext;
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
    private final Interceptors interceptors;

    /**
     * Where a reply should go, when this publisher is asking a question.
     *
     * <p>Not final and not carried by {@link #with(PublishOptions)}: it is set last, by
     * {@link #replyingTo(String)}, on a publisher built for one exchange and routing key.
     */
    private @org.jspecify.annotations.Nullable String replyTo;

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
                PublishOptions.defaults(), new Interceptors());
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
        this(connection, codec, exchange, routingKey, origin, telemetry, registrar, publishingPaused, options,
                new Interceptors());
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
            PublishOptions options,
            Interceptors interceptors) {
        this.connection = connection;
        this.codec = codec;
        this.exchange = exchange == null ? "" : exchange;
        this.routingKey = routingKey == null ? "" : routingKey;
        this.origin = origin;
        this.telemetry = telemetry;
        this.registrar = registrar;
        this.publishingPaused = publishingPaused;
        this.options = options;
        this.interceptors = interceptors;
    }

    /**
     * Names the queue a reply to this message should be sent to.
     *
     * <p>Used by {@link Requester}; publishing this by hand means taking on the correlation and
     * the timeout yourself, which is what {@code Requester} exists to avoid.
     *
     * @param replyQueue queue the responder should answer on
     * @return a publisher that asks for a reply
     */
    public DefaultPublisher<T> replyingTo(String replyQueue) {
        DefaultPublisher<T> copy = with(options);
        copy.replyTo = java.util.Objects.requireNonNull(replyQueue, "replyQueue");
        return copy;
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
                telemetry, registrar, publishingPaused, options, interceptors);
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
                registrar, publishingPaused, options, interceptors);
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
    public java.util.concurrent.CompletableFuture<PublishResult> sendAsync(T payload) {
        return sendAsync(payload, Envelope.of(routingKey.isEmpty() ? "message" : routingKey)
                .origin(origin)
                .build());
    }

    @Override
    public java.util.concurrent.CompletableFuture<PublishResult> sendAsync(T payload, Envelope envelope) {
        Prepared prepared = prepare(payload, envelope);
        // The scope closes when the confirm lands rather than when this method returns, so the
        // recorded latency is the message's, not the caller's. Closing it here would report every
        // asynchronous publish as taking microseconds.
        Telemetry.Scope scope = telemetry.publishStarted(exchange, routingKey, prepared.envelope);
        java.util.concurrent.CompletableFuture<ConfirmResult> confirm;
        try {
            confirm = connection.sendAsync(toMessage(prepared));
        } catch (RuntimeException e) {
            scope.failed(e);
            scope.close();
            interceptors.onPublishError(prepared.context, e);
            throw e;
        }
        return confirm.handle((result, failure) -> {
            try {
                if (failure != null) {
                    scope.failed(failure);
                    interceptors.onPublishError(prepared.context, failure);
                    throw failure instanceof RuntimeException
                            ? (RuntimeException) failure
                            : new AceMqException("publishing " + prepared.envelope.id() + " failed", failure);
                }
                return complete(prepared, result, scope);
            } finally {
                scope.close();
            }
        });
    }

    @Override
    public java.util.List<PublishResult> sendAll(java.util.Collection<? extends T> payloads) {
        java.util.Objects.requireNonNull(payloads, "payloads");
        // Everything goes out first, and only then is anything awaited. Awaiting each in turn
        // would be the synchronous path with extra objects.
        java.util.List<java.util.concurrent.CompletableFuture<PublishResult>> inFlight = new java.util.ArrayList<>(
                payloads.size());
        for (T payload : payloads) {
            inFlight.add(sendAsync(payload));
        }

        java.util.List<PublishResult> results = new java.util.ArrayList<>(inFlight.size());
        Throwable firstFailure = null;
        int failed = 0;
        for (java.util.concurrent.CompletableFuture<PublishResult> future : inFlight) {
            try {
                results.add(future.join());
            } catch (java.util.concurrent.CompletionException e) {
                failed++;
                if (firstFailure == null) {
                    firstFailure = e.getCause() == null ? e : e.getCause();
                }
            }
        }

        if (firstFailure != null) {
            // The count matters. A batch that half succeeded is the ordinary outcome of a broker
            // problem partway through, and a caller told only "it failed" will resend messages
            // that already arrived.
            throw new PublishFailedException(failed + " of " + inFlight.size() + " messages were not confirmed;"
                    + " " + results.size() + " were. The first failure was: " + firstFailure.getMessage(),
                    firstFailure);
        }
        return results;
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
        Prepared prepared = prepare(payload, envelope);

        try (Telemetry.Scope scope = telemetry.publishStarted(exchange, routingKey, prepared.envelope)) {
            ConfirmResult result;
            try {
                result = connection.send(toMessage(prepared));
            } catch (RuntimeException e) {
                scope.failed(e);
                interceptors.onPublishError(prepared.context, e);
                throw e;
            }
            return complete(prepared, result, scope);
        }
    }

    /**
     * Everything that happens before a message reaches the transport, shared by both paths.
     *
     * <p>Extracted so the asynchronous publish cannot drift from the synchronous one. Pause
     * checks, interceptors, provenance stamping and trace propagation are exactly the things
     * that get quietly forgotten in a second copy.
     */
    private Prepared prepare(T payload, Envelope envelope) {
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

        // Interceptors run before encoding, so one that adds a header changes what is actually
        // written rather than something already serialised. An exception here is deliberately
        // not caught: refusing a publish is what a policy interceptor is for.
        PublishContext context = new PublishContext(exchange, routingKey, stamped, payload);
        if (!interceptors.isEmptyForPublishing()) {
            try {
                context = interceptors.beforePublish(context);
            } catch (RuntimeException e) {
                interceptors.onPublishError(context, e);
                throw e;
            }
            stamped = context.envelope();
        }

        return new Prepared(stamped, context, codec.encode(payload));
    }

    /**
     * Builds the message to hand the transport.
     *
     * <p>Called inside the telemetry scope, and that is not incidental: trace context is read
     * from whatever span is current, so gathering these headers before the publish span exists
     * propagates the caller's span instead of this publish. A consumer would then attach its
     * work to the wrong parent -- a broken trace, and one that still looks like a trace.
     */
    private OutboundMessage toMessage(Prepared prepared) {
        Map<String, Object> headers = new LinkedHashMap<>(EnvelopeHeaders.toHeaders(prepared.envelope));
        headers.putAll(telemetry.propagationHeaders());

        OutboundMessage.Builder outbound = OutboundMessage.body(prepared.body)
                .exchange(exchange)
                .routingKey(routingKey)
                .headers(headers)
                .messageId(prepared.envelope.id())
                .contentType(codec.contentType())
                .expiration(options.expiration().orElse(null))
                .priority(options.priority().orElse(null))
                .replyTo(replyTo);
        if (!options.persistent()) {
            outbound.transientDelivery();
        }
        if (!options.mandatory()) {
            outbound.allowUnroutable();
        }
        return outbound.build();
    }

    /** Turns the broker's answer into a result or the right failure, for both paths. */
    private PublishResult complete(Prepared prepared, ConfirmResult result, Telemetry.Scope scope) {
        String id = prepared.envelope.id();
        if (!result.isConfirmed()) {
            scope.outcome(MetricNames.OUTCOME_FAILED);
            PublishFailedException failure = new PublishFailedException("the broker did not confirm message " + id
                    + " to exchange '" + exchange + "' with routing key '" + routingKey + "': " + result.detail());
            interceptors.onPublishError(prepared.context, failure);
            throw failure;
        }
        if (!result.isRouted()) {
            scope.outcome(MetricNames.OUTCOME_UNROUTABLE);
            PublishFailedException failure = new PublishFailedException("message " + id + " was accepted by the"
                    + " broker but could not be routed: nothing is bound to exchange '" + exchange + "' for"
                    + " routing key '" + routingKey + "'. The message has been discarded. Declare the binding, or"
                    + " publish with an explicit allowance for unroutable messages if that is intended.");
            interceptors.onPublishError(prepared.context, failure);
            throw failure;
        }

        scope.outcome(MetricNames.OUTCOME_CONFIRMED);
        log.debug("published {} to {}/{} in {}", id, exchange, routingKey, result.latency());
        PublishResult outcome = new PublishResult(id, result.isRouted(), result.latency());
        interceptors.afterConfirm(prepared.context, outcome);
        return outcome;
    }

    /** A message ready for the transport, with what interceptors and telemetry still need. */
    private static final class Prepared {

        private final Envelope envelope;
        private final PublishContext context;
        private final byte[] body;

        Prepared(Envelope envelope, PublishContext context, byte[] body) {
            this.envelope = envelope;
            this.context = context;
            this.body = body;
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
