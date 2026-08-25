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
import java.util.concurrent.atomic.AtomicBoolean;

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
 * @param <T> payload type
 */
final class DefaultPublisher<T> implements Publisher<T> {

    private static final Logger log = LoggerFactory.getLogger(DefaultPublisher.class);

    private final TransportConnection connection;
    private final Codec codec;
    private final String exchange;
    private final String routingKey;
    private final String origin;
    private final Telemetry telemetry;
    private final AtomicBoolean closed = new AtomicBoolean();

    DefaultPublisher(
            TransportConnection connection,
            Codec codec,
            String exchange,
            String routingKey,
            String origin,
            Telemetry telemetry) {
        this.connection = connection;
        this.codec = codec;
        this.exchange = exchange == null ? "" : exchange;
        this.routingKey = routingKey == null ? "" : routingKey;
        this.origin = origin;
        this.telemetry = telemetry;
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

            OutboundMessage message = OutboundMessage.body(body)
                    .exchange(exchange)
                    .routingKey(routingKey)
                    .headers(headers)
                    .messageId(stamped.id())
                    .contentType(codec.contentType())
                    .build();

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
