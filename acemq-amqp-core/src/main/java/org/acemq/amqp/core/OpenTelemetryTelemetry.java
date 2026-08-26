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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Telemetry;
import org.jspecify.annotations.Nullable;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * Emits OpenTelemetry spans for publishes and deliveries.
 *
 * <p>The point of tracing a message system is the join: the span covering a handler must be a
 * child of the span that published the message, even though the two ran in different processes
 * minutes apart. That is what the {@code traceparent} header carries, and why it is written on
 * the way out and read on the way in.
 *
 * <p>Span names and attributes follow the OpenTelemetry messaging conventions, so existing
 * tooling recognises them without configuration.
 */
final class OpenTelemetryTelemetry implements Telemetry {

    private static final AttributeKey<String> MESSAGING_SYSTEM = AttributeKey.stringKey("messaging.system");
    private static final AttributeKey<String> DESTINATION = AttributeKey.stringKey("messaging.destination.name");
    private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("messaging.operation");
    private static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("messaging.message.id");
    private static final AttributeKey<String> CONVERSATION_ID = AttributeKey
            .stringKey("messaging.message.conversation_id");
    private static final AttributeKey<String> ROUTING_KEY = AttributeKey
            .stringKey("messaging.rabbitmq.destination.routing_key");
    private static final AttributeKey<String> MESSAGE_TYPE = AttributeKey.stringKey("messaging.acemq.message_type");
    private static final AttributeKey<Long> ATTEMPT = AttributeKey.longKey("messaging.acemq.attempt");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("messaging.acemq.outcome");

    private static final TextMapSetter<Map<String, String>> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(key, value);
        }
    };

    private static final TextMapGetter<Map<String, Object>> GETTER = new TextMapGetter<Map<String, Object>>() {
        @Override
        public Iterable<String> keys(Map<String, Object> carrier) {
            return carrier.keySet();
        }

        @Override
        public @Nullable String get(@Nullable Map<String, Object> carrier, String key) {
            Object value = carrier == null ? null : carrier.get(key);
            return value == null ? null : value.toString();
        }
    };

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final String transport;

    OpenTelemetryTelemetry(OpenTelemetry openTelemetry, String transport) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("org.acemq.amqp");
        this.transport = transport;
    }

    // The context scope returned by makeCurrent() is not leaked: it is handed to
    // SpanScope, which closes it and ends the span. The analyser cannot see across
    // that hand-off, and a try-with-resources here would close the scope before the
    // message is even published.
    @SuppressWarnings("MustBeClosedChecker")
    @Override
    public Scope publishStarted(String exchange, String routingKey, Envelope envelope) {
        String destination = exchange == null || exchange.isEmpty() ? routingKey : exchange;
        Span span = tracer.spanBuilder(destination + MetricNames.SPAN_PUBLISH_SUFFIX)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(MESSAGING_SYSTEM, transport)
                .setAttribute(DESTINATION, destination == null ? "" : destination)
                .setAttribute(OPERATION, "publish")
                .setAttribute(MESSAGE_ID, envelope.id())
                .setAttribute(CONVERSATION_ID, envelope.correlationId())
                .setAttribute(ROUTING_KEY, routingKey == null ? "" : routingKey)
                .setAttribute(MESSAGE_TYPE, envelope.type())
                .startSpan();
        return new SpanScope(span, span.makeCurrent());
    }

    @SuppressWarnings("MustBeClosedChecker") // Closed by SpanScope, as above.
    @Override
    public Scope consumeStarted(String queue, Envelope envelope) {
        // The parent lives in the message's own headers, written when it was published.
        Context parent = openTelemetry
                .getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), new LinkedHashMap<>(envelope.headers()), GETTER);

        Span span = tracer.spanBuilder(queue + MetricNames.SPAN_PROCESS_SUFFIX)
                .setParent(parent)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(MESSAGING_SYSTEM, transport)
                .setAttribute(DESTINATION, queue)
                .setAttribute(OPERATION, "process")
                .setAttribute(MESSAGE_ID, envelope.id())
                .setAttribute(CONVERSATION_ID, envelope.correlationId())
                .setAttribute(MESSAGE_TYPE, envelope.type())
                .setAttribute(ATTEMPT, (long) envelope.attempt())
                .startSpan();
        return new SpanScope(span, span.makeCurrent());
    }

    @Override
    public void messageRetried(String queue, Envelope envelope, Duration delay) {
        Span current = Span.current();
        if (current.isRecording()) {
            current.addEvent("message.retried", io.opentelemetry.api.common.Attributes.of(
                    AttributeKey.stringKey("messaging.destination.name"), queue,
                    AttributeKey.longKey("messaging.acemq.retry_delay_ms"), delay.toMillis(),
                    ATTEMPT, (long) envelope.attempt()));
        }
    }

    @Override
    public void messageDeadLettered(String queue, Envelope envelope, String reason) {
        Span current = Span.current();
        if (current.isRecording()) {
            // The reason is unbounded text, which a span tolerates and a metric does not.
            current.addEvent("message.dead_lettered", io.opentelemetry.api.common.Attributes.of(
                    AttributeKey.stringKey("messaging.destination.name"), queue,
                    AttributeKey.stringKey("messaging.acemq.reason"), reason == null ? "" : reason,
                    ATTEMPT, (long) envelope.attempt()));
        }
    }

    @Override
    public Map<String, String> propagationHeaders() {
        Map<String, String> carrier = new HashMap<>();
        openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), carrier, SETTER);
        return carrier;
    }

    /** Ends a span and closes the scope that made it current. */
    private static final class SpanScope implements Telemetry.Scope {

        private final Span span;
        private final io.opentelemetry.context.Scope scope;
        private boolean closed;

        SpanScope(Span span, io.opentelemetry.context.Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        @Override
        public void outcome(String outcome) {
            span.setAttribute(OUTCOME, outcome);
            if (MetricNames.OUTCOME_UNROUTABLE.equals(outcome)
                    || MetricNames.OUTCOME_FAILED.equals(outcome)
                    || MetricNames.OUTCOME_DEAD_LETTERED.equals(outcome)) {
                span.setStatus(StatusCode.ERROR, outcome);
            }
        }

        @Override
        public void failed(Throwable failure) {
            span.recordException(failure);
            span.setStatus(StatusCode.ERROR, failure.getMessage() == null ? "failed" : failure.getMessage());
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            scope.close();
            span.end();
        }
    }
}
