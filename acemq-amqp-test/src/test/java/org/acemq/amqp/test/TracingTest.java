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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.OpenTelemetrySupport;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

/**
 * Spans, and the join between them.
 *
 * <p>Tracing a message system is only worth anything if the handler's span attaches to the
 * publish that caused it. The two run in different threads, usually in different processes,
 * possibly minutes apart, so the link cannot come from thread state: it has to travel with the
 * message. These tests assert that it does.
 */
@DisplayName("tracing")
class TracingTest {

    private InMemorySpanExporter spans;
    private OpenTelemetrySdk sdk;
    private AceMq mq;

    @BeforeEach
    void setUp() {
        spans = InMemorySpanExporter.create();
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spans))
                        .build())
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        sdk.close();
        InMemoryTransport.reset();
    }

    private AceMq connect(String brokerName) {
        mq = AceMq.connect("memory://" + brokerName, OpenTelemetrySupport.telemetry(sdk, "in-memory"));
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @Test
    @Timeout(20)
    void a_publish_produces_a_producer_span_named_after_its_destination() {
        connect("tracing-publish");

        mq.publisher("orders", "order.placed").send("payload");

        await().atMost(Duration.ofSeconds(5)).until(() -> !spans.getFinishedSpanItems().isEmpty());
        SpanData span = spans.getFinishedSpanItems().get(0);

        assertThat(span.getName()).isEqualTo("orders publish");
        assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(attribute(span, "messaging.system")).isEqualTo("in-memory");
        assertThat(attribute(span, "messaging.destination.name")).isEqualTo("orders");
        assertThat(attribute(span, "messaging.operation")).isEqualTo("publish");
        assertThat(attribute(span, "messaging.rabbitmq.destination.routing_key")).isEqualTo("order.placed");
        assertThat(attribute(span, "messaging.acemq.outcome")).isEqualTo("confirmed");
    }

    @Test
    @Timeout(20)
    void a_handler_span_is_a_child_of_the_publish_that_caused_it() {
        connect("tracing-join");
        AtomicInteger handled = new AtomicInteger();

        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> handled.incrementAndGet())) {
            mq.publisher("orders", "order.placed").send("payload");
            await().atMost(Duration.ofSeconds(10)).until(() -> handled.get() == 1);
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> spans.getFinishedSpanItems().size() == 2);
        }

        SpanData publish = spanNamed("orders publish");
        SpanData process = spanNamed("orders.new process");

        // The point of the whole exercise: one trace spanning both sides of the broker.
        assertThat(process.getParentSpanId()).isEqualTo(publish.getSpanId());
        assertThat(process.getTraceId()).isEqualTo(publish.getTraceId());
        assertThat(process.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(attribute(process, "messaging.operation")).isEqualTo("process");
        assertThat(attribute(process, "messaging.acemq.outcome")).isEqualTo("acked");
    }

    @Test
    @Timeout(20)
    void the_trace_travels_in_a_w3c_header_that_other_tools_understand() {
        connect("tracing-header");

        try (MessageConsumer consumer = mq.consume("orders.new", String.class, message -> {
            // traceparent is the W3C name, deliberately not prefixed like the AceMQ headers,
            // so a consumer written against any other tracing library still finds it.
            assertThat(message.headers()).containsKey(AceHeaders.TRACEPARENT);
            assertThat(String.valueOf(message.headers().get(AceHeaders.TRACEPARENT)))
                    .startsWith("00-");
        })) {
            mq.publisher("orders", "order.placed").send("payload");
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> spans.getFinishedSpanItems().size() == 2);
        }
    }

    @Test
    @Timeout(30)
    void a_retry_keeps_the_original_trace_and_records_why() {
        connect("tracing-retry");
        RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofMillis(50)).withJitter(0);

        try (MessageConsumer consumer = mq.consume(
                "orders.new", String.class, ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    throw new IllegalStateException("downstream unavailable");
                })) {

            mq.publisher("orders", "order.placed").send("payload");
            await().atMost(Duration.ofSeconds(15)).until(() -> consumer.deadLettered() == 1);
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> spans.getFinishedSpanItems().size() >= 3);
        }

        List<SpanData> processSpans = spans.getFinishedSpanItems().stream()
                .filter(span -> span.getName().equals("orders.new process"))
                .collect(java.util.stream.Collectors.toList());

        assertThat(processSpans).hasSize(2);
        assertThat(attribute(processSpans.get(0), "messaging.acemq.outcome")).isEqualTo("retried");
        assertThat(attribute(processSpans.get(1), "messaging.acemq.outcome")).isEqualTo("dead_lettered");

        // The retry and the give-up are recorded as events, where an unbounded reason string
        // is welcome; a metric tag would not tolerate it.
        assertThat(processSpans.get(0).getEvents())
                .extracting(event -> event.getName())
                .contains("message.retried");
        assertThat(processSpans.get(1).getEvents())
                .extracting(event -> event.getName())
                .contains("message.dead_lettered");

        // A failing handler's span must carry the exception, or a trace shows a slow span with
        // no explanation.
        assertThat(processSpans.get(0).getStatus().getStatusCode())
                .isEqualTo(io.opentelemetry.api.trace.StatusCode.ERROR);
    }

    private SpanData spanNamed(String name) {
        return spans.getFinishedSpanItems().stream()
                .filter(span -> span.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no span named '" + name + "' among "
                        + spans.getFinishedSpanItems().stream()
                                .map(SpanData::getName)
                                .collect(java.util.stream.Collectors.toList())));
    }

    private static String attribute(SpanData span, String key) {
        Object value = span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(key));
        return value == null ? null : value.toString();
    }
}
