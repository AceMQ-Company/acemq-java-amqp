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
package org.acemq.amqp.benchmarks;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MicrometerSupport;
import org.acemq.amqp.core.OpenTelemetrySupport;
import org.acemq.amqp.core.Telemetries;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;

/**
 * What instrumentation costs.
 *
 * <p>Doc 10 requires the price of telemetry to be a published number rather than a promise.
 * Every case here is the same publish through the same engine, differing only in where it
 * reports, so the difference between them is the instrumentation and nothing else.
 *
 * <p>The in-memory transport is used deliberately. A real broker's network round trip and
 * confirm latency are measured in milliseconds and would swamp a difference measured in
 * microseconds, turning this into a benchmark of RabbitMQ. These numbers are engine overhead;
 * the separate budget against the raw client needs a real broker and is measured elsewhere.
 */
/*
 * Three forks, for the same reason as the publish benchmark: an interval that does not
 * contain the fork-to-fork spread is not an interval. These are in-process and microsecond-
 * scale, so each iteration gathers plenty of samples in a second and the cost is about three
 * minutes for the four cases.
 *
 * Last night's run is the argument: publishWithoutTelemetry came out at 7.0 +/- 33.5 us/op
 * from one fork -- an error bar five times the number it decorates, published as a figure
 * about what instrumentation costs.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, warmups = 1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
public class TelemetryOverheadBenchmark {

    private AceMq withoutTelemetry;
    private AceMq withMetrics;
    private AceMq withTracing;
    private AceMq withBoth;

    private Publisher<String> plain;
    private Publisher<String> metered;
    private Publisher<String> traced;
    private Publisher<String> meteredAndTraced;

    private SimpleMeterRegistry registry;
    private OpenTelemetrySdk sdk;

    @Setup(Level.Trial)
    public void setUp() {
        registry = new SimpleMeterRegistry();
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().build())
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

        Telemetry micrometer = MicrometerSupport.telemetry(registry, "in-memory");
        Telemetry openTelemetry = OpenTelemetrySupport.telemetry(sdk, "in-memory");

        withoutTelemetry = connect("bench-none", Telemetry.NONE);
        withMetrics = connect("bench-metrics", micrometer);
        withTracing = connect("bench-traces", openTelemetry);
        withBoth = connect("bench-both", Telemetries.composite(micrometer, openTelemetry));

        plain = withoutTelemetry.publisher("orders", "order.placed");
        metered = withMetrics.publisher("orders", "order.placed");
        traced = withTracing.publisher("orders", "order.placed");
        meteredAndTraced = withBoth.publisher("orders", "order.placed");
    }

    private static AceMq connect(String broker, Telemetry telemetry) {
        AceMq mq = AceMq.connect("memory://" + broker, telemetry);
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        withoutTelemetry.close();
        withMetrics.close();
        withTracing.close();
        withBoth.close();
        sdk.close();
        registry.close();
        InMemoryTransport.reset();
    }

    /** The baseline: the engine with instrumentation switched off. */
    @Benchmark
    public void publishWithoutTelemetry(Blackhole blackhole) {
        blackhole.consume(plain.send("payload"));
    }

    /** Metrics only. The difference from the baseline is what Micrometer costs. */
    @Benchmark
    public void publishWithMetrics(Blackhole blackhole) {
        blackhole.consume(metered.send("payload"));
    }

    /** Tracing only, including writing the W3C trace header onto the message. */
    @Benchmark
    public void publishWithTracing(Blackhole blackhole) {
        blackhole.consume(traced.send("payload"));
    }

    /** Both, which is what a production deployment usually runs. */
    @Benchmark
    public void publishWithMetricsAndTracing(Blackhole blackhole) {
        blackhole.consume(meteredAndTraced.send("payload"));
    }

    /** Building an envelope and deriving the next attempt, which every retry pays for. */
    @Benchmark
    public void envelopeAndNextAttempt(Blackhole blackhole) {
        Envelope envelope = Envelope.of("order.placed")
                .correlationId("flow-1")
                .header("tenant", "acme")
                .build();
        blackhole.consume(envelope.nextAttempt());
    }
}
