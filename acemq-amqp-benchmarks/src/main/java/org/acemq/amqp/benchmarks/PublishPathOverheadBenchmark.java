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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.TransportConnection;
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

/**
 * What AceMQ's own code costs, measured where it can actually be seen.
 *
 * <p>{@link PublishOverheadBenchmark} answers "does this matter in practice" and cannot answer
 * "by how much". It times a full confirmed publish to a real broker — around 450 microseconds,
 * of which AceMQ's own work is roughly twenty. Asking a measurement of a 450 microsecond
 * network round trip to resolve a twenty microsecond component is asking it to find four
 * percent inside noise that is worth ten, and it cannot: the nightly reports intervals like
 * [-3.0%, +16.5%], which contain both zero and the budget.
 *
 * <p>So this benchmark removes the broker instead of fighting it. Every case publishes into the
 * in-process transport, which settles a message in nanoseconds, and the difference between the
 * cases is the library layer and nothing else. Same sink, same JVM, same payload; what changes
 * is how much of AceMQ sits in front of it.
 *
 * <ul>
 *   <li>{@link #transportOnly} — an {@link OutboundMessage} built by hand and handed straight to
 *       the transport. This is the floor: the least any library could do to publish a byte array.
 *   <li>{@link #aceMqPreEncoded} — the same bytes through {@code Publisher}, so the difference
 *       from the floor is the envelope, the AceMQ headers, the interceptor chain, the telemetry
 *       hooks and the confirm bookkeeping.
 *   <li>{@link #aceMqTypedPayload} — an application object through the same publisher, so the
 *       difference from the previous case is serialization.
 * </ul>
 *
 * <p>Two subtractions, each answering a question the end-to-end benchmark cannot:
 *
 * <pre>
 *   aceMqPreEncoded  - transportOnly    = envelope, headers, bookkeeping
 *   aceMqTypedPayload - aceMqPreEncoded = JSON serialization
 * </pre>
 *
 * <p>Nothing here is a substitute for the end-to-end pair. A number from this benchmark says
 * what the library costs in CPU; it says nothing about what that is as a fraction of a real
 * publish, which depends entirely on the broker and the network in front of it. Both benchmarks
 * are needed, and neither answers the other's question.
 *
 * <p>No queue is bound, and both paths publish with unroutable messages allowed. That is
 * deliberate: a bound queue would accumulate several million messages over a measurement run and
 * turn this into a benchmark of a growing data structure. What is being timed is the path up to
 * and including the transport's settlement of the message, which is identical either way.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, warmups = 1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
public class PublishPathOverheadBenchmark {

    private static final String BROKER = "memory://publish-path-overhead";
    private static final String EXCHANGE = "bench";
    private static final String ROUTING_KEY = "bench.publish";
    private static final String JSON = "{\"id\":\"o-1\",\"total\":42.00}";
    private static final byte[] PAYLOAD = JSON.getBytes(StandardCharsets.UTF_8);

    private AceMq mq;
    private Publisher<String> preEncoded;
    private Publisher<Order> typed;

    private TransportConnection transport;

    @Setup(Level.Trial)
    public void setUp() {
        InMemoryTransport.reset();

        // Unroutable allowed on both paths: no queue is bound, so a mandatory publish
        // would fail rather than measure anything.
        PublishOptions options = PublishOptions.defaults().allowUnroutable();

        mq = AceMq.connect(BROKER, Telemetry.NONE);
        mq.declareExchange(EXCHANGE, "topic");
        preEncoded = mq.publisher(EXCHANGE, ROUTING_KEY, String.class, options);
        typed = mq.publisher(EXCHANGE, ROUTING_KEY, Order.class, options);

        // A second connection to the same named broker shares its state, so the floor
        // case settles messages through exactly the same code the publisher reaches.
        transport = new InMemoryTransport().connect(ConnectionConfig.url(BROKER).build());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (mq != null) {
            mq.close();
        }
        if (transport != null && transport.isOpen()) {
            transport.close();
        }
    }

    /** The floor: a message built by hand, handed straight to the transport. */
    @Benchmark
    public void transportOnly(Blackhole blackhole) {
        blackhole.consume(transport.send(OutboundMessage.body(PAYLOAD)
                .exchange(EXCHANGE)
                .routingKey(ROUTING_KEY)
                .contentType("application/json")
                .allowUnroutable()
                .build()));
    }

    /** The same bytes through AceMQ: envelope, headers, interceptors, bookkeeping. */
    @Benchmark
    public void aceMqPreEncoded(Blackhole blackhole) {
        blackhole.consume(preEncoded.send(JSON));
    }

    /** An application object through AceMQ: everything above, plus serialization. */
    @Benchmark
    public void aceMqTypedPayload(Blackhole blackhole) {
        blackhole.consume(typed.send(new Order("o-1", 42.00)));
    }

    /** A small application type, so the typed case pays a realistic serialization cost. */
    public static final class Order {

        private final String id;
        private final double total;

        public Order(String id, double total) {
            this.id = id;
            this.total = total;
        }

        public String getId() {
            return id;
        }

        public double getTotal() {
            return total;
        }
    }
}
