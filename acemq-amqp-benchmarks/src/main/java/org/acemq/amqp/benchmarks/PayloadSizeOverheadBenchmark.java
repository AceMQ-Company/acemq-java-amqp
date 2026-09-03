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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.QueueType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * Where the end-to-end overhead actually comes from.
 *
 * <p>Two measurements sit either side of a gap that needed explaining.
 * {@link PublishOverheadBenchmark} sees AceMQ cost roughly nineteen microseconds more than the
 * raw client per confirmed publish. {@link PublishPathOverheadBenchmark} measures AceMQ's own
 * code at about half a microsecond. Ninety-seven percent of the observed difference is therefore
 * not the library's CPU, and something else has to account for it.
 *
 * <p>The candidate is bytes. AceMQ puts an envelope on every message — an id, a type, a version,
 * a correlation id, a first-seen timestamp — as AMQP headers. On the twenty-six byte payload the
 * other benchmark publishes, those headers are several times larger than the message itself, and
 * a persistent publish pays for every one of them at the broker: parsed, routed, and written to
 * disk before the confirm comes back.
 *
 * <p>If that is the explanation, the overhead is a fixed number of bytes rather than a
 * percentage, and it shrinks as the payload grows. This benchmark tests exactly that, by running
 * the same confirmed-publish pair at payload sizes from smaller than the envelope to far larger
 * than it. A flat percentage across the sizes would refute the hypothesis; a percentage that
 * falls as the payload grows confirms it.
 *
 * <p>Deliberately not part of the nightly gate. It answers a design question — what am I paying
 * for, and when does it matter — rather than guarding against regression, and it takes four times
 * as long as the pair that is gated.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2, warmups = 1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 8, time = 2)
@State(Scope.Benchmark)
public class PayloadSizeOverheadBenchmark {

    private static final String EXCHANGE = "bench";
    private static final String ROUTING_KEY = "bench.publish";
    private static final String QUEUE = "bench.queue";

    /**
     * 32 bytes is smaller than AceMQ's envelope; 8 KB is far larger. If the cost is bytes on
     * the wire, the percentage falls across this range. If it is per-message work, it does not.
     */
    @Param({"32", "1024", "8192"})
    public int payloadBytes;

    private RabbitMQContainer broker;

    private AceMq mq;
    private Publisher<String> acemqPublisher;
    private Publisher<byte[]> bytesPublisher;
    private String payload;

    private Connection rawConnection;
    private Channel rawChannel;
    private AMQP.BasicProperties rawProperties;
    private byte[] rawPayload;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        broker = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4-management"));
        broker.start();

        char[] filler = new char[payloadBytes];
        Arrays.fill(filler, 'x');
        payload = new String(filler);
        rawPayload = payload.getBytes(StandardCharsets.UTF_8);

        mq = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE);
        mq.declareExchange(EXCHANGE, "topic");
        mq.declareQueue(QUEUE, QueueType.CLASSIC, java.util.Collections.emptyMap());
        mq.bind(QUEUE, EXCHANGE, "bench.*");
        acemqPublisher = mq.publisher(EXCHANGE, ROUTING_KEY);
        bytesPublisher = mq.publisher(EXCHANGE, ROUTING_KEY, byte[].class).asBytes();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(broker.getAmqpUrl());
        rawConnection = factory.newConnection("benchmark-raw");
        rawChannel = rawConnection.createChannel();
        rawChannel.confirmSelect();
        rawProperties = MessageProperties.PERSISTENT_BASIC;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (mq != null) {
            mq.close();
        }
        if (rawChannel != null && rawChannel.isOpen()) {
            rawChannel.close();
        }
        if (rawConnection != null && rawConnection.isOpen()) {
            rawConnection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    /**
     * Through AceMQ, publishing a {@code String} — what an application actually writes, and
     * what the gated benchmark measures. Includes a UTF-8 encode of the payload per publish.
     */
    @Benchmark
    public void acemqAtSize(Blackhole blackhole) {
        blackhole.consume(acemqPublisher.send(payload));
    }

    /**
     * Through AceMQ, publishing bytes that are already encoded.
     *
     * <p>This is the like-for-like case, and it exists because the first version of this
     * benchmark did not have one. Comparing a {@code String} publish against a raw client
     * handed a pre-built {@code byte[]} charges AceMQ for an encode the other side never
     * does — invisible on a 26-byte message and several microseconds at 8 KB, which showed up
     * as overhead that grew with the payload and was read, briefly, as a finding about the
     * library. It was a finding about the benchmark.
     */
    @Benchmark
    public void acemqBytesAtSize(Blackhole blackhole) {
        blackhole.consume(bytesPublisher.send(rawPayload));
    }

    /** The same confirmed publish by hand, at the same payload size. */
    @Benchmark
    public void rawClientAtSize(Blackhole blackhole) throws Exception {
        rawChannel.basicPublish(EXCHANGE, ROUTING_KEY, true, rawProperties, rawPayload);
        blackhole.consume(rawChannel.waitForConfirms(10_000));
    }
}
