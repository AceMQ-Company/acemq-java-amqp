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
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * What AceMQ costs on top of the RabbitMQ client it wraps.
 *
 * <p>This is the benchmark behind the five percent overhead budget in doc 10, and the claim in
 * the README. It is the one measurement that cannot be faked with an in-memory transport,
 * because the thing being compared is a real publish against a real broker.
 *
 * <p>Both cases run in the same JMH invocation, against the same broker, in the same JVM, with
 * the same confirm semantics: publish, wait for the broker to confirm, return. That matters
 * more than the absolute numbers. Continuous-integration hardware is shared and noisy, so an
 * absolute figure from it means little, but a ratio taken from two measurements minutes apart
 * on the same machine cancels most of that noise and is worth gating on.
 *
 * <p>The raw case is written the way the budget requires: it is what an application would have
 * to write by hand to get the same guarantee AceMQ gives by default. A raw publish without
 * {@code waitForConfirms} would be faster and would be measuring something else entirely,
 * namely the cost of not knowing whether the message arrived.
 */
/*
 * Three forks and ten iterations, not one and five.
 *
 * The gate enforces a 5% budget, and it can only do that if the measurement resolves better
 * than 5%. One fork of five iterations of a network round-trip on a shared runner produced
 * +/-7%, which cannot tell a breach from a clean run -- and reported one every night from
 * 27 August. Forks matter more than iterations here: each one is a fresh JVM, so
 * fork-to-fork spread includes the JIT and allocation luck that repeated iterations inside
 * one JVM never sample, and it is that spread the interval needs to contain.
 *
 * Cost: two benchmarks, three forks, 5x2s warmup and 10x3s measurement each, so about four
 * minutes plus JVM starts, against a job timeout of sixty.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, warmups = 1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 3)
@State(Scope.Benchmark)
public class PublishOverheadBenchmark {

    private static final byte[] PAYLOAD = "{\"id\":\"o-1\",\"total\":42.00}".getBytes(StandardCharsets.UTF_8);
    private static final String EXCHANGE = "bench";
    private static final String ROUTING_KEY = "bench.publish";
    private static final String QUEUE = "bench.queue";

    private RabbitMQContainer broker;

    private AceMq mq;
    private Publisher<String> acemqPublisher;

    private Connection rawConnection;
    private Channel rawChannel;
    private AMQP.BasicProperties rawProperties;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        broker = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4-management"));
        broker.start();

        // AceMQ, configured as an application would leave it: confirms on.
        mq = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE);
        mq.declareExchange(EXCHANGE, "topic");
        mq.declareQueue(QUEUE, QueueType.CLASSIC, java.util.Collections.emptyMap());
        mq.bind(QUEUE, EXCHANGE, "bench.*");
        acemqPublisher = mq.publisher(EXCHANGE, ROUTING_KEY);

        // The raw client, doing the same job by hand: confirm mode, persistent
        // delivery, mandatory publish, and a wait for the broker to confirm.
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

    /** One confirmed publish through AceMQ. */
    @Benchmark
    public void acemqConfirmedPublish(Blackhole blackhole) {
        blackhole.consume(acemqPublisher.send("{\"id\":\"o-1\",\"total\":42.00}"));
    }

    /** The same confirmed publish written by hand against the RabbitMQ client. */
    @Benchmark
    public void rawClientConfirmedPublish(Blackhole blackhole) throws Exception {
        rawChannel.basicPublish(EXCHANGE, ROUTING_KEY, true, rawProperties, PAYLOAD);
        blackhole.consume(rawChannel.waitForConfirms(10_000));
    }
}
