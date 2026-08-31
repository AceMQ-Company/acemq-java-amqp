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
package org.acemq.amqp.transport.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.StreamConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * Streams against a real broker, because there is nowhere else to test them.
 *
 * <p>Replay, retention and independent consumer positions are broker behaviour. An in-memory
 * imitation of an offset log would pass and would be evidence about the imitation, so there is
 * none: these run against RabbitMQ or they do not run.
 */
@DisplayName("a stream")
class StreamIT {

    private static RabbitMQContainer broker;

    private AceMq mq;
    private String stream;

    @BeforeAll
    static void startBroker() {
        broker = new RabbitMQContainer(BrokerImage.current());
        // Streams are a plugin, and off unless asked for.
        broker.withPluginsEnabled("rabbitmq_stream");
        broker.start();
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.stop();
        }
    }

    @BeforeEach
    void connect() {
        mq = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE);
        // A stream per test. They are not emptied by being read, so sharing one would let each
        // test see everything the ones before it wrote.
        stream = "orders.log." + UUID.randomUUID();
        mq.declareStream(stream, Duration.ofHours(1), 20_000_000L);
    }

    private void write(int count) {
        for (int i = 0; i < count; i++) {
            mq.publisher("", stream, String.class).asText().send("event-" + i);
        }
    }

    private void closeConnection() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
    }

    @Nested
    @DisplayName("reading it")
    class Reading {

        @Test
        @Timeout(120)
        void from_the_beginning_replays_everything_written_before_the_consumer_existed() {
            List<String> seen = new CopyOnWriteArrayList<>();
            try {
                write(10);

                // The consumer is created after every message was written, and still sees them
                // all. On a queue there would be nothing left to read.
                try (StreamConsumer consumer = mq.stream(stream, String.class)
                        .fromFirst()
                        .consume(message -> seen.add(message.payload()))) {

                    await().atMost(Duration.ofSeconds(60)).until(() -> seen.size() == 10);
                    assertThat(seen).first().isEqualTo("event-0");
                    assertThat(seen).last().isEqualTo("event-9");
                    assertThat(consumer.handled()).isEqualTo(10);
                    assertThat(consumer.lastHandledOffset()).hasValue(9L);
                }
            } finally {
                closeConnection();
            }
        }

        @Test
        @Timeout(120)
        void from_an_offset_resumes_where_a_previous_consumer_stopped() {
            List<String> first = new CopyOnWriteArrayList<>();
            List<String> second = new CopyOnWriteArrayList<>();
            try {
                write(10);

                long resumeFrom;
                // Stopping is how a consumer's position is pinned down. Letting it run and
                // reading the offset afterwards races: it carries on while the test looks.
                try (StreamConsumer consumer = mq.stream(stream, String.class)
                        .fromFirst()
                        .prefetch(1)
                        .consume(message -> {
                            if (first.size() == 5) {
                                throw new IllegalStateException("stopping after five");
                            }
                            first.add(message.payload());
                        })) {
                    await().atMost(Duration.ofSeconds(60)).until(() -> !consumer.isRunning());
                    resumeFrom = consumer.lastHandledOffset().orElseThrow(AssertionError::new);
                    assertThat(resumeFrom).isEqualTo(4L);
                }

                // The offset a consumer recorded is the thing to persist, and one past it is
                // where the next one starts. This is the whole resume story.
                try (StreamConsumer consumer = mq.stream(stream, String.class)
                        .fromOffset(resumeFrom + 1)
                        .consume(message -> second.add(message.payload()))) {

                    await().atMost(Duration.ofSeconds(60)).until(() -> second.size() == 5);
                    assertThat(second).containsExactly("event-5", "event-6", "event-7", "event-8", "event-9");
                    assertThat(second).doesNotContainAnyElementsOf(first);
                    assertThat(consumer.queue()).isEqualTo(stream);
                }
            } finally {
                closeConnection();
            }
        }

        @Test
        @Timeout(120)
        void two_consumers_hold_their_own_positions() {
            List<String> fromStart = new CopyOnWriteArrayList<>();
            List<String> fromNow = new CopyOnWriteArrayList<>();
            try {
                write(5);

                try (StreamConsumer replaying = mq.stream(stream, String.class)
                        .fromFirst()
                        .consume(message -> fromStart.add(message.payload()));
                        StreamConsumer live = mq.stream(stream, String.class)
                                .fromNext()
                                .consume(message -> fromNow.add(message.payload()))) {

                    await().atMost(Duration.ofSeconds(60)).until(() -> fromStart.size() == 5);

                    write(2);
                    await().atMost(Duration.ofSeconds(60)).until(() -> fromNow.size() == 2);

                    // Reading did not consume anything: one saw all seven, the other only the
                    // two written after it attached. On a queue they would have shared them.
                    await().atMost(Duration.ofSeconds(60)).until(() -> fromStart.size() == 7);
                    assertThat(replaying.handled()).isEqualTo(7);
                    assertThat(live.handled()).isEqualTo(2);
                }
            } finally {
                closeConnection();
            }
        }
    }

    @Nested
    @DisplayName("when a handler fails")
    class Failure {

        @Test
        @Timeout(120)
        void the_reader_stops_because_a_gap_in_a_projection_is_invisible_once_it_exists() {
            AtomicInteger seen = new AtomicInteger();
            try {
                write(5);

                try (StreamConsumer consumer = mq.stream(stream, String.class)
                        .fromFirst()
                        .prefetch(1)
                        .consume(message -> {
                            if (seen.incrementAndGet() == 3) {
                                throw new IllegalStateException("this one is bad");
                            }
                        })) {

                    await().atMost(Duration.ofSeconds(60)).until(() -> !consumer.isRunning());

                    assertThat(consumer.handled()).isEqualTo(2);
                    assertThat(consumer.failed()).isEqualTo(1);
                    assertThat(consumer.skipped()).isZero();
                    assertThat(consumer.stoppedBy()).isPresent();
                    assertThat(consumer.stoppedBy().get()).hasMessageContaining("this one is bad");
                }
            } finally {
                closeConnection();
            }
        }

        @Test
        @Timeout(120)
        void skipping_carries_on_and_counts_what_nothing_processed() {
            AtomicInteger seen = new AtomicInteger();
            try {
                write(5);

                try (StreamConsumer consumer = mq.stream(stream, String.class)
                        .fromFirst()
                        .prefetch(1)
                        .skipFailures()
                        .consume(message -> {
                            if (seen.incrementAndGet() == 3) {
                                throw new IllegalStateException("this one is bad");
                            }
                        })) {

                    await().atMost(Duration.ofSeconds(60)).until(() -> consumer.handled() == 4);

                    // Nothing else in the system will say that offset 2 went unprocessed. This
                    // counter is the only record, which is why the default is to stop.
                    assertThat(consumer.skipped()).isEqualTo(1);
                    assertThat(consumer.isRunning()).isTrue();
                }
            } finally {
                closeConnection();
            }
        }
    }

    @Nested
    @DisplayName("what it refuses")
    class Refusals {

        @Test
        @Timeout(120)
        void a_stream_consumer_cannot_be_created_without_a_prefetch() {
            try {
                assertThatThrownBy(() -> mq.stream(stream, String.class).prefetch(0))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("only backpressure a stream has");
            } finally {
                closeConnection();
            }
        }

        @Test
        @Timeout(120)
        void the_broker_reports_that_it_supports_streams() {
            try {
                assertThat(mq.supports(Capability.STREAMS)).isTrue();
            } finally {
                closeConnection();
            }
        }

    }
}
