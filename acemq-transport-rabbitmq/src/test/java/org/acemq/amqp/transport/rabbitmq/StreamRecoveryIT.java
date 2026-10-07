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
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.StreamConsumer;
import org.acemq.amqp.core.StreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * A stream reader whose connection is lost half-way through, and comes back.
 *
 * <p>The client re-subscribes a recovered consumer with the arguments it was first given. For a
 * stream that is the original {@code x-stream-offset}: a reader that began at {@code first}
 * replayed everything, and one that began at {@code next} skipped what was appended while it
 * was away. The connection is closed the way an operator or a broker restart would, through the
 * management API.
 */
class StreamRecoveryIT {

    private static final int BEFORE = 500;
    private static final int DURING = 50;
    private static final int PREFETCH = 10;

    private static RabbitMQContainer broker;

    @BeforeAll
    static void startBroker() {
        broker = new RabbitMQContainer(BrokerImage.current());
        broker.withPluginsEnabled("rabbitmq_stream");
        broker.start();
    }

    @AfterAll
    static void stopBroker() {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    @Timeout(180)
    void a_reader_from_first_neither_replays_nor_skips_after_a_reconnect() throws Exception {
        survivesAReconnect(StreamReader::fromFirst, true);
    }

    @Test
    @Timeout(180)
    void a_reader_from_next_does_not_lose_what_was_appended_while_it_was_away() throws Exception {
        survivesAReconnect(StreamReader::fromNext, false);
    }

    private void survivesAReconnect(Function<StreamReader<String>, StreamReader<String>> start, boolean writeFirst)
            throws Exception {
        String stream = "recovery.log." + UUID.randomUUID();
        List<String> seen = new CopyOnWriteArrayList<>();
        try (AceMq mq = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE)) {
            mq.declareStream(stream, Duration.ofHours(1), 50_000_000L);
            if (writeFirst) {
                write(mq, stream, 0, BEFORE);
            }
            try (StreamConsumer reader = start.apply(mq.stream(stream, String.class)).prefetch(PREFETCH)
                    .consume(message -> {
                        seen.add(message.payload());
                        Thread.sleep(5);
                    })) {
                if (!writeFirst) {
                    // A reader from "next" has to be attached before anything is written.
                    Thread.sleep(1_000);
                    write(mq, stream, 0, BEFORE);
                }
                await().atMost(Duration.ofSeconds(60)).until(() -> seen.size() >= BEFORE / 2);
                closeEveryConnection();

                // Appended while the reader is away; a separate connection, since its own is
                // still waiting out the recovery interval.
                try (AceMq other = AceMq.connect(broker.getAmqpUrl(), Telemetry.NONE)) {
                    write(other, stream, BEFORE, DURING);
                }

                await().atMost(Duration.ofSeconds(90))
                        .alias("all " + (BEFORE + DURING) + " entries seen")
                        .conditionEvaluationListener(c -> {
                            if (c.getRemainingTimeInMS() < 1_000) {
                                System.err.println("seen=" + seen.size() + " distinct=" + new HashSet<>(seen).size()
                                        + " running=" + reader.isRunning() + " " + reader + " " + reader.stoppedBy());
                            }
                        })
                        .until(() -> new HashSet<>(seen).size() >= BEFORE + DURING);
                Thread.sleep(2_000);
                assertThat(new HashSet<>(seen)).hasSize(BEFORE + DURING);
                // In flight when the connection went: up to a prefetch of them comes again,
                // as a queue would redeliver them. Anything beyond that is a replay.
                assertThat(seen.size()).isBetween(BEFORE + DURING, BEFORE + DURING + PREFETCH);
                assertThat(reader.isRunning()).isTrue();
            }
        }
    }

    private static void write(AceMq mq, String stream, int from, int count) {
        for (int i = from; i < from + count; i++) {
            mq.publisher("", stream, String.class).asText().send("event-" + i);
        }
    }

    /** Closes every client connection the broker has, as an operator would from the UI. */
    static void closeEveryConnection() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        String auth = "Basic " + Base64.getEncoder().encodeToString(
                (broker.getAdminUsername() + ":" + broker.getAdminPassword()).getBytes(StandardCharsets.UTF_8));
        List<String> names = new ArrayList<>();
        // The management API's connection list lags the broker by a statistics interval.
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).until(() -> {
            String body = http.send(HttpRequest.newBuilder(URI.create(broker.getHttpUrl() + "/api/connections"))
                    .header("Authorization", auth).build(), HttpResponse.BodyHandlers.ofString()).body();
            Matcher m = Pattern.compile("\"name\":\"([^\"]*->[^\"]*)\"").matcher(body);
            names.clear();
            while (m.find()) {
                names.add(m.group(1));
            }
            return !names.isEmpty();
        });
        for (String name : names) {
            String path = URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20");
            int status = http.send(HttpRequest.newBuilder(URI.create(broker.getHttpUrl() + "/api/connections/" + path))
                    .header("Authorization", auth).DELETE().build(), HttpResponse.BodyHandlers.discarding())
                    .statusCode();
            assertThat(status).isIn(204, 404);
        }
    }
}
