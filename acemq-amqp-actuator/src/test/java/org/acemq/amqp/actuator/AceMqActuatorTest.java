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
package org.acemq.amqp.actuator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MicrometerSupport;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * The actuator against a real connection, over real HTTP.
 *
 * <p>Nothing here mocks {@link AceMq}. The point of the health and info endpoints is that they
 * report what the connection actually says, and a mock answers whatever the test asked it to,
 * which would leave the wiring between them untested -- exactly the seam that broke when the
 * Spring Boot indicator and this one disagreed about what "blocked" means.
 *
 * <p>Every actuator binds port 0. A fixed port makes two tests that both want it fail depending
 * on which ran first, and on a busy machine it makes them fail for no reason at all.
 */
class AceMqActuatorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private @org.jspecify.annotations.Nullable AceMqActuator actuator;
    private @org.jspecify.annotations.Nullable AceMq mq;

    @AfterEach
    void stop() {
        if (actuator != null) {
            actuator.close();
            actuator = null;
        }
        if (mq != null) {
            mq.close();
            mq = null;
        }
        // The global registry is process-wide, so a leak here follows the next test into its
        // own assertions. close() should have removed ours; this proves it did.
        assertThat(Metrics.globalRegistry.getRegistries()).isEmpty();
    }

    private AceMqActuator start(ActuatorOptions.Builder builder) {
        actuator = AceMqActuator.start(builder.port(0).build());
        return actuator;
    }

    private AceMq connect(String broker, PrometheusMeterRegistry registry) {
        mq = AceMq.connect("memory://" + broker, MicrometerSupport.telemetry(registry, "in-memory"));
        return topology(mq);
    }

    /** Somewhere for a message to go, so publishing produces a metric rather than an error. */
    private static AceMq topology(AceMq mq) {
        mq.declareExchange("orders", "topic");
        // Classic by name: the in-memory transport refuses a quorum queue and says so, which
        // is the right answer and not the one this test is about.
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    @Test
    void servesTheThreePathsEveryLibraryServes() {
        AceMqActuator started = start(ActuatorOptions.builder());

        assertThat(started.paths())
                .containsExactly("/acemq-metrics", "/acemq-health", "/acemq-info");
        assertThat(ActuatorOptions.DEFAULT_PORT).isEqualTo(9464);
    }

    @Test
    void metricsComeOutInTheFormatPrometheusAsksFor() throws IOException {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        AceMq connection = connect("actuator-metrics", registry);
        connection.publisher("orders", "order.placed").send("payload");

        AceMqActuator started = start(ActuatorOptions.builder().connection(connection).registry(registry));
        Response response = get(started, "/acemq-metrics");

        assertThat(response.status).isEqualTo(200);
        // The version parameter is not decoration: Prometheus refuses a body without it.
        assertThat(response.contentType).isEqualTo("text/plain; version=0.0.4; charset=utf-8");
        // Recorded by MicrometerTelemetry in the core, not by anything in this module.
        assertThat(response.body).contains("# TYPE acemq_publish_total counter");
        assertThat(response.body).contains("acemq_publish_duration_seconds_bucket");
        assertThat(response.body).containsPattern("acemq_publish_total\\{[^}]*exchange=\"orders\"[^}]*} 1\\.0");
        assertThat(response.body).isEqualTo(started.metrics());
    }

    @Test
    void anOpenConnectionIsUpAndSaysWhatItIs() throws IOException {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        AceMq connection = connect("actuator-health", registry);

        AceMqActuator started = start(ActuatorOptions.builder().connection(connection).registry(registry));
        Response response = get(started, "/acemq-health");

        assertThat(response.status).isEqualTo(200);
        JsonNode body = JSON.readTree(response.body);
        assertThat(body.get("status").asText()).isEqualTo("UP");

        JsonNode details = body.get("components").get("acemq").get("details");
        assertThat(details.get("transport").asText()).isEqualTo("in-memory");
        assertThat(details.get("open").asBoolean()).isTrue();
        assertThat(details.get("blocked").asBoolean()).isFalse();
        assertThat(details.has("inFlight")).isTrue();
    }

    @Test
    void aClosedConnectionIsDownAndAnswers503() throws IOException {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        AceMq connection = connect("actuator-health-down", registry);
        AceMqActuator started = start(ActuatorOptions.builder().connection(connection).registry(registry));

        connection.close();

        Response response = get(started, "/acemq-health");

        // 503 rather than 200 with a sad body: a readiness probe reads the status code and
        // nothing else, so a down connection reported as 200 keeps the instance in rotation.
        assertThat(response.status).isEqualTo(503);
        JsonNode body = JSON.readTree(response.body);
        assertThat(body.get("status").asText()).isEqualTo("DOWN");
        assertThat(body.get("components").get("acemq").get("details").get("open").asBoolean()).isFalse();
    }

    @Test
    void withNoConnectionHealthSaysItDoesNotKnowRatherThanGuessing() throws IOException {
        AceMqActuator started = start(ActuatorOptions.builder());

        Response response = get(started, "/acemq-health");

        // Not 503. Nothing has failed; the actuator was simply not told what to watch, and
        // reporting that as down would have an orchestrator restart a healthy process for ever.
        assertThat(response.status).isEqualTo(200);
        assertThat(JSON.readTree(response.body).get("status").asText()).isEqualTo("UNKNOWN");
    }

    @Test
    void infoNamesTheLibraryTheApplicationAndWhatTheTransportCanDo() throws IOException {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        AceMq connection = connect("actuator-info", registry);

        AceMqActuator started = start(ActuatorOptions.builder()
                .connection(connection)
                .registry(registry)
                .application("orders", "1.4.2"));
        Response response = get(started, "/acemq-info");

        assertThat(response.status).isEqualTo(200);
        JsonNode body = JSON.readTree(response.body);
        assertThat(body.get("library").asText()).isEqualTo("acemq-java-amqp");
        // Read from the jar manifest, which a build running out of target/classes has not
        // got. The field is always there; the value says so rather than vanishing.
        assertThat(body.get("libraryVersion").asText()).isNotEmpty();
        assertThat(body.get("name").asText()).isEqualTo("orders");
        assertThat(body.get("version").asText()).isEqualTo("1.4.2");
        assertThat(body.get("transport").asText()).isEqualTo("in-memory");
        assertThat(body.get("capabilities").isArray()).isTrue();

        List<String> capabilities = new java.util.ArrayList<>();
        body.get("capabilities").forEach(node -> capabilities.add(node.asText()));
        assertThat(capabilities).isSorted();
    }

    @Test
    void aMistypedPathIsToldWhatDoesExist() throws IOException {
        AceMqActuator started = start(ActuatorOptions.builder());

        Response response = get(started, "/metrics");

        assertThat(response.status).isEqualTo(404);
        // A scrape aimed at the wrong path otherwise meets an empty body and the operator
        // has to read the library to find out what the right one is.
        assertThat(response.body).contains("/acemq-metrics", "/acemq-health", "/acemq-info");
    }

    @Test
    void anythingButGetAndHeadIsRefused() throws IOException {
        AceMqActuator started = start(ActuatorOptions.builder());

        Response response = send(started, "/acemq-metrics", "DELETE");

        assertThat(response.status).isEqualTo(405);
    }

    @Test
    void headAnswersWithNoBody() throws IOException {
        AceMqActuator started = start(ActuatorOptions.builder());

        Response response = send(started, "/acemq-info", "HEAD");

        assertThat(response.status).isEqualTo(200);
        assertThat(response.body).isEmpty();
    }

    @Test
    void anActuatorStartedLateSeesTheSeriesButNotWhatWasCountedBeforeIt() throws IOException {
        // The zero-configuration route, and the one with a trap in it worth pinning down.
        // AceMq.connect with no telemetry argument detects Micrometer and records into the
        // global registry; the actuator attaches its own registry to that composite
        // afterwards. Micrometer replays the meters that already exist into a registry added
        // later, so the series turn up -- but it replays their *registration*, not their
        // accumulated values, so a message published before the actuator started is counted
        // nowhere this endpoint can see.
        //
        // Worth a test of its own because the failure is silent and plausible-looking: the
        // series is present, the scrape parses, the dashboard draws, and the number is simply
        // lower than the truth by however long the actuator took to start.
        mq = topology(AceMq.connect("memory://actuator-late"));
        mq.publisher("orders", "order.placed").send("before the actuator existed");

        AceMqActuator started = start(ActuatorOptions.builder().connection(mq));

        assertThat(get(started, "/acemq-metrics").body)
                .containsPattern("acemq_publish_total\\{[^}]*exchange=\"orders\"[^}]*} 0\\.0");

        mq.publisher("orders", "order.placed").send("after it existed");

        assertThat(get(started, "/acemq-metrics").body)
                .containsPattern("acemq_publish_total\\{[^}]*exchange=\"orders\"[^}]*} 1\\.0");
    }

    @Test
    void aRegistryTheActuatorCreatedIsDetachedAndClosedOnTheWayOut() {
        AceMqActuator started = start(ActuatorOptions.builder());
        PrometheusMeterRegistry created = started.registry();

        assertThat(Metrics.globalRegistry.getRegistries()).contains(created);

        started.close();
        actuator = null;

        // Left attached, a stopped actuator keeps collecting into a registry nothing serves,
        // and the next one to start would scrape the previous one's meters.
        assertThat(Metrics.globalRegistry.getRegistries()).doesNotContain(created);
        assertThat(created.isClosed()).isTrue();
    }

    @Test
    void aRegistryTheCallerSuppliedIsLeftAloneOnTheWayOut() {
        PrometheusMeterRegistry mine = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        AceMqActuator started = start(ActuatorOptions.builder().registry(mine));
        assertThat(started.registry()).isSameAs(mine);
        // Never attached: a caller wiring a registry explicitly did so to stay out of the
        // global one, and quietly joining them would mirror every other library's meters in.
        assertThat(Metrics.globalRegistry.getRegistries()).doesNotContain(mine);

        started.close();
        actuator = null;

        assertThat(mine.isClosed()).isFalse();
        mine.close();
    }

    @Test
    void closingTwiceIsHarmless() {
        AceMqActuator started = start(ActuatorOptions.builder());

        started.close();
        started.close();
        actuator = null;

        assertThat(Metrics.globalRegistry.getRegistries()).isEmpty();
    }

    @Test
    void aPortThatCannotBeBoundSaysSoAndLeavesNothingBehind() {
        AceMqActuator first = start(ActuatorOptions.builder());
        int taken = first.port();

        assertThatThrownBy(() -> AceMqActuator.start(ActuatorOptions.builder().port(taken).build()))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("could not listen on");

        // The failed start must not leave its registry in the global one, or a process that
        // retries on another port ends up scraping through two registries for ever.
        assertThat(Metrics.globalRegistry.getRegistries()).hasSize(1);
    }

    @Test
    void theEndpointIsReachableAtTheUrlItReports() throws IOException {
        AceMqActuator started = start(ActuatorOptions.builder());

        assertThat(started.url()).isEqualTo("http://127.0.0.1:" + started.port());
        assertThat(get(started, "/acemq-info").status).isEqualTo(200);
    }

    @Test
    void thePathsCanBeMovedWhenSomethingElseOwnsTheDefaults() throws IOException {
        AceMqActuator started = start(ActuatorOptions.builder()
                .metricsPath("/internal/metrics")
                .healthPath("/internal/health")
                .infoPath("/internal/info"));

        assertThat(get(started, "/internal/info").status).isEqualTo(200);
        assertThat(get(started, "/acemq-info").status).isEqualTo(404);
    }

    private Response get(AceMqActuator actuator, String path) throws IOException {
        return send(actuator, path, "GET");
    }

    private Response send(AceMqActuator actuator, String path, String method) throws IOException {
        URL url = new URL(actuator.url() + path);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod(method);
        http.setConnectTimeout(5000);
        http.setReadTimeout(5000);
        try {
            int status = http.getResponseCode();
            InputStream stream = status < 400 ? http.getInputStream() : http.getErrorStream();
            String body = stream == null ? "" : read(stream);
            return new Response(status, http.getHeaderField("Content-Type"), body);
        } finally {
            http.disconnect();
        }
    }

    private static String read(InputStream stream) throws IOException {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = stream.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        stream.close();
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static final class Response {

        private final int status;
        private final String contentType;
        private final String body;

        private Response(int status, String contentType, String body) {
            this.status = status;
            this.contentType = contentType == null ? "" : contentType;
            this.body = body;
        }
    }
}
