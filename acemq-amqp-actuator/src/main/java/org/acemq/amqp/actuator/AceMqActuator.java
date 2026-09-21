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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.core.AceMq;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Serves AceMQ's metrics, health and version over HTTP.
 *
 * <pre>{@code
 * AceMq mq = AceMq.connect("amqp://localhost");
 * try (AceMqActuator actuator = AceMqActuator.start(mq)) {
 *     // http://127.0.0.1:9464/acemq-metrics
 * }
 * }</pre>
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@code MicrometerTelemetry} in the core is the right way to get these numbers, and a
 * Spring Boot application needs nothing else: Actuator supplies both halves of a scrape
 * surface -- a {@code MeterRegistry} to record into and an endpoint to serve from -- and the
 * starter wires the library to it. A worker, a daemon or a command-line consumer has neither
 * half, so on the JVM alone, and nowhere else in the family, a non-framework application could
 * not expose its messaging metrics without writing an HTTP endpoint of its own. Go ships
 * {@code actuator}, .NET ships {@code AceMq.Amqp.Diagnostics}, Python ships {@code prometheus},
 * Ruby ships {@code telemetry}; this is Java's.
 *
 * <p>It is an addition to {@code MicrometerTelemetry}, not a second implementation of it. The
 * numbers are recorded by the core exactly as they always were; this owns a
 * {@link PrometheusMeterRegistry} they land in and a small HTTP server that renders it.
 *
 * <h2>When to use it, and when not to</h2>
 *
 * <ul>
 *   <li><strong>Spring Boot:</strong> do not. Add {@code spring-boot-starter-actuator} and
 *       {@code micrometer-registry-prometheus} and scrape {@code /actuator/prometheus}. That
 *       endpoint is managed, secured and configured alongside every other endpoint the
 *       application has, and a second HTTP server on a second port is one more thing to
 *       firewall for no gain.
 *   <li><strong>Quarkus, Micronaut, Helidon, a servlet application:</strong> also no, for the
 *       same reason -- each has a metrics endpoint already. If you want the library's registry
 *       served from it, take {@link #metrics()} or {@link #registry()} and hand it to whatever
 *       you already run, without starting this server at all.
 *   <li><strong>A worker, a batch consumer, a daemon, a CLI:</strong> yes. This is the case
 *       nothing else covers.
 * </ul>
 *
 * <h2>The HTTP server</h2>
 *
 * <p>{@code com.sun.net.httpserver.HttpServer}, from the JDK's {@code jdk.httpserver} module.
 * Despite the package name it is a supported API, present in every JDK since 6, and it costs
 * this module no dependency at all. Embedding Jetty or Netty to serve three text endpoints
 * would put a second HTTP stack inside applications that frequently already have one, and the
 * version fights that follow are a poor trade for a scrape endpoint.
 *
 * <p>It is not an application server and is not meant to face the internet. What a real
 * deployment wants is this bound to loopback or a pod-local address, scraped from the same
 * network namespace by a sidecar or a node agent, with authentication supplied by the platform
 * -- a network policy, a mesh, an ingress -- rather than by a messaging library. A deployment
 * that wants more than that already has an HTTP server, and should serve {@link #metrics()}
 * from it instead.
 *
 * <h2>These endpoints are unauthenticated</h2>
 *
 * <p>Health names the transport and the broker's state; metrics carry queue and exchange names
 * and traffic rates. See {@link ActuatorOptions.Builder#bindAddress(String)}.
 */
public final class AceMqActuator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AceMqActuator.class);

    /** Prometheus refuses a body without the version parameter, so it is spelled out. */
    private static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";

    private static final ObjectMapper JSON = new ObjectMapper();

    /** How long close() waits for a request in flight before stopping the server anyway. */
    private static final int STOP_GRACE_SECONDS = 1;

    private final HttpServer http;
    private final ExecutorService workers;
    private final PrometheusMeterRegistry registry;
    private final ActuatorOptions options;
    private final @Nullable AceMq connection;

    /** Whether close() undoes what start() did to the global registry and closes ours. */
    private final boolean ownsRegistry;

    private final AtomicBoolean closed = new AtomicBoolean();

    private AceMqActuator(
            HttpServer http,
            ExecutorService workers,
            PrometheusMeterRegistry registry,
            ActuatorOptions options,
            boolean ownsRegistry) {
        this.http = http;
        this.workers = workers;
        this.registry = registry;
        this.options = options;
        this.connection = options.connection();
        this.ownsRegistry = ownsRegistry;
    }

    /**
     * Starts an actuator on the default port and paths, reporting on one connection.
     *
     * @param mq the connection health and info describe
     * @return the running actuator
     * @throws AceMqException if the port cannot be bound
     */
    public static AceMqActuator start(AceMq mq) {
        return start(ActuatorOptions.builder()
                .connection(Objects.requireNonNull(mq, "mq must not be null"))
                .build());
    }

    /**
     * Starts an actuator.
     *
     * @param options how to expose it
     * @return the running actuator
     * @throws AceMqException if the port cannot be bound
     */
    public static AceMqActuator start(ActuatorOptions options) {
        Objects.requireNonNull(options, "options must not be null");

        PrometheusMeterRegistry supplied = options.registry();
        boolean ownsRegistry = supplied == null;
        PrometheusMeterRegistry registry = supplied != null
                ? supplied
                : new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        if (ownsRegistry && options.attachToGlobalRegistry()) {
            // Where AceMq.connect(url) sends its metrics when no telemetry was named: the
            // core detects Micrometer and records into the global registry, so attaching
            // here is what joins the numbers the library is already producing to the
            // endpoint that serves them.
            //
            // Start the actuator before the traffic does. Micrometer replays the meters
            // that already exist into a registry added later, but only their registration
            // -- not their accumulated values. Anything counted before this line is counted
            // nowhere this endpoint can see it, and the failure is silent: the series turns
            // up, the scrape parses, and the number is simply low. A test pins that down.
            Metrics.addRegistry(registry);
        }

        HttpServer http;
        try {
            http = HttpServer.create(new InetSocketAddress(options.bindAddress(), options.port()), 0);
        } catch (IOException e) {
            if (ownsRegistry && options.attachToGlobalRegistry()) {
                Metrics.removeRegistry(registry);
            }
            if (ownsRegistry) {
                registry.close();
            }
            throw new AceMqException(
                    "could not listen on " + options.bindAddress() + ":" + options.port()
                            + ". The port may be in use, or the address may not exist on this host.",
                    e);
        }

        // Two threads, and daemons. The JDK's server dispatches on a thread of its own and
        // would otherwise run every handler there in turn, so one slow health check would
        // hold up the scrape queued behind it. Daemons because a metrics endpoint is never
        // the reason a process should refuse to exit.
        ExecutorService workers = Executors.newFixedThreadPool(2, new ActuatorThreads());
        http.setExecutor(workers);

        AceMqActuator actuator = new AceMqActuator(http, workers, registry, options, ownsRegistry);
        http.createContext(options.metricsPath(), exchange -> actuator.serve(exchange, Endpoint.METRICS));
        http.createContext(options.healthPath(), exchange -> actuator.serve(exchange, Endpoint.HEALTH));
        http.createContext(options.infoPath(), exchange -> actuator.serve(exchange, Endpoint.INFO));
        // The catch-all, so a scrape aimed at a mistyped path is told what does exist rather
        // than meeting the JDK server's bare 404 with an empty body.
        http.createContext("/", exchange -> actuator.serve(exchange, Endpoint.UNKNOWN));
        http.start();

        log.info("AceMQ actuator listening on {}", actuator.url());
        return actuator;
    }

    /**
     * The registry the metrics are read from.
     *
     * <p>Hand it to {@code MicrometerSupport.telemetry(registry, transport)} when connecting if
     * you would rather wire the connection to it explicitly than rely on Micrometer's global
     * registry -- or hand it to a framework's own Prometheus endpoint and never start this
     * server at all.
     *
     * @return the registry
     */
    public PrometheusMeterRegistry registry() {
        return registry;
    }

    /**
     * @return the metrics body, byte for byte what a scrape receives
     */
    public String metrics() {
        return registry.scrape();
    }

    /**
     * Where it is listening.
     *
     * <p>Worth logging at start-up, and the only way to learn the port when 0 was asked for.
     *
     * @return the base URL
     */
    public String url() {
        InetSocketAddress address = http.getAddress();
        return "http://" + options.bindAddress() + ":" + address.getPort();
    }

    /**
     * @return the port actually bound, which differs from the one requested when 0 was asked for
     */
    public int port() {
        return http.getAddress().getPort();
    }

    /**
     * @return the three paths being served, for logging at start-up
     */
    public List<String> paths() {
        return Collections.unmodifiableList(
                Arrays.asList(options.metricsPath(), options.healthPath(), options.infoPath()));
    }

    /**
     * Stops the server and releases the port.
     *
     * <p>A registry the actuator created is detached from Micrometer's global registry and
     * closed; one that was supplied is left alone, because the caller owns it. Calling this
     * twice is harmless.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        http.stop(STOP_GRACE_SECONDS);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(STOP_GRACE_SECONDS, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
        if (ownsRegistry) {
            if (options.attachToGlobalRegistry()) {
                // Left attached, a closed actuator would keep collecting into a registry
                // nothing serves -- and a test that starts a second one would then scrape
                // the meters of the first.
                Metrics.removeRegistry(registry);
            }
            registry.close();
        }
        log.info("AceMQ actuator stopped");
    }

    private void serve(HttpExchange exchange, Endpoint endpoint) throws IOException {
        try {
            String method = exchange.getRequestMethod();
            if (!"GET".equals(method) && !"HEAD".equals(method)) {
                respond(exchange, 405, "text/plain; charset=utf-8", "Only GET and HEAD.\n");
                return;
            }
            if (endpoint == Endpoint.METRICS) {
                respond(exchange, 200, PROMETHEUS_CONTENT_TYPE, metrics());
            } else if (endpoint == Endpoint.HEALTH) {
                health(exchange);
            } else if (endpoint == Endpoint.INFO) {
                respond(exchange, 200, JSON_CONTENT_TYPE, info());
            } else {
                respond(exchange, 404, "text/plain; charset=utf-8",
                        "Nothing here. Try " + options.metricsPath() + ", " + options.healthPath() + " or "
                                + options.infoPath() + ".\n");
            }
        } catch (RuntimeException e) {
            // A failure reporting on the application must never take down the application it
            // is reporting on, and a handler that throws out of here kills the connection
            // with no status at all -- which a scraper records as the target being down.
            log.warn("the actuator failed to answer {}", exchange.getRequestURI(), e);
            respond(exchange, 500, "text/plain; charset=utf-8", "The actuator failed to answer.\n");
        } finally {
            exchange.close();
        }
    }

    private void health(HttpExchange exchange) throws IOException {
        ObjectNode root = JSON.createObjectNode();
        AceMq mq = connection;

        if (mq == null) {
            // No verdict to give. UNKNOWN with a 200 rather than DOWN with a 503: nothing has
            // failed, the actuator simply was not told what to watch, and reporting that as
            // down would have an orchestrator restart a healthy process for ever.
            root.put("status", "UNKNOWN");
            respond(exchange, 200, JSON_CONTENT_TYPE, JSON.writeValueAsString(root) + "\n");
            return;
        }

        boolean open = mq.isOpen();
        // Down means the connection is not open, and only that. A broker applying back
        // pressure is reported as up with the reason, matching the Spring Boot health
        // indicator, which documents why: a blocked connection is the broker protecting
        // itself from disk or memory pressure, and an application that fails its own health
        // check for it gets restarted by an orchestrator into the same blocked broker, having
        // thrown away whatever it was holding. .NET keeps degraded on 200 for the same reason.
        root.put("status", open ? "UP" : "DOWN");

        ObjectNode component = JSON.createObjectNode();
        component.put("status", open ? "UP" : "DOWN");
        ObjectNode details = JSON.createObjectNode();
        details.put("transport", mq.transportName());
        details.put("open", open);
        details.put("blocked", mq.isBlocked());
        details.put("inFlight", mq.inFlight());
        Optional<String> reason = mq.blockedReason();
        if (reason.isPresent()) {
            details.put("blockedReason", reason.get());
        }
        component.set("details", details);
        // Nested under a component name, the shape Spring Boot's /actuator/health uses, so a
        // dashboard or a parser written against a Boot service reads this one unchanged.
        ObjectNode components = JSON.createObjectNode();
        components.set("acemq", component);
        root.set("components", components);

        respond(exchange, open ? 200 : 503, JSON_CONTENT_TYPE, JSON.writeValueAsString(root) + "\n");
    }

    private String info() throws IOException {
        ObjectNode root = JSON.createObjectNode();
        root.put("library", "acemq-java-amqp");
        root.put("libraryVersion", libraryVersion());
        if (options.applicationName() != null) {
            root.put("name", options.applicationName());
        }
        if (options.applicationVersion() != null) {
            root.put("version", options.applicationVersion());
        }
        AceMq mq = connection;
        if (mq != null) {
            root.put("transport", mq.transportName());
            // Sorted, because this is read by eye during an incident and by diff between two
            // deployments, and an unordered set makes both harder than they need to be.
            SortedSet<String> names = new TreeSet<>();
            for (Capability capability : mq.capabilities()) {
                names.add(capability.name());
            }
            ArrayNode capabilities = root.putArray("capabilities");
            for (String name : names) {
                capabilities.add(name);
            }
        }
        return JSON.writeValueAsString(root) + "\n";
    }

    /**
     * The library's version, from the jar manifest.
     *
     * <p>"unknown" when there is no manifest to read, which is the case in this project's own
     * tests and in anything running from a directory of classes. Reported rather than hidden:
     * a field that silently disappears is harder to explain than one that says it does not
     * know.
     */
    private static String libraryVersion() {
        Package where = AceMq.class.getPackage();
        String version = where == null ? null : where.getImplementationVersion();
        return version == null ? "unknown" : version;
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        if ("HEAD".equals(exchange.getRequestMethod())) {
            // -1 means "no body", which is what HEAD requires. Sending the length here and no
            // bytes leaves the client waiting for a body that never arrives.
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private enum Endpoint {
        METRICS, HEALTH, INFO, UNKNOWN
    }

    /** Named threads, so a stack dump says which of them belongs to the actuator. */
    private static final class ActuatorThreads implements ThreadFactory {

        private static final AtomicInteger POOLS = new AtomicInteger();

        private final int pool = POOLS.incrementAndGet();
        private final AtomicInteger next = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "acemq-actuator-" + pool + "-" + next.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
