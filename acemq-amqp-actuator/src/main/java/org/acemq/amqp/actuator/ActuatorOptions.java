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

import java.util.Objects;

import org.acemq.amqp.core.AceMq;
import org.jspecify.annotations.Nullable;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * How an {@link AceMqActuator} is exposed.
 *
 * <p>Every default is the one the Go and .NET libraries use, so an operator moving between
 * services in different languages meets the same port and the same three paths.
 *
 * <pre>{@code
 * AceMqActuator actuator = AceMqActuator.start(ActuatorOptions.builder()
 *         .connection(mq)
 *         .application("orders", "1.4.2")
 *         .build());
 * }</pre>
 */
public final class ActuatorOptions {

    /** Prometheus text format. */
    public static final String DEFAULT_METRICS_PATH = "/acemq-metrics";

    /** Connection health, as JSON. */
    public static final String DEFAULT_HEALTH_PATH = "/acemq-health";

    /** Library version, application version and transport capabilities, as JSON. */
    public static final String DEFAULT_INFO_PATH = "/acemq-info";

    /** The OpenTelemetry convention for a Prometheus endpoint, and what Go and .NET use. */
    public static final int DEFAULT_PORT = 9464;

    private final @Nullable AceMq connection;
    private final String bindAddress;
    private final int port;
    private final String metricsPath;
    private final String healthPath;
    private final String infoPath;
    private final @Nullable String applicationName;
    private final @Nullable String applicationVersion;
    private final @Nullable PrometheusMeterRegistry registry;
    private final boolean attachToGlobalRegistry;

    private ActuatorOptions(Builder builder) {
        this.connection = builder.connection;
        this.bindAddress = builder.bindAddress;
        this.port = builder.port;
        this.metricsPath = builder.metricsPath;
        this.healthPath = builder.healthPath;
        this.infoPath = builder.infoPath;
        this.applicationName = builder.applicationName;
        this.applicationVersion = builder.applicationVersion;
        this.registry = builder.registry;
        this.attachToGlobalRegistry = builder.attachToGlobalRegistry;
    }

    /** @return a builder holding the defaults */
    public static Builder builder() {
        return new Builder();
    }

    /** @return the connection health and info report on, absent when none was supplied */
    public @Nullable AceMq connection() {
        return connection;
    }

    /** @return the interface to listen on */
    public String bindAddress() {
        return bindAddress;
    }

    /** @return the port to listen on, or 0 to take whatever the operating system gives */
    public int port() {
        return port;
    }

    /** @return the path serving the Prometheus text format */
    public String metricsPath() {
        return metricsPath;
    }

    /** @return the path serving the health report */
    public String healthPath() {
        return healthPath;
    }

    /** @return the path serving library and transport information */
    public String infoPath() {
        return infoPath;
    }

    /** @return what the application calls itself, for {@code /acemq-info} */
    public @Nullable String applicationName() {
        return applicationName;
    }

    /** @return the application's own version, for {@code /acemq-info} */
    public @Nullable String applicationVersion() {
        return applicationVersion;
    }

    /** @return the registry to scrape, or absent to have the actuator create one */
    public @Nullable PrometheusMeterRegistry registry() {
        return registry;
    }

    /** @return whether a registry the actuator created is added to Micrometer's global one */
    public boolean attachToGlobalRegistry() {
        return attachToGlobalRegistry;
    }

    /** Collects the settings. */
    public static final class Builder {

        private @Nullable AceMq connection;
        private String bindAddress = "127.0.0.1";
        private int port = DEFAULT_PORT;
        private String metricsPath = DEFAULT_METRICS_PATH;
        private String healthPath = DEFAULT_HEALTH_PATH;
        private String infoPath = DEFAULT_INFO_PATH;
        private @Nullable String applicationName;
        private @Nullable String applicationVersion;
        private @Nullable PrometheusMeterRegistry registry;
        private boolean attachToGlobalRegistry = true;

        private Builder() {
        }

        /**
         * The connection {@code /acemq-health} reports on and {@code /acemq-info} describes.
         *
         * <p>Optional. Without it the actuator still serves metrics, because a process may
         * want its numbers scraped before it has connected to anything, and health then
         * answers {@code UNKNOWN} rather than inventing a verdict.
         *
         * @param connection the connection to report on
         * @return this builder
         */
        public Builder connection(AceMq connection) {
            this.connection = Objects.requireNonNull(connection, "connection must not be null");
            return this;
        }

        /**
         * The interface to listen on. Loopback by default.
         *
         * <p><strong>These endpoints are unauthenticated.</strong> They name queues, report
         * broker state and show traffic rates, which is more than an anonymous caller on the
         * network should learn about a service. Binding to {@code 0.0.0.0} publishes all of it
         * to anything that can reach the port.
         *
         * <p>Leave it on loopback and let the scraper reach it from the same host, the same
         * pod, or a sidecar; where it genuinely has to be reachable from elsewhere, put
         * something in front that authenticates. This library will not do that for you, and
         * says so rather than implying otherwise by shipping a token check nobody configures.
         *
         * @param bindAddress the address to bind
         * @return this builder
         */
        public Builder bindAddress(String bindAddress) {
            this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress must not be null");
            return this;
        }

        /**
         * The port to listen on.
         *
         * @param port the port, or 0 to take an ephemeral one -- which is what a test wants,
         *     because a fixed port makes two tests that both bind it fail depending on order
         * @return this builder
         */
        public Builder port(int port) {
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("port must be between 0 and 65535, not " + port);
            }
            this.port = port;
            return this;
        }

        /**
         * Where the metrics are served.
         *
         * <p>Namespaced by default so it cannot collide with an application's own
         * {@code /metrics}. Change it if you like, but the scrape configuration has to change
         * with it: a Prometheus job pointed at the old path reports the target as down rather
         * than as misconfigured, and those read very differently at three in the morning.
         *
         * @param metricsPath the path, starting with a slash
         * @return this builder
         */
        public Builder metricsPath(String metricsPath) {
            this.metricsPath = path(metricsPath, "metricsPath");
            return this;
        }

        /**
         * @param healthPath where the health report is served, starting with a slash
         * @return this builder
         */
        public Builder healthPath(String healthPath) {
            this.healthPath = path(healthPath, "healthPath");
            return this;
        }

        /**
         * @param infoPath where the library and transport information is served
         * @return this builder
         */
        public Builder infoPath(String infoPath) {
            this.infoPath = path(infoPath, "infoPath");
            return this;
        }

        /**
         * What the application calls itself, reported by {@code /acemq-info}.
         *
         * <p>So a running instance can say what it is without anybody having to ask the
         * deployment that started it.
         *
         * @param name the application's name
         * @param version the application's version
         * @return this builder
         */
        public Builder application(String name, String version) {
            this.applicationName = Objects.requireNonNull(name, "name must not be null");
            this.applicationVersion = Objects.requireNonNull(version, "version must not be null");
            return this;
        }

        /**
         * A registry to scrape instead of one the actuator creates.
         *
         * <p>For the explicit wiring the observability guide prefers: build the registry, hand
         * it to {@code MicrometerSupport.telemetry(registry, transport)} when connecting, and
         * pass the same one here. Nothing then depends on Micrometer's global registry, which
         * is process-wide mutable state that two connections cannot report into separately.
         *
         * <p>A registry supplied this way is never added to the global registry and is never
         * closed by {@link AceMqActuator#close()}: the actuator did not create it and has no
         * business deciding when it ends.
         *
         * @param registry the registry to serve
         * @return this builder
         */
        public Builder registry(PrometheusMeterRegistry registry) {
            this.registry = Objects.requireNonNull(registry, "registry must not be null");
            return this;
        }

        /**
         * Whether a registry the actuator creates is added to Micrometer's global registry.
         *
         * <p>True by default, and that default is what makes {@code AceMqActuator.start(mq)}
         * work with no other wiring: {@code AceMq.connect(url)} with no telemetry argument
         * detects Micrometer and records into the global registry, so attaching to it is what
         * connects the numbers the library is already producing to the endpoint serving them.
         *
         * <p>Start the actuator before the traffic. Micrometer replays the meters that already
         * exist into a registry added later, but only their registration, not their
         * accumulated values: anything counted before the actuator started is counted nowhere
         * this endpoint can see. Nothing breaks visibly -- the series appears, the scrape
         * parses, the number is simply low -- which is why it is said here rather than left to
         * be discovered. Wire {@link #registry(PrometheusMeterRegistry)} explicitly if the
         * ordering cannot be arranged.
         *
         * <p>Turn it off to keep this endpoint to the meters you route here yourself. It has
         * no effect on a registry passed to {@link #registry(PrometheusMeterRegistry)}, which
         * is never attached.
         *
         * @param attachToGlobalRegistry whether to attach
         * @return this builder
         */
        public Builder attachToGlobalRegistry(boolean attachToGlobalRegistry) {
            this.attachToGlobalRegistry = attachToGlobalRegistry;
            return this;
        }

        /** @return the options */
        public ActuatorOptions build() {
            if (metricsPath.equals(healthPath) || metricsPath.equals(infoPath) || healthPath.equals(infoPath)) {
                // Two paths the same means one endpoint silently shadows another, and which
                // one wins is a detail of the dispatch order rather than anything anyone chose.
                throw new IllegalArgumentException(
                        "the three paths must differ: metrics=" + metricsPath + " health=" + healthPath + " info="
                                + infoPath);
            }
            return new ActuatorOptions(this);
        }

        private static String path(String value, String what) {
            Objects.requireNonNull(value, what + " must not be null");
            if (!value.startsWith("/")) {
                // The JDK's HttpServer silently never matches a context that does not start
                // with a slash, so this would otherwise show up as a 404 with no explanation.
                throw new IllegalArgumentException(what + " must start with a slash, not " + value);
            }
            return value;
        }
    }
}
