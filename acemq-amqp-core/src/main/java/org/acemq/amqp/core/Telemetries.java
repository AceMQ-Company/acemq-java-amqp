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
package org.acemq.amqp.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Telemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the telemetry sink for a connection.
 *
 * <p>Micrometer and OpenTelemetry are optional dependencies, so neither may be present at
 * runtime. Presence is checked by attempting to load one class from each and catching the
 * failure. That is the only reliable test: an application that excluded the dependency has no
 * class to load, and asking about it any other way means importing it.
 *
 * <p>When neither is available the core uses {@link Telemetry#NONE}, whose methods are empty
 * and whose scopes allocate nothing. Instrumentation that an application did not ask for
 * should cost it nothing.
 *
 * <p>No optional type is named anywhere in this class, not even in a method signature. A
 * signature is part of a class's descriptors and is resolved when the class is loaded, so a
 * single convenience factory taking a {@code MeterRegistry} here would make this class
 * unloadable in any application without Micrometer. The typed factories live in
 * {@code MicrometerSupport} and {@code OpenTelemetrySupport}, which are only loaded by callers
 * that already have the dependency.
 */
public final class Telemetries {

    private static final Logger log = LoggerFactory.getLogger(Telemetries.class);

    private Telemetries() {
        throw new AssertionError("Telemetries is a utility and must not be instantiated");
    }

    /**
     * @param transport transport short name, used as the messaging system tag
     * @return a sink combining whichever providers are present, or a no-op when none are
     */
    public static Telemetry autoDetect(String transport) {
        List<Telemetry> found = new ArrayList<>();

        // Construction goes through a class of its own per provider, and no optional type is
        // named here. Naming one — even as a local variable — makes the JVM resolve it when
        // this method first runs, before the guard on the line above can execute, so the first
        // connection in an application without that dependency dies with NoClassDefFoundError.
        if (isPresent("io.micrometer.core.instrument.MeterRegistry")) {
            found.add(MicrometerSupport.globalRegistry(transport));
            log.debug("Micrometer detected: metrics will be recorded to the global registry");
        }

        if (isPresent("io.opentelemetry.api.OpenTelemetry")) {
            found.add(OpenTelemetrySupport.global(transport));
            log.debug("OpenTelemetry detected: spans will be emitted");
        }

        if (found.isEmpty()) {
            log.debug("no telemetry provider on the classpath; instrumentation is disabled");
            return Telemetry.NONE;
        }
        return found.size() == 1 ? found.get(0) : new CompositeTelemetry(found);
    }

    /**
     * Combines several sinks into one.
     *
     * @param delegates sinks to fan out to
     * @return a sink reporting to all of them
     */
    @SuppressWarnings("ReferenceEquality") // Telemetry.NONE is a singleton; identity is the point.
    public static Telemetry composite(Telemetry... delegates) {
        List<Telemetry> list = new ArrayList<>();
        for (Telemetry delegate : delegates) {
            // Identity rather than equals: NONE is a specific instance meaning
            // "record nothing", and another sink that merely compares equal to it
            // would still be a sink worth calling.
            if (delegate != null && delegate != Telemetry.NONE) {
                list.add(delegate);
            }
        }
        if (list.isEmpty()) {
            return Telemetry.NONE;
        }
        return list.size() == 1 ? list.get(0) : new CompositeTelemetry(list);
    }

    private static boolean isPresent(String className) {
        try {
            Class.forName(className, false, Telemetries.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /**
     * Fans out to several providers.
     *
     * <p>A provider that throws must not break the operation being measured, nor stop the
     * other providers recording it, so failures are logged once at debug and swallowed.
     */
    private static final class CompositeTelemetry implements Telemetry {

        private final List<Telemetry> delegates;

        CompositeTelemetry(List<Telemetry> delegates) {
            this.delegates = delegates;
        }

        @Override
        public Scope publishStarted(String exchange, String routingKey, Envelope envelope) {
            List<Scope> scopes = new ArrayList<>(delegates.size());
            for (Telemetry delegate : delegates) {
                try {
                    scopes.add(delegate.publishStarted(exchange, routingKey, envelope));
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to start a publish scope", e);
                }
            }
            return new CompositeScope(scopes);
        }

        @Override
        public Scope consumeStarted(String queue, Envelope envelope) {
            List<Scope> scopes = new ArrayList<>(delegates.size());
            for (Telemetry delegate : delegates) {
                try {
                    scopes.add(delegate.consumeStarted(queue, envelope));
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to start a consume scope", e);
                }
            }
            return new CompositeScope(scopes);
        }

        @Override
        public Scope requestStarted(String destination, Envelope envelope) {
            List<Scope> scopes = new ArrayList<>(delegates.size());
            for (Telemetry delegate : delegates) {
                try {
                    scopes.add(delegate.requestStarted(destination, envelope));
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to start a request scope", e);
                }
            }
            return new CompositeScope(scopes);
        }

        @Override
        public void outboxPublished(String exchange, String routingKey, Duration lag) {
            for (Telemetry delegate : delegates) {
                try {
                    delegate.outboxPublished(exchange, routingKey, lag);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record an outbox publish", e);
                }
            }
        }

        @Override
        public void outboxFailed(String exchange, String routingKey, String reason) {
            for (Telemetry delegate : delegates) {
                try {
                    delegate.outboxFailed(exchange, routingKey, reason);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record an outbox failure", e);
                }
            }
        }

        @Override
        public void pipelineRunFinished(String pipeline, String step, String outcome, Duration age) {
            for (Telemetry delegate : delegates) {
                try {
                    delegate.pipelineRunFinished(pipeline, step, outcome, age);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record a pipeline run", e);
                }
            }
        }

        @Override
        public void messageRetried(String queue, Envelope envelope, Duration delay) {
            for (Telemetry delegate : delegates) {
                try {
                    delegate.messageRetried(queue, envelope, delay);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record a retry", e);
                }
            }
        }

        @Override
        public void messageDeadLettered(String queue, Envelope envelope, String reason) {
            for (Telemetry delegate : delegates) {
                try {
                    delegate.messageDeadLettered(queue, envelope, reason);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record a dead letter", e);
                }
            }
        }

        @Override
        public Map<String, String> propagationHeaders() {
            Map<String, String> headers = new LinkedHashMap<>();
            for (Telemetry delegate : delegates) {
                try {
                    headers.putAll(delegate.propagationHeaders());
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to supply propagation headers", e);
                }
            }
            return headers;
        }
    }

    /** Closes several scopes as one, in reverse order so nesting unwinds correctly. */
    private static final class CompositeScope implements Telemetry.Scope {

        private final List<Telemetry.Scope> scopes;

        CompositeScope(List<Telemetry.Scope> scopes) {
            this.scopes = scopes;
        }

        @Override
        public void outcome(String outcome) {
            for (Telemetry.Scope scope : scopes) {
                try {
                    scope.outcome(outcome);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record an outcome", e);
                }
            }
        }

        @Override
        public void failed(Throwable failure) {
            for (Telemetry.Scope scope : scopes) {
                try {
                    scope.failed(failure);
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to record a failure", e);
                }
            }
        }

        @Override
        public void close() {
            for (int i = scopes.size() - 1; i >= 0; i--) {
                try {
                    scopes.get(i).close();
                } catch (RuntimeException e) {
                    log.debug("telemetry provider failed to close a scope", e);
                }
            }
        }
    }
}
