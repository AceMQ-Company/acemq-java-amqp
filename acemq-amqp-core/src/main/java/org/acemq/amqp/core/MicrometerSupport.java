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

import org.acemq.amqp.api.Telemetry;

/**
 * Creates the Micrometer sink, in a class of its own.
 *
 * <p>The separation is load-bearing rather than tidy. A method that mentions a Micrometer type
 * causes the JVM to resolve that type when the method first runs, before any guard inside the
 * method can execute. Putting the reference in a separate class means the class is only loaded
 * at the moment it is used, which is inside a branch that has already established Micrometer is
 * present.
 *
 * <p>Getting this wrong does not degrade gracefully: it throws {@link NoClassDefFoundError} on
 * the first connection in any application that chose not to depend on Micrometer.
 */
public final class MicrometerSupport {

    private MicrometerSupport() {
        throw new AssertionError("MicrometerSupport is a utility and must not be instantiated");
    }

    /**
     * @param transport transport short name
     * @return a sink recording to Micrometer's global registry
     */
    static Telemetry globalRegistry(String transport) {
        return new MicrometerTelemetry(io.micrometer.core.instrument.Metrics.globalRegistry, transport);
    }

    /**
     * Records AceMQ activity into a registry you supply.
     *
     * <p>Preferable to auto-detection, which falls back to Micrometer's global registry: that
     * is process-wide mutable state, so two connections cannot report separately and a test
     * cannot isolate its own measurements.
     *
     * @param registry the registry to record into
     * @param transport transport short name, used as the messaging system tag
     * @return a telemetry sink writing to that registry
     */
    public static Telemetry telemetry(io.micrometer.core.instrument.MeterRegistry registry, String transport) {
        return new MicrometerTelemetry(
                java.util.Objects.requireNonNull(registry, "registry must not be null"), transport);
    }
}
