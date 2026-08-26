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
 * Creates the OpenTelemetry sink, in a class of its own.
 *
 * <p>Separated for the same reason as {@link MicrometerSupport}: a reference to an optional
 * type inside a method is resolved when that method runs, so the guard has to live in a
 * different class from the reference it guards.
 */
public final class OpenTelemetrySupport {

    private OpenTelemetrySupport() {
        throw new AssertionError("OpenTelemetrySupport is a utility and must not be instantiated");
    }

    /**
     * @param transport transport short name
     * @return a sink emitting spans through the global OpenTelemetry instance
     */
    static Telemetry global(String transport) {
        return new OpenTelemetryTelemetry(io.opentelemetry.api.GlobalOpenTelemetry.get(), transport);
    }

    /**
     * Emits spans through an OpenTelemetry instance you supply.
     *
     * @param openTelemetry the instance to use
     * @param transport transport short name, used as the messaging system attribute
     * @return a telemetry sink emitting spans
     */
    public static Telemetry telemetry(io.opentelemetry.api.OpenTelemetry openTelemetry, String transport) {
        return new OpenTelemetryTelemetry(
                java.util.Objects.requireNonNull(openTelemetry, "openTelemetry must not be null"), transport);
    }
}
