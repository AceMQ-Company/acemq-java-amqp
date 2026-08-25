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
package org.acemq.amqp.api;

import java.time.Duration;
import java.util.Map;

/**
 * Where AceMQ reports what it is doing.
 *
 * <p>Implemented by the Micrometer and OpenTelemetry bindings, and by a no-op used when
 * neither is present. The interface is deliberately small and free of any vendor type, so the
 * core never imports a metrics library and an application that wants neither pays nothing.
 *
 * <p>Implementations must not throw. Instrumentation that can break the thing it observes is
 * worse than no instrumentation, so the core does not defend against it and expects
 * implementations to swallow their own failures.
 */
public interface Telemetry {

    /** A telemetry sink that records nothing. */
    Telemetry NONE = new Telemetry() {

        @Override
        public Scope publishStarted(String exchange, String routingKey, Envelope envelope) {
            return Scope.NONE;
        }

        @Override
        public Scope consumeStarted(String queue, Envelope envelope) {
            return Scope.NONE;
        }

        @Override
        public void messageRetried(String queue, Envelope envelope, Duration delay) {
            // nothing to record
        }

        @Override
        public void messageDeadLettered(String queue, Envelope envelope, String reason) {
            // nothing to record
        }

        @Override
        public Map<String, String> propagationHeaders() {
            return java.util.Collections.emptyMap();
        }
    };

    /**
     * Called immediately before a message is published.
     *
     * @param exchange target exchange
     * @param routingKey routing key
     * @param envelope the envelope being sent
     * @return a scope that must be closed with the outcome
     */
    Scope publishStarted(String exchange, String routingKey, Envelope envelope);

    /**
     * Called immediately before a handler runs.
     *
     * @param queue queue the delivery came from
     * @param envelope the delivery's envelope
     * @return a scope that must be closed with the outcome
     */
    Scope consumeStarted(String queue, Envelope envelope);

    /**
     * Records that a message was sent for another attempt.
     *
     * @param queue source queue
     * @param envelope envelope of the message being retried
     * @param delay how long it will wait
     */
    void messageRetried(String queue, Envelope envelope, Duration delay);

    /**
     * Records that a message was given up on.
     *
     * @param queue source queue
     * @param envelope envelope of the message
     * @param reason why it was given up on
     */
    void messageDeadLettered(String queue, Envelope envelope, String reason);

    /**
     * Headers to add to an outgoing message so a trace continues across the broker.
     *
     * <p>Returns W3C trace context when tracing is active, and nothing otherwise. Called while
     * the publish scope is open, so the context reflects the span being created.
     *
     * @return headers to merge into the message, never {@code null}
     */
    Map<String, String> propagationHeaders();

    /**
     * An operation in progress.
     *
     * <p>Closing without reporting an outcome is treated as a failure, since an operation that
     * neither succeeded nor failed is almost always one that threw.
     */
    interface Scope extends AutoCloseable {

        /** A scope that records nothing. */
        Scope NONE = new Scope() {

            @Override
            public void outcome(String outcome) {
                // nothing to record
            }

            @Override
            public void failed(Throwable failure) {
                // nothing to record
            }

            @Override
            public void close() {
                // nothing to release
            }
        };

        /**
         * Records how the operation ended.
         *
         * @param outcome one of the outcome values in {@link MetricNames}
         */
        void outcome(String outcome);

        /**
         * Records that the operation threw.
         *
         * @param failure what went wrong
         */
        void failed(Throwable failure);

        @Override
        void close();
    }
}
