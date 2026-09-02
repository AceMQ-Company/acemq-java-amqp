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

    // The methods below are default no-ops, and will stay that way. This interface is
    // implemented by applications with their own monitoring, and every abstract method added
    // after the fact breaks all of them at compile time for a signal they did not ask for.
    // A sink that wants one of these overrides it; a sink written before it existed keeps
    // working. The four above predate the rule and cannot be changed now without doing the
    // exact thing this comment forbids.

    /**
     * Called once per request/reply round trip, around the wait for the answer.
     *
     * <p>The publish and the reply's delivery are each already traced, and the trace context in
     * the message headers chains them. What none of that produces is a single span whose
     * duration is what the caller actually experienced — the question "how long did asking take"
     * has no answer in a picture made of two unrelated hops. This is that span, and the two hops
     * become its children.
     *
     * @param destination where the request was sent
     * @param envelope the request's envelope
     * @return a scope closed with {@code answered} or {@code timed_out}
     */
    default Scope requestStarted(String destination, Envelope envelope) {
        return Scope.NONE;
    }

    /**
     * Called by the outbox relay when a record reaches the broker.
     *
     * <p>The lag is the number that matters and the one nothing else can see: a row committed
     * and not yet published is a message that exists, is owed to somebody, and appears in no
     * queue depth anywhere. A relay that has stopped looks exactly like a system with nothing
     * to send until this is measured.
     *
     * @param exchange where the record was published
     * @param routingKey the record's routing key
     * @param lag how long the record sat between being committed and being published
     */
    default void outboxPublished(String exchange, String routingKey, Duration lag) {
        // no-op
    }

    /**
     * Called by the outbox relay when a record could not be published.
     *
     * @param exchange where the record was bound for
     * @param routingKey the record's routing key
     * @param reason why it failed
     */
    default void outboxFailed(String exchange, String routingKey, String reason) {
        // no-op
    }

    /**
     * Called when a message leaves a pipeline, whether it finished or stopped early.
     *
     * <p>A pipeline's steps are each traced as ordinary hops, so the stages are visible. What is
     * not visible from them is the run: how long the whole thing took, and how often it ends
     * before the last step. Both are properties of the run rather than of any step in it.
     *
     * @param pipeline the pipeline's name
     * @param step the step it left at, which is the last one when it completed
     * @param outcome {@code completed} or {@code ended_early}
     * @param age how long the message had existed when it left
     */
    default void pipelineRunFinished(String pipeline, String step, String outcome, Duration age) {
        // no-op
    }

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
