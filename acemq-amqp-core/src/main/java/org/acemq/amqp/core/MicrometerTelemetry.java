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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Telemetry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

/**
 * Records AceMQ activity into a Micrometer registry.
 *
 * <p>Only reached when Micrometer is on the classpath and a registry has been supplied, which
 * is why the core can depend on it optionally: an application without Micrometer never loads
 * this class and never pays for it.
 *
 * <p>Tag cardinality is chosen carefully. Queue, exchange and message type are bounded by the
 * topology, so they are safe. Message identifiers and routing keys containing identifiers are
 * not, which is why the message id appears on spans, where a high-cardinality value is useful,
 * and never on a metric, where it would multiply the time series until the backend fell over.
 */
final class MicrometerTelemetry implements Telemetry {

    private final MeterRegistry registry;
    private final String transport;
    private final AtomicInteger inFlight = new AtomicInteger();

    MicrometerTelemetry(MeterRegistry registry, String transport) {
        this.registry = registry;
        this.transport = transport;
    }

    @Override
    public Scope publishStarted(String exchange, String routingKey, Envelope envelope) {
        Tags tags = Tags.of(
                MetricNames.TAG_EXCHANGE, exchange == null ? "" : exchange,
                MetricNames.TAG_ROUTING_KEY, routingKey == null ? "" : routingKey,
                MetricNames.TAG_MESSAGE_TYPE, envelope.type(),
                MetricNames.TAG_TRANSPORT, transport);
        return new MeterScope(MetricNames.PUBLISH_DURATION, MetricNames.PUBLISH_TOTAL, tags, null);
    }

    @Override
    public Scope consumeStarted(String queue, Envelope envelope) {
        Tags tags = Tags.of(
                MetricNames.TAG_QUEUE, queue,
                MetricNames.TAG_MESSAGE_TYPE, envelope.type(),
                MetricNames.TAG_TRANSPORT, transport);

        DistributionSummary.builder(MetricNames.CONSUME_ATTEMPTS)
                .description("which delivery attempt a message was on when it was handled")
                .tags(tags)
                .register(registry)
                .record(envelope.attempt());

        inFlight.incrementAndGet();
        registry.gauge(MetricNames.CONSUME_IN_FLIGHT, tags, inFlight);
        return new MeterScope(MetricNames.CONSUME_DURATION, MetricNames.CONSUME_TOTAL, tags, inFlight);
    }

    @Override
    public void messageRetried(String queue, Envelope envelope, Duration delay) {
        Counter.builder(MetricNames.RETRIED_TOTAL)
                .description("messages sent to a retry queue for another attempt")
                .tags(Tags.of(
                        MetricNames.TAG_QUEUE, queue,
                        MetricNames.TAG_MESSAGE_TYPE, envelope.type(),
                        MetricNames.TAG_TRANSPORT, transport))
                .register(registry)
                .increment();
    }

    @Override
    public void messageDeadLettered(String queue, Envelope envelope, String reason) {
        // The reason is not a tag. It contains exception messages, which are unbounded, and a
        // metrics backend meets unbounded tags by falling over. It belongs on the span and in
        // the message header, both of which tolerate it.
        Counter.builder(MetricNames.DEAD_LETTERED_TOTAL)
                .description("messages sent to a dead-letter or parking queue")
                .tags(Tags.of(
                        MetricNames.TAG_QUEUE, queue,
                        MetricNames.TAG_MESSAGE_TYPE, envelope.type(),
                        MetricNames.TAG_TRANSPORT, transport))
                .register(registry)
                .increment();
    }

    @Override
    public Map<String, String> propagationHeaders() {
        // Metrics carry no context across the broker; tracing does that.
        return Collections.emptyMap();
    }

    /** Times one operation and counts how it ended. */
    private final class MeterScope implements Scope {

        private final String durationMetric;
        private final String countMetric;
        private final Tags tags;
        private final AtomicInteger gauge;
        private final Timer.Sample sample;
        private String outcome;
        private boolean closed;

        MeterScope(String durationMetric, String countMetric, Tags tags, AtomicInteger gauge) {
            this.durationMetric = durationMetric;
            this.countMetric = countMetric;
            this.tags = tags;
            this.gauge = gauge;
            this.sample = Timer.start(registry);
        }

        @Override
        public void outcome(String outcome) {
            this.outcome = outcome;
        }

        @Override
        public void failed(Throwable failure) {
            if (this.outcome == null) {
                this.outcome = MetricNames.OUTCOME_FAILED;
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (gauge != null) {
                gauge.decrementAndGet();
            }
            // An operation that closed without saying how it went almost certainly threw.
            String result = outcome == null ? MetricNames.OUTCOME_FAILED : outcome;
            Tags withOutcome = tags.and(MetricNames.TAG_OUTCOME, result);

            sample.stop(Timer.builder(durationMetric)
                    .description("how long the operation took")
                    .tags(withOutcome)
                    .publishPercentileHistogram()
                    .register(registry));
            Counter.builder(countMetric).tags(withOutcome).register(registry).increment();
        }
    }
}
