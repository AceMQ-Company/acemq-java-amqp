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

/**
 * The metric and span names AceMQ emits.
 *
 * <p>These are public API in the same sense the envelope headers are. A dashboard, an alert
 * rule and a service-level objective are all written against these strings, and every language
 * port emits the same ones so that a polyglot estate can be watched on one dashboard. Renaming
 * one silently breaks alerting in a way no compiler will catch, so they are frozen and changed
 * only in a major version.
 */
public final class MetricNames {

    /** Time from calling send to the broker confirming, tagged with the outcome. */
    public static final String PUBLISH_DURATION = "acemq.publish.duration";

    /** Messages published, tagged with the outcome. */
    public static final String PUBLISH_TOTAL = "acemq.publish.total";

    /** Time spent in a handler, tagged with the outcome. */
    public static final String CONSUME_DURATION = "acemq.consume.duration";

    /** Deliveries handled, tagged with the outcome. */
    public static final String CONSUME_TOTAL = "acemq.consume.total";

    /** Which attempt a delivery was, so a rising distribution shows a struggling dependency. */
    public static final String CONSUME_ATTEMPTS = "acemq.consume.attempts";

    /** Deliveries currently in a handler, bounded by prefetch times concurrency. */
    public static final String CONSUME_IN_FLIGHT = "acemq.consume.in.flight";

    /** Messages sent to a retry queue. */
    public static final String RETRIED_TOTAL = "acemq.messages.retried.total";

    /** Messages sent to a dead-letter or parking queue, tagged with {@link #TAG_OUTCOME}. */
    public static final String DEAD_LETTERED_TOTAL = "acemq.messages.dead.lettered.total";

    /**
     * Messages that could not be moved to a dead-letter or parking queue, tagged with
     * {@link #TAG_QUEUE} and {@link #TAG_TARGET}.
     *
     * <p>The counter that separates two failures which look identical from anywhere else. A
     * message dead-lettered normally leaves the source queue and appears in the dead-letter
     * queue; a message whose dead-letter queue was never declared leaves the source queue and
     * appears nowhere, because the republish failed and the delivery went back to the broker
     * with nothing to catch it. Queue depths show the same picture in both cases — one queue
     * going down — and only this number says which of the two happened.
     *
     * <p>Worth an alert at any value above zero. It does not rise under load or during a
     * deploy; it rises when a topology is wrong.
     */
    public static final String SET_ASIDE_FAILED = "acemq.messages.set.aside.failed";

    /**
     * Retries that had to wait in the consumer because the rung queue they belonged in is not on
     * the broker, tagged with {@link #TAG_QUEUE} and {@link #TAG_RUNG}.
     *
     * <p>Nothing breaks: the message is still retried and the wait still happens. What is lost
     * is the reason the rung exists — a consumer restarted mid-wait turns a five-minute backoff
     * into no backoff at all — and the only other sign of it is a log line on a path nobody
     * watches.
     */
    public static final String RUNG_MISSING = "acemq.retry.rung.missing";

    /** Round trip of a request/reply call, as the caller experienced it. */
    public static final String REQUEST_DURATION = "acemq.request.duration";

    /** Request/reply calls, tagged with {@link #TAG_OUTCOME}. */
    public static final String REQUEST_TOTAL = "acemq.request.total";

    /**
     * How long an outbox record waited between being committed and being published.
     *
     * <p>The one number that reveals a stopped relay. A committed, unpublished row is a message
     * that exists and is owed to somebody, and it appears in no queue depth anywhere.
     */
    public static final String OUTBOX_LAG = "acemq.outbox.lag";

    /** Outbox records the relay has handled, tagged with {@link #TAG_OUTCOME}. */
    public static final String OUTBOX_TOTAL = "acemq.outbox.total";

    /** How long a message had existed when it left a pipeline. */
    public static final String PIPELINE_RUN_DURATION = "acemq.pipeline.run.duration";

    /** Pipeline runs that finished, tagged with {@link #TAG_OUTCOME} and {@link #TAG_STEP}. */
    public static final String PIPELINE_RUN_TOTAL = "acemq.pipeline.run.total";

    // ---------- tag keys ----------

    /** Exchange a message was published to; empty string for the default exchange. */
    public static final String TAG_EXCHANGE = "exchange";

    /** Routing key used, or the queue name when publishing without an exchange. */
    public static final String TAG_ROUTING_KEY = "routing.key";

    /** Queue a delivery came from. */
    public static final String TAG_QUEUE = "queue";

    /** Transport short name, such as {@code rabbitmq}. */
    public static final String TAG_TRANSPORT = "transport";

    /** Logical message type from the envelope. */
    public static final String TAG_MESSAGE_TYPE = "message.type";

    /**
     * Where a message was being set aside to when that failed: the dead-letter queue or the
     * parking lot. Bounded by the topology, so it is safe as a tag.
     */
    public static final String TAG_TARGET = "target";

    /**
     * The retry rung a wait belonged in, such as {@code orders.new.retry.40s}.
     *
     * <p>Bounded by the retry policy, which names a fixed handful of rungs, so it is safe as a
     * tag. It is what makes {@link #RUNG_MISSING} actionable: the counter says a rung is not
     * there and this says which one to declare.
     */
    public static final String TAG_RUNG = "rung";

    /**
     * What happened. One of {@code confirmed}, {@code unroutable}, {@code failed} for a
     * publish; {@code acked}, {@code retried}, {@code dead_lettered}, {@code parked},
     * {@code rejected} for a delivery.
     */
    public static final String TAG_OUTCOME = "outcome";

    // ---------- outcome values ----------

    public static final String OUTCOME_CONFIRMED = "confirmed";
    public static final String OUTCOME_UNROUTABLE = "unroutable";
    public static final String OUTCOME_FAILED = "failed";
    public static final String OUTCOME_ACKED = "acked";
    public static final String OUTCOME_RETRIED = "retried";
    public static final String OUTCOME_DEAD_LETTERED = "dead_lettered";

    /**
     * A message set aside in the parking lot rather than the dead-letter queue.
     *
     * <p>Distinct from {@link #OUTCOME_DEAD_LETTERED} because the two mean different things to
     * whoever is on call. Dead-lettered is "this failed as many times as the policy allows",
     * which is usually a dependency being down and usually fixes itself. Parked is "nothing can
     * read this message" — a payload that will not decode, which will not decode on any future
     * attempt either — and it is a deploy or a schema problem that will not fix itself. Sharing
     * one outcome value would put both on the same graph and make neither actionable.
     */
    public static final String OUTCOME_PARKED = "parked";

    public static final String OUTCOME_REJECTED = "rejected";
    public static final String OUTCOME_ANSWERED = "answered";
    public static final String OUTCOME_TIMED_OUT = "timed_out";
    public static final String OUTCOME_PUBLISHED = "published";
    public static final String OUTCOME_COMPLETED = "completed";
    public static final String OUTCOME_ENDED_EARLY = "ended_early";

    /** Pipeline a run belongs to. */
    public static final String TAG_PIPELINE = "pipeline";

    /** Step a pipeline run was at when it finished. */
    public static final String TAG_STEP = "step";

    // ---------- span names ----------

    /** Span covering a publish. Rendered as {@code <destination> publish}. */
    public static final String SPAN_PUBLISH_SUFFIX = " publish";

    /** Span covering a handler running. Rendered as {@code <queue> process}. */
    public static final String SPAN_PROCESS_SUFFIX = " process";

    /** Span covering a whole request/reply round trip. Rendered as {@code <destination> request}. */
    public static final String SPAN_REQUEST_SUFFIX = " request";

    private MetricNames() {
        throw new AssertionError("MetricNames is a constant holder and must not be instantiated");
    }
}
