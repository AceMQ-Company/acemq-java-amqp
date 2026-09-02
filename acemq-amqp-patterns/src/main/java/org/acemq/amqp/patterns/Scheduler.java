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
package org.acemq.amqp.patterns;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivering a message later.
 *
 * <pre>{@code
 * try (Scheduler scheduler = Scheduler.on(mq)) {
 *     scheduler.in(Duration.ofHours(4), "billing", "invoice.due", invoice);
 *     scheduler.at(renewalDate, "policies", "policy.renew", policy);
 * }
 * }</pre>
 *
 * <h2>Why not a per-message time to live</h2>
 *
 * <p>The obvious implementation is to set {@code expiration} on the message, drop it in a queue
 * nobody consumes, and let it dead-letter to its destination. It is what most articles suggest
 * and it is wrong for anything but a single fixed delay, because <strong>a classic queue expires
 * messages only at its head</strong>.
 *
 * <p>Put a four-hour message in, then a one-minute message behind it, and the one-minute message
 * is delivered in four hours. Nothing reports this: the queue looks healthy, the message is not
 * lost, it is simply late by a factor nobody predicted. It is the single most common way a
 * home-made scheduler fails, and it fails in production under mixed load rather than in testing
 * under uniform load.
 *
 * <h2>What this does instead</h2>
 *
 * <p>A small ladder of queues, each with a <em>uniform</em> time to live, and a message hops
 * through them until it is due:
 *
 * <pre>
 * acemq.schedule.1s  acemq.schedule.10s  acemq.schedule.1m  acemq.schedule.10m  acemq.schedule.1h
 * </pre>
 *
 * <p>Every message in a given rung has the same delay, so head-of-line expiry is not a problem —
 * the head is always the message due soonest. Each expiry returns the message to this
 * scheduler, which either delivers it or puts it in the largest rung that does not overshoot.
 * A four-hour delay is four one-hour hops; a ninety-second delay is one minute, then three tens.
 *
 * <p>The cost is honest and worth stating: a long delay is several broker round trips rather
 * than one, and delivery is accurate to about the smallest rung rather than to the second. A
 * scheduler that must fire at 09:00:00.000 exactly is a scheduler, not a message broker.
 *
 * <p>The alternative is RabbitMQ's delayed-message-exchange plugin, which does this properly and
 * is a plugin — so it is not available everywhere, and a library that silently required it would
 * be a library that works on your laptop.
 */
public final class Scheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    /** Where a message waits, and where it comes back to be re-examined. */
    static final String EXCHANGE = "acemq.schedule";

    /** The queue every expired message returns to. */
    static final String CONTROL = "acemq.schedule.due";

    // Deliberately not the "x-acemq-" prefix. That one is reserved: AceHeaders.isAceHeader
    // matches it, and the envelope drops every header carrying it from the application's view
    // on the way in, because engine headers are materialised as envelope fields instead. A
    // scheduler header using it would be written on publish and gone on consume -- which is
    // exactly what happened while this class was being written, and cost an afternoon.
    private static final String TARGET_EXCHANGE = "x-schedule-exchange";
    private static final String TARGET_ROUTING_KEY = "x-schedule-routing-key";
    private static final String DUE_AT = "x-schedule-due-at";

    /**
     * What the payload was encoded as when it was scheduled.
     *
     * <p>Carried because the scheduler republishes bytes rather than objects, and a consumer
     * picks its codec from the content type. Publishing pre-encoded bytes under
     * {@code application/octet-stream} produces a message the intended consumer cannot decode —
     * it arrives, it is the right bytes, and nothing can read it. The outbox relay had exactly
     * this bug once; unlike the relay, a scheduler moves whatever it is given, so it cannot
     * assume JSON and has to remember.
     */
    private static final String CONTENT_TYPE = "x-schedule-content-type";

    /**
     * The rungs, longest first.
     *
     * <p>Five of them, spanning a second to an hour. More rungs mean finer accuracy and more
     * queues; fewer mean more hops for a long delay. This spread delivers a one-day message in
     * twenty-four hops and a one-minute message in one, which is the right way round — short
     * delays are common and want to be cheap.
     */
    private static final List<Duration> RUNGS = List.of(
            Duration.ofHours(1), Duration.ofMinutes(10), Duration.ofMinutes(1),
            Duration.ofSeconds(10), Duration.ofSeconds(1));

    private final AceMq mq;
    private final MessageConsumer due;
    private final AtomicLong scheduled = new AtomicLong();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong hops = new AtomicLong();

    private Scheduler(AceMq mq) {
        this.mq = mq;
        declareTopology();
        this.due = mq.consume(
                CONTROL,
                byte[].class,
                // Raw bytes: this scheduler never looks inside a payload, and decoding one it
                // has no business understanding is how a scheduler acquires opinions about
                // message formats.
                ConsumerOptions.prefetch(50).as(org.acemq.amqp.core.Codecs.byName("bytes")),
                message -> forward(message.payload(), message.headers()));
    }

    /**
     * @param mq an open connection
     * @return a scheduler, with its queues declared
     */
    public static Scheduler on(AceMq mq) {
        return new Scheduler(Objects.requireNonNull(mq, "mq"));
    }

    /**
     * Delivers a message after a delay.
     *
     * @param delay how long to wait; zero or negative delivers immediately
     * @param exchange where it should eventually go
     * @param routingKey the routing key it should eventually carry
     * @param payload the message
     */
    public void in(Duration delay, String exchange, String routingKey, Object payload) {
        at(Instant.now().plus(Objects.requireNonNull(delay, "delay")), exchange, routingKey, payload);
    }

    /**
     * Delivers a message at a moment.
     *
     * @param when the moment; anything in the past delivers immediately
     * @param exchange where it should eventually go
     * @param routingKey the routing key it should eventually carry
     * @param payload the message
     */
    public void at(Instant when, String exchange, String routingKey, Object payload) {
        Objects.requireNonNull(when, "when");
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKey, "routingKey");

        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put(TARGET_EXCHANGE, exchange);
        headers.put(TARGET_ROUTING_KEY, routingKey);
        headers.put(DUE_AT, when.toEpochMilli());

        scheduled.incrementAndGet();
        // Encoded once, here, and carried as bytes from then on. The content type goes with
        // them, because that is how the eventual consumer chooses a codec.
        org.acemq.amqp.api.Codec codec = org.acemq.amqp.core.Codecs.forPublishing();
        headers.put(CONTENT_TYPE, codec.contentType());
        route(codec.encode(payload), headers, when);
    }

    /** Called for every message that has come out of a rung. */
    private void forward(byte[] payload, Map<String, Object> headers) {
        Object dueAt = headers.get(DUE_AT);
        Object exchange = headers.get(TARGET_EXCHANGE);
        Object routingKey = headers.get(TARGET_ROUTING_KEY);
        if (dueAt == null || exchange == null || routingKey == null) {
            throw new AceMqException("a message reached " + CONTROL + " without the headers a"
                    + " scheduled message carries. Something else is publishing into the scheduler's"
                    + " queues, which it must not: they are an implementation detail of this class.");
        }
        route(payload, headers, Instant.ofEpochMilli(((Number) dueAt).longValue()));
    }

    /** Delivers if it is due, and otherwise puts it in the largest rung that does not overshoot. */
    private void route(byte[] payload, Map<String, Object> headers, Instant when) {
        Duration remaining = Duration.between(Instant.now(), when);

        if (remaining.isNegative() || remaining.isZero() || remaining.compareTo(RUNGS.get(RUNGS.size() - 1)) < 0) {
            // Due, or so nearly due that another hop would cost more than the accuracy it buys.
            deliver(payload, headers);
            return;
        }

        Duration rung = RUNGS.stream()
                .filter(candidate -> candidate.compareTo(remaining) <= 0)
                .findFirst()
                .orElse(RUNGS.get(RUNGS.size() - 1));

        hops.incrementAndGet();
        mq.publisher(EXCHANGE, rungName(rung), byte[].class)
                .as(org.acemq.amqp.core.Codecs.byName("bytes"))
                .send(payload, Envelope.of("ScheduledMessage").headers(headers).build());
    }

    private void deliver(byte[] payload, Map<String, Object> headers) {
        String exchange = String.valueOf(headers.get(TARGET_EXCHANGE));
        String routingKey = String.valueOf(headers.get(TARGET_ROUTING_KEY));

        Object contentType = headers.get(CONTENT_TYPE);

        // The scheduler's own headers are not passed on: they are bookkeeping, and a consumer
        // that started depending on them would be depending on how a message got to it.
        delivered.incrementAndGet();
        mq.publisher(exchange, routingKey, byte[].class)
                .as(verbatim(contentType == null ? "application/json" : String.valueOf(contentType)))
                .send(payload, Envelope.of("ScheduledMessage").build());
    }

    private void declareTopology() {
        mq.declareExchange(EXCHANGE, "direct");

        for (Duration rung : RUNGS) {
            Map<String, Object> arguments = new LinkedHashMap<>();
            arguments.put("x-message-ttl", rung.toMillis());
            arguments.put("x-dead-letter-exchange", EXCHANGE);
            arguments.put("x-dead-letter-routing-key", CONTROL);
            // Every message in this queue has the same delay, so the head is always the one due
            // soonest. That is what makes head-of-line expiry harmless here.
            mq.declareQueue(rungName(rung), org.acemq.amqp.transport.QueueType.CLASSIC, arguments);
            mq.bind(rungName(rung), EXCHANGE, rungName(rung));
        }

        mq.declareQueue(CONTROL, org.acemq.amqp.transport.QueueType.CLASSIC, Collections.emptyMap());
        mq.bind(CONTROL, EXCHANGE, CONTROL);
        log.debug("scheduler declared with rungs {}", RUNGS);
    }

    /**
     * Writes already-encoded bytes out unchanged, under the content type they were encoded as.
     *
     * <p>Publishing them through an ordinary codec would encode them a second time, and what
     * arrives is JSON containing JSON. Publishing them as raw bytes loses the content type, and
     * what arrives cannot be decoded by the consumer that was waiting for it.
     */
    private static org.acemq.amqp.api.Codec verbatim(String contentType) {
        return new org.acemq.amqp.api.Codec() {

            @Override
            public String contentType() {
                return contentType;
            }

            @Override
            public byte[] encode(Object payload) {
                return (byte[]) payload;
            }

            @Override
            public <T> T decode(byte[] body, Class<T> target) {
                throw new UnsupportedOperationException("the scheduler only publishes");
            }

            @Override
            public boolean canDecode(@org.jspecify.annotations.Nullable String type) {
                return false;
            }
        };
    }

    static String rungName(Duration rung) {
        return EXCHANGE + "." + describe(rung);
    }

    private static String describe(Duration rung) {
        long millis = rung.toMillis();
        if (millis % 3_600_000 == 0) {
            return (millis / 3_600_000) + "h";
        }
        if (millis % 60_000 == 0) {
            return (millis / 60_000) + "m";
        }
        return (millis / 1_000) + "s";
    }

    /** @return messages handed to this scheduler */
    public long scheduled() {
        return scheduled.get();
    }

    /** @return messages that reached their destination */
    public long delivered() {
        return delivered.get();
    }

    /**
     * @return how many times a message moved between rungs. Divided by {@link #delivered()} this
     *     is the average number of hops, which is the number to look at if the scheduler is
     *     busier than expected: long delays cost hops
     */
    public long hops() {
        return hops.get();
    }

    @Override
    public void close() throws Exception {
        due.close();
    }

    @Override
    public String toString() {
        return "Scheduler{rungs=" + RUNGS + ", scheduled=" + scheduled + ", delivered=" + delivered + "}";
    }
}
