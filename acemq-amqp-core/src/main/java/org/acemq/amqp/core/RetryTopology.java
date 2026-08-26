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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportConnection;
import org.jspecify.annotations.Nullable;

/**
 * Builds the queues that make a retry happen inside the broker rather than inside a handler.
 *
 * <p>The mechanism is a ladder. Each distinct delay in the policy gets its own queue with a
 * message time-to-live and a dead-letter target pointing back at the source queue. A message
 * that needs to wait five seconds is published into the five-second rung, sits there doing
 * nothing, expires, and is dead-lettered home. No consumer is involved and no thread waits.
 *
 * <p>For a queue named {@code orders.new} with an exponential policy, this produces:
 *
 * <pre>
 * orders.new.retry.1s     ttl 1s   -&gt; orders.new
 * orders.new.retry.5s     ttl 5s   -&gt; orders.new
 * orders.new.retry.25s    ttl 25s  -&gt; orders.new
 * orders.new.dlq                   (attempts exhausted, or too old)
 * orders.new.parked                (could not even be decoded)
 * </pre>
 *
 * <p>Two details are easy to get wrong and are worth stating. Rungs are keyed by delay rather
 * than by attempt number, so a policy that reaches its ceiling stops creating new queues
 * instead of adding an identical one per remaining attempt. And messages are only ever
 * <em>published</em> into a rung — nothing consumes one, because a consumer would defeat the
 * entire purpose by taking the message before its time-to-live expired.
 */
final class RetryTopology {

    /** Exchange every rung dead-letters through on its way back to the source queue. */
    static final String RETRY_EXCHANGE = "acemq.retry";

    /** Exchange used to reach the dead-letter and parking queues. */
    static final String DEAD_LETTER_EXCHANGE = "acemq.dlx";

    private final String sourceQueue;
    private final RetryPolicy policy;
    private final Map<Duration, String> rungs;
    private final String deadLetterQueue;
    private final String parkingLotQueue;

    private RetryTopology(
            String sourceQueue,
            RetryPolicy policy,
            Map<Duration, String> rungs,
            String deadLetterQueue,
            String parkingLotQueue) {
        this.sourceQueue = sourceQueue;
        this.policy = policy;
        this.rungs = Collections.unmodifiableMap(rungs);
        this.deadLetterQueue = deadLetterQueue;
        this.parkingLotQueue = parkingLotQueue;
    }

    /**
     * Works out the topology a policy needs, without touching the broker.
     *
     * @param sourceQueue the queue being consumed
     * @param policy the retry schedule
     * @return the queues required
     */
    static RetryTopology forQueue(String sourceQueue, RetryPolicy policy) {
        Map<Duration, String> rungs = new LinkedHashMap<>();
        Set<Duration> distinct = new LinkedHashSet<>(policy.schedule());
        for (Duration delay : distinct) {
            rungs.put(delay, sourceQueue + ".retry." + describe(delay));
        }
        return new RetryTopology(sourceQueue, policy, rungs, sourceQueue + ".dlq", sourceQueue + ".parked");
    }

    /**
     * Declares everything this topology needs.
     *
     * <p>Safe to call repeatedly: declaring a queue that already exists with the same
     * arguments is how AMQP is meant to be used.
     *
     * @param connection the broker connection
     */
    void declare(TransportConnection connection) {
        connection.declareExchange(RETRY_EXCHANGE, "direct", true);
        connection.declareExchange(DEAD_LETTER_EXCHANGE, "direct", true);

        // Each rung expires its messages back to the source queue. A rung is never consumed;
        // the time-to-live is the only thing that ever removes a message from it.
        rungs.forEach((delay, queueName) -> {
            Map<String, Object> arguments = new LinkedHashMap<>();
            arguments.put("x-message-ttl", delay.toMillis());
            arguments.put("x-dead-letter-exchange", RETRY_EXCHANGE);
            arguments.put("x-dead-letter-routing-key", sourceQueue);
            connection.declareQueue(queueName, QueueType.CLASSIC, true, arguments);
        });

        // One binding brings every expired message back to the queue it came from.
        if (!rungs.isEmpty()) {
            connection.bindQueue(sourceQueue, RETRY_EXCHANGE, sourceQueue);
        }

        connection.declareQueue(deadLetterQueue, QueueType.CLASSIC, true, Collections.emptyMap());
        connection.bindQueue(deadLetterQueue, DEAD_LETTER_EXCHANGE, deadLetterQueue);

        connection.declareQueue(parkingLotQueue, QueueType.CLASSIC, true, Collections.emptyMap());
        connection.bindQueue(parkingLotQueue, DEAD_LETTER_EXCHANGE, parkingLotQueue);
    }

    /**
     * Picks the rung a delay belongs in.
     *
     * <p>The requested delay is rounded to the nearest rung that is not shorter than it, so a
     * handler asking for three seconds waits five rather than one. Waiting slightly too long
     * is harmless; retrying too early defeats the backoff.
     *
     * @param delay how long the message should wait
     * @return the queue to publish it into, or empty when no rungs exist
     */
    java.util.Optional<String> rungFor(Duration delay) {
        String best = null;
        Duration bestDelay = null;
        for (Map.Entry<Duration, String> rung : rungs.entrySet()) {
            if (rung.getKey().compareTo(delay) >= 0 && (bestDelay == null || rung.getKey().compareTo(bestDelay) < 0)) {
                bestDelay = rung.getKey();
                best = rung.getValue();
            }
        }
        if (best == null && !rungs.isEmpty()) {
            // Longer than every rung: use the longest one available.
            for (Map.Entry<Duration, String> rung : rungs.entrySet()) {
                if (bestDelay == null || rung.getKey().compareTo(bestDelay) > 0) {
                    bestDelay = rung.getKey();
                    best = rung.getValue();
                }
            }
        }
        return java.util.Optional.ofNullable(best);
    }

    String sourceQueue() {
        return sourceQueue;
    }

    RetryPolicy policy() {
        return policy;
    }

    String deadLetterQueue() {
        return deadLetterQueue;
    }

    String parkingLotQueue() {
        return parkingLotQueue;
    }

    /** @return the rung queue names, in schedule order */
    List<String> rungQueues() {
        return new ArrayList<>(rungs.values());
    }

    /**
     * Renders a duration as a short, stable queue-name suffix.
     *
     * <p>Queue names end up in dashboards and alerts, so {@code orders.new.retry.5s} is worth
     * the small amount of code it takes to avoid {@code orders.new.retry.PT5S}.
     */
    static String describe(Duration delay) {
        long millis = delay.toMillis();
        if (millis % 3_600_000 == 0 && millis >= 3_600_000) {
            return (millis / 3_600_000) + "h";
        }
        if (millis % 60_000 == 0 && millis >= 60_000) {
            return (millis / 60_000) + "m";
        }
        if (millis % 1_000 == 0 && millis >= 1_000) {
            return (millis / 1_000) + "s";
        }
        return millis + "ms";
    }

    @Override
    public String toString() {
        return "RetryTopology{queue=" + sourceQueue + ", rungs=" + rungs.values() + ", dlq=" + deadLetterQueue + "}";
    }

    /** Lower-cases a name the way queue naming conventions expect. */
    static @Nullable String normalise(@Nullable String name) {
        return name == null ? null : name.toLowerCase(Locale.ROOT);
    }
}
