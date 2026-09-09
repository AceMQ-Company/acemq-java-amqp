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
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportConnection;
import org.jspecify.annotations.Nullable;

/**
 * Builds the queues that make a long retry happen inside the broker rather than inside a
 * handler.
 *
 * <p>The mechanism is a ladder. Each distinct <em>long</em> delay in the policy gets its own
 * queue with a message time-to-live and a dead-letter target pointing back at the source queue.
 * A message that needs to wait a minute is published into the one-minute rung, sits there doing
 * nothing, expires, and is dead-lettered home. No consumer is involved and no thread waits for
 * it here.
 *
 * <p>Only the long delays. A wait below {@link RetryPolicy#brokerWaitThreshold()} is spent in
 * the consumer instead and gets no queue at all, which is why a schedule that runs in a few
 * seconds costs the broker nothing. For a queue named {@code orders.new} with
 * {@code exponential(6, ofSeconds(10), ofMinutes(5))} and the default thirty-second threshold,
 * whose schedule is 10s, 20s, 40s, 80s and 160s:
 *
 * <pre>
 * orders.new.retry.40s    ttl 40s   -&gt; orders.new
 * orders.new.retry.80s    ttl 80s   -&gt; orders.new
 * orders.new.retry.160s   ttl 160s  -&gt; orders.new
 * orders.new.dlq                    (attempts exhausted, or too old)
 * orders.new.parked                 (could not even be decoded)
 * </pre>
 *
 * <p>The ten- and twenty-second waits get no rung; the consumer holds those itself.
 *
 * <p>Three details are easy to get wrong and are worth stating. Rungs are keyed by delay rather
 * than by attempt number, so a policy that reaches its ceiling stops creating new queues
 * instead of adding an identical one per remaining attempt. They are then keyed by name as
 * well, because two delays can render to the same name and a second queue by that name with a
 * different time-to-live is not a second rung but a {@code PRECONDITION_FAILED} at declaration
 * time. And messages are only ever <em>published</em> into a rung — nothing consumes one,
 * because a consumer would defeat the entire purpose by taking the message before its
 * time-to-live expired.
 */
final class RetryTopology {

    /** Exchange every rung dead-letters through on its way back to the source queue. */
    static final String RETRY_EXCHANGE = "acemq.retry";

    /**
     * Exchange used to reach the dead-letter and parking queues.
     *
     * <p>Taken from {@link Topology} rather than spelled again here. The name is the thing two
     * services have to agree on, and a second copy of a string is a second place for it to
     * drift.
     */
    static final String DEAD_LETTER_EXCHANGE = Topology.DEAD_LETTER_EXCHANGE;

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
     * <p>Rungs come from {@link RetryPolicy#brokerRungs()} rather than from the whole schedule,
     * so the short waits the consumer holds itself do not each leave a queue behind on somebody
     * else's broker.
     *
     * @param sourceQueue the queue being consumed
     * @param policy the retry schedule
     * @return the queues required
     */
    static RetryTopology forQueue(String sourceQueue, RetryPolicy policy) {
        Map<Duration, String> rungs = new LinkedHashMap<>();
        Set<String> names = new LinkedHashSet<>();
        for (Duration delay : policy.brokerRungs()) {
            // Keyed by name as well as by delay. Two delays can render to the same name, and a
            // second queue by that name with a different time-to-live is not a second rung — it
            // is a PRECONDITION_FAILED the moment the second consumer declares it.
            String name = sourceQueue + ".retry." + describe(delay);
            if (names.add(name)) {
                rungs.put(delay, name);
            }
        }
        return new RetryTopology(sourceQueue, policy, rungs, sourceQueue + ".dlq", sourceQueue + ".parked");
    }

    /**
     * The argument table a rung queue must be declared with.
     *
     * <p>A contract rather than a preference, and pinned by a test for that reason. Two services
     * consuming the same queue declare the same rung by name, so if one of them declares it with
     * different arguments the second is refused with {@code PRECONDITION_FAILED} and cannot
     * consume at all.
     *
     * <p>{@code x-message-ttl} and never a per-message expiration. RabbitMQ expires messages only
     * from the head of a queue, so one queue holding per-message time-to-live values releases
     * nothing while a message with a long one sits at the front: a thirty-second wait queued
     * behind a ten-minute wait becomes a ten-minute wait, and the delays that come out bear no
     * relation to the ones that went in. One queue per distinct delay is more queues and is the
     * only arrangement that delivers the schedule it was given.
     *
     * @param sourceQueue the queue expired messages go back to
     * @param delay the rung's wait
     * @return the arguments, in a stable order
     */
    static Map<String, Object> rungArguments(String sourceQueue, Duration delay) {
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put("x-message-ttl", delay.toMillis());
        arguments.put("x-dead-letter-exchange", RETRY_EXCHANGE);
        arguments.put("x-dead-letter-routing-key", sourceQueue);
        return arguments;
    }

    /**
     * Declares everything this topology needs.
     *
     * <p>Safe to call repeatedly: declaring a queue that already exists with the same
     * arguments is how AMQP is meant to be used.
     *
     * <p>The source queue is <em>not</em> declared here, and that is deliberate. It belongs to
     * whoever set the service up, who chose its type and its arguments; redeclaring it from a
     * consumer would mean guessing both, and a guess of {@code classic} against a quorum queue
     * is a {@code PRECONDITION_FAILED} that stops the consumer starting at all. It also means
     * this method cannot put {@code x-dead-letter-exchange} on the source queue — the broker
     * route that catches a message this library never sees. Ask for that where the queue is
     * declared, with {@link Topology.Builder#queueWithDeadLetter}, which is where the Go,
     * .NET, Python and Ruby libraries put it too.
     *
     * @param connection the broker connection
     */
    void declare(TransportConnection connection) {
        connection.declareExchange(RETRY_EXCHANGE, "direct", true);
        connection.declareExchange(DEAD_LETTER_EXCHANGE, "direct", true);

        // Each rung expires its messages back to the source queue. A rung is never consumed;
        // the time-to-live is the only thing that ever removes a message from it.
        rungs.forEach((delay, queueName) -> connection.declareQueue(queueName, QueueType.CLASSIC, true,
                rungArguments(sourceQueue, delay)));

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
     * <p>Empty for anything below the policy's threshold, and that is an answer the caller acts
     * on rather than a failure: waiting in the consumer is the other half of the design, not a
     * fallback.
     *
     * <p>Above it, the requested delay is rounded to the nearest rung that is not shorter than
     * it, so a caller asking for fifty seconds waits eighty rather than forty. Waiting slightly
     * too long is harmless; retrying too early defeats the backoff.
     *
     * @param delay how long the message should wait
     * @return the queue to publish it into, or empty when the wait belongs in the consumer
     */
    java.util.Optional<String> rungFor(Duration delay) {
        if (!policy.waitsInBroker(delay)) {
            return java.util.Optional.empty();
        }
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

    /**
     * The name a rung for this wait would have, whether or not one exists.
     *
     * <p>Wanted on the path where {@link #rungFor} found nothing: a counter saying a rung is
     * missing is only actionable if it also says which queue to declare, and the name is
     * derivable from the wait even when the queue is not there.
     *
     * @param delay the wait
     * @return the rung queue's name
     */
    String rungNameFor(Duration delay) {
        return sourceQueue + ".retry." + describe(delay);
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

    /** @return each rung's delay and queue name, in schedule order */
    Map<Duration, String> rungs() {
        return rungs;
    }

    /**
     * Renders a duration as a short, stable queue-name suffix.
     *
     * <p>Queue names end up in dashboards and alerts, so {@code orders.new.retry.5s} is worth
     * the small amount of code it takes to avoid {@code orders.new.retry.PT5S}.
     *
     * <p>Sub-second delays render in milliseconds — {@code retry.500ms} — where the Go, Python
     * and Ruby libraries render {@code retry.0s}. The divergence is unreachable through the
     * default thirty-second threshold, and it only appears for a policy that has deliberately
     * lowered the threshold below a second, which is asking the broker to hold a message for
     * less time than it takes to publish it. Milliseconds are kept because they are the answer
     * that cannot collide: two different sub-second waits both named {@code retry.0s} would be
     * one queue with two time-to-live values, and the second declaration of it is refused.
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
