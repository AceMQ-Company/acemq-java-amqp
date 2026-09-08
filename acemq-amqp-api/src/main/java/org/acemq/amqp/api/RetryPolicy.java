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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * When, and how often, a failed message should be tried again, and where the waiting happens.
 *
 * <p>A policy is a schedule, not a mechanism. It says a fourth attempt should happen roughly
 * eight seconds after the third; it does not say how the waiting happens. That separation
 * matters, because a long wait must never be a {@code Thread.sleep} inside a handler: a
 * sleeping handler holds an unacknowledged delivery and a prefetch slot, so a handful of slow
 * retries can stop a consumer dead while the queue behind it grows — and a consumer that
 * restarts halfway through a five-minute backoff loses the whole wait, because the broker
 * redelivers the unacknowledged message at once and a five-minute policy delivers in none.
 *
 * <p>So the waiting is split at a threshold. A wait shorter than
 * {@link #brokerWaitThreshold()} is spent in the consumer, where the seconds a restart loses
 * are only seconds and a held prefetch slot is cheap. A wait at or above it is spent in the
 * broker, in a {@code {queue}.retry.{delay}} queue whose {@code x-message-ttl} is the wait and
 * whose dead-letter target is the source queue. Splitting rather than picking one of the two
 * takes the durability where it is worth its complexity and leaves the simplicity where it is
 * not: a schedule that runs in a few seconds costs the broker no queues at all.
 *
 * <p>Jitter follows the same line, and has to. A rung queue's time-to-live is fixed when the
 * queue is declared, so a jittered delay would name a queue that does not exist; and above the
 * threshold the spread comes free anyway, because each message's time-to-live starts when it
 * enters the rung, so a fleet that failed over ten seconds is released over ten seconds. Jitter
 * therefore applies to consumer waits only.
 *
 * <p>Two independent limits apply, and either one ends the retries:
 *
 * <ul>
 *   <li>{@link #maxAttempts()} — how many deliveries in total, including the first;
 *   <li>{@link #maxMessageAge()} — how old the message may get, measured from its first
 *       publish rather than from the most recent failure, or zero for no limit at all.
 * </ul>
 *
 * <p>The age limit is what stops a message from circulating for days after an outage: five
 * attempts with an hour between them is six hours of retrying that almost nobody intends. It is
 * off unless {@link #giveUpAfter(Duration)} turns it on, because a limit nobody asked for is a
 * policy nobody asked for: {@link #maxAttempts()} already bounds the retrying, and a library
 * that also invented an age at which to stop would be making a decision on the caller's behalf
 * and dead-lettering messages for it. Zero means never, in this library and in the Go, .NET,
 * Python and Ruby ones, so the same policy abandons the same message in all five.
 */
public final class RetryPolicy {

    /**
     * Waits this long or longer are spent in the broker rather than in the consumer.
     *
     * <p>Thirty seconds is roughly where the two costs cross. Below it, the seconds a restart
     * loses are only seconds and holding one prefetch slot is cheaper than asking an operator's
     * broker for another queue. Above it, a consumer that restarts mid-wait loses the wait
     * entirely, which is a correctness bug rather than a throughput one.
     *
     * <p>The number is part of the cross-language contract: the Go, .NET, Python and Ruby
     * libraries default to the same thirty seconds, so the same policy needs the same rungs
     * whichever of them declares the topology.
     */
    public static final Duration DEFAULT_BROKER_WAIT_THRESHOLD = Duration.ofSeconds(30);

    /**
     * The age limit of a policy that has none: zero, meaning a message is never too old.
     *
     * <p>A sentinel that stands in for "no limit" has to be a value no caller would mean
     * literally, and zero is the only one: an age limit of nothing would abandon every message
     * on its first failure, which is {@link #none()} spelled the long way round. The alternative
     * this replaced — a year — looked like a safety net and behaved like a decision, because a
     * message that reached it was dead-lettered by a Java consumer and retried by the other four
     * libraries.
     */
    private static final Duration NO_AGE_LIMIT = Duration.ZERO;

    private static final RetryPolicy NONE = new RetryPolicy(
            1, Collections.emptyList(), NO_AGE_LIMIT, 0.0, DEFAULT_BROKER_WAIT_THRESHOLD);

    private final int maxAttempts;
    private final List<Duration> schedule;
    private final Duration maxMessageAge;
    private final double jitterFactor;
    private final Duration brokerWaitThreshold;

    private RetryPolicy(
            int maxAttempts,
            List<Duration> schedule,
            Duration maxMessageAge,
            double jitterFactor,
            Duration brokerWaitThreshold) {
        this.maxAttempts = maxAttempts;
        this.schedule = Collections.unmodifiableList(new ArrayList<>(schedule));
        this.maxMessageAge = maxMessageAge;
        this.jitterFactor = jitterFactor;
        this.brokerWaitThreshold = brokerWaitThreshold;
    }

    /**
     * How long before the next attempt, and where the message spends it.
     *
     * <p>Two answers rather than one because they cannot be worked out separately: jitter
     * applies only to a wait spent in the consumer, so a caller handed a delay on its own could
     * not tell whether it had already been moved — and a moved delay does not name a rung queue.
     */
    public static final class Wait {

        private final Duration delay;
        private final boolean inBroker;

        Wait(Duration delay, boolean inBroker) {
            this.delay = delay;
            this.inBroker = inBroker;
        }

        /** @return how long the message waits */
        public Duration delay() {
            return delay;
        }

        /** @return whether it waits on a rung queue rather than in the consumer */
        public boolean isInBroker() {
            return inBroker;
        }

        @Override
        public String toString() {
            return "Wait{" + delay + (inBroker ? ", in the broker}" : ", in the consumer}");
        }
    }

    /**
     * A policy that never retries: one attempt, then the dead-letter queue.
     *
     * @return the no-retry policy
     */
    public static RetryPolicy none() {
        return NONE;
    }

    /**
     * Retries a fixed number of times with the same delay between each, with no jitter.
     *
     * <p>No jitter is the point of asking for a fixed policy: a caller who says "every thirty
     * seconds" has said something exact, and a library that quietly returned twenty-six would be
     * answering a question nobody asked. Add it back with {@link #withJitter(double)} when a
     * fleet failing in lockstep is the worry.
     *
     * @param maxAttempts total deliveries including the first; must be at least 1
     * @param delay wait between attempts
     * @return a fixed-delay policy
     */
    public static RetryPolicy fixed(int maxAttempts, Duration delay) {
        requireAtLeastOne(maxAttempts);
        Objects.requireNonNull(delay, "delay must not be null");
        List<Duration> schedule = new ArrayList<>();
        for (int i = 1; i < maxAttempts; i++) {
            schedule.add(delay);
        }
        return new RetryPolicy(maxAttempts, schedule, NO_AGE_LIMIT, 0.0, DEFAULT_BROKER_WAIT_THRESHOLD);
    }

    /**
     * Retries with a doubling delay, capped.
     *
     * <p>For example {@code exponential(5, ofSeconds(1), ofMinutes(5))} waits one second, then
     * two, four and eight, before giving up after the fifth delivery.
     *
     * <p>Doubling and twenty percent jitter are the cross-language default rather than a taste:
     * the same policy has to produce the same four numbers in Go, .NET, Python and Ruby, because
     * the same message can be retried by a consumer written in any of them and a message that
     * waited one second under one library and five under another has no schedule at all.
     *
     * @param maxAttempts total deliveries including the first; must be at least 1
     * @param initialDelay wait before the second attempt
     * @param maxDelay ceiling for any single wait
     * @return an exponential policy with a multiplier of two
     */
    public static RetryPolicy exponential(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        return exponential(maxAttempts, initialDelay, 2.0, maxDelay);
    }

    /**
     * Retries with an exponentially growing delay and an explicit multiplier.
     *
     * @param maxAttempts total deliveries including the first; must be at least 1
     * @param initialDelay wait before the second attempt
     * @param multiplier growth factor; must be at least 1
     * @param maxDelay ceiling for any single wait
     * @return an exponential policy
     */
    public static RetryPolicy exponential(
            int maxAttempts, Duration initialDelay, double multiplier, Duration maxDelay) {
        requireAtLeastOne(maxAttempts);
        Objects.requireNonNull(initialDelay, "initialDelay must not be null");
        Objects.requireNonNull(maxDelay, "maxDelay must not be null");
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be at least 1, was " + multiplier);
        }
        if (initialDelay.isNegative() || initialDelay.isZero()) {
            throw new IllegalArgumentException("initialDelay must be positive, was " + initialDelay);
        }

        List<Duration> schedule = new ArrayList<>();
        double current = (double) initialDelay.toMillis();
        for (int i = 1; i < maxAttempts; i++) {
            // Capped inside the loop as well as at the end of it, so a schedule that reaches the
            // ceiling stays there instead of overflowing a double on a long enough policy.
            long millis = (long) Math.min(current, (double) maxDelay.toMillis());
            schedule.add(Duration.ofMillis(millis));
            current = Math.min(current * multiplier, (double) maxDelay.toMillis());
        }
        // Twenty percent jitter by default, and in both directions. Without it, a downstream
        // outage that fails a thousand messages at once retries all thousand at the same
        // instant, and keeps doing so; jitter that only ever delays turns a thundering herd
        // into a slower thundering herd.
        return new RetryPolicy(maxAttempts, schedule, NO_AGE_LIMIT, 0.20, DEFAULT_BROKER_WAIT_THRESHOLD);
    }

    /**
     * Returns a copy that abandons messages older than the given age.
     *
     * <p>This is the only way to get an age limit; no factory sets one. The limit is reached
     * rather than passed, so a message whose age is exactly the limit is dead-lettered — the
     * same boundary the other four libraries keep.
     *
     * @param maxMessageAge age measured from first publish, or zero for no limit
     * @return a policy with the age limit applied
     */
    public RetryPolicy giveUpAfter(Duration maxMessageAge) {
        Objects.requireNonNull(maxMessageAge, "maxMessageAge must not be null");
        return new RetryPolicy(maxAttempts, schedule, maxMessageAge, jitterFactor, brokerWaitThreshold);
    }

    /**
     * Returns a copy with a different amount of randomness applied to each delay.
     *
     * <p>Only to a delay waited in the consumer. A delay handed to a rung queue is left exactly
     * as the schedule produced it, because the queue's time-to-live is fixed at declaration and
     * a jittered delay would name a queue nobody declared.
     *
     * @param jitterFactor fraction of the delay to vary by, between 0 and 1
     * @return a policy with that jitter
     */
    public RetryPolicy withJitter(double jitterFactor) {
        if (jitterFactor < 0.0 || jitterFactor > 1.0) {
            throw new IllegalArgumentException("jitterFactor must be between 0 and 1, was " + jitterFactor);
        }
        return new RetryPolicy(maxAttempts, schedule, maxMessageAge, jitterFactor, brokerWaitThreshold);
    }

    /**
     * Returns a copy that moves the line between waiting here and waiting there.
     *
     * <p>Zero is the way out: with no threshold nothing is long enough to reach the broker, so
     * every wait is spent in the consumer and no rung queue is ever declared. That is the right
     * setting for a service that may not create queues on the broker it consumes from, and the
     * wrong one for a policy whose delays are measured in minutes.
     *
     * @param threshold waits this long or longer go to a rung queue, or zero for none of them
     * @return a policy with that threshold
     */
    public RetryPolicy waitInBrokerFrom(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold must not be null");
        if (threshold.isNegative()) {
            throw new IllegalArgumentException("threshold must not be negative, was " + threshold);
        }
        return new RetryPolicy(maxAttempts, schedule, maxMessageAge, jitterFactor, threshold);
    }

    /** @return total deliveries allowed, including the first */
    public int maxAttempts() {
        return maxAttempts;
    }

    /** @return how old a message may get before it is abandoned; zero means never */
    public Duration maxMessageAge() {
        return maxMessageAge;
    }

    /**
     * Whether this policy abandons a message for being old, as opposed to for running out of
     * attempts.
     *
     * <p>Only a positive limit is one. Zero — the default — says no message is ever too old, and
     * a negative duration says the same rather than abandoning everything, because a limit that
     * has already elapsed before the message was published cannot have been meant literally.
     */
    private boolean hasAgeLimit() {
        return maxMessageAge.compareTo(Duration.ZERO) > 0;
    }

    /** @return the fraction of each delay that is randomised */
    public double jitterFactor() {
        return jitterFactor;
    }

    /** @return the wait at which the broker takes over from the consumer; zero means never */
    public Duration brokerWaitThreshold() {
        return brokerWaitThreshold;
    }

    /**
     * Whether a wait of this length belongs on a rung queue rather than in the consumer.
     *
     * @param delay an unjittered delay, as {@link #schedule()} reports them
     * @return whether the broker rather than the consumer should hold it
     */
    public boolean waitsInBroker(Duration delay) {
        return !brokerWaitThreshold.isZero()
                && delay != null
                && !delay.isZero()
                && !delay.isNegative()
                && delay.compareTo(brokerWaitThreshold) >= 0;
    }

    /**
     * The delays this policy needs a rung queue for, in the order the schedule reaches them.
     *
     * <p>Exactly the entries of {@link #schedule()} that are at or above the threshold, with
     * repeats removed — a fixed policy that waits a minute three times needs one queue, not
     * three. It is a finite list because the schedule is, which is what lets the rungs be
     * declared with the rest of the topology instead of being conjured by a consumer at the
     * moment it first fails.
     *
     * @return one delay per rung queue; empty when every wait is spent in the consumer
     */
    public List<Duration> brokerRungs() {
        List<Duration> rungs = new ArrayList<>();
        for (Duration delay : schedule) {
            if (waitsInBroker(delay) && !rungs.contains(delay)) {
                rungs.add(delay);
            }
        }
        return Collections.unmodifiableList(rungs);
    }

    /**
     * The delays this policy uses, without jitter.
     *
     * <p>The engine reads this to work out which retry queues to create, which is why it is
     * exposed rather than kept private: the topology is derived from the policy instead of
     * being configured separately and drifting away from it.
     *
     * @return one delay per retry, in order; empty when the policy never retries
     */
    public List<Duration> schedule() {
        return schedule;
    }

    /**
     * How long to wait before the next attempt.
     *
     * <p>The delay only. {@link #nextWait(int, Duration)} is the whole answer, and is what a
     * consumer needs: a delay that will be spent in the broker is reported here exactly as the
     * schedule produced it, because that is the delay that names a rung queue, while a delay
     * that will be spent in the consumer has already had jitter applied to it.
     *
     * @param attempt the attempt that just failed, starting at 1
     * @param messageAge how long ago the message was first published
     * @return the delay before the next attempt, or empty when the message should be
     *     dead-lettered because the attempts or the age limit are exhausted
     */
    public Optional<Duration> nextDelay(int attempt, Duration messageAge) {
        return nextWait(attempt, messageAge).map(Wait::delay);
    }

    /**
     * How long to wait before the next attempt, and where the message spends it.
     *
     * @param attempt the attempt that just failed, starting at 1
     * @param messageAge how long ago the message was first published, or {@code null} when it
     *     is not known, which counts as young enough
     * @return the wait, or empty when the message should be dead-lettered because the attempts
     *     or the age limit are exhausted
     */
    public Optional<Wait> nextWait(int attempt, Duration messageAge) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1, was " + attempt);
        }
        if (attempt >= maxAttempts) {
            return Optional.empty();
        }
        if (hasAgeLimit() && messageAge != null && messageAge.compareTo(maxMessageAge) >= 0) {
            return Optional.empty();
        }

        Duration base = schedule.get(Math.min(attempt - 1, schedule.size() - 1));
        if (waitsInBroker(base)) {
            // Deliberately not jittered. A rung queue's time-to-live is fixed when it is
            // declared, so a moved delay would name a queue that does not exist; and the spread
            // jitter buys is already there, because each message's time-to-live starts when it
            // arrives on the rung rather than when the batch failed.
            return Optional.of(new Wait(base, true));
        }
        return Optional.of(new Wait(applyJitter(base), false));
    }

    /**
     * Spreads a delay by a random fraction, in both directions, so that a batch of failures
     * does not retry in lockstep.
     */
    private Duration applyJitter(Duration base) {
        if (jitterFactor <= 0.0) {
            return base;
        }
        double spread = base.toMillis() * jitterFactor;
        double offset = (Math.random() * 2 - 1) * spread;
        long millis = Math.max(1, (long) (base.toMillis() + offset));
        return Duration.ofMillis(millis);
    }

    private static void requireAtLeastOne(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException(
                    "maxAttempts must be at least 1, was " + maxAttempts + ". A value of 1 means deliver once and"
                            + " never retry; use RetryPolicy.none() to say that explicitly.");
        }
    }

    @Override
    public String toString() {
        return "RetryPolicy{maxAttempts=" + maxAttempts + ", schedule=" + schedule + ", maxMessageAge=" + maxMessageAge
                + ", jitter=" + jitterFactor + ", brokerWaitThreshold=" + brokerWaitThreshold + "}";
    }
}
