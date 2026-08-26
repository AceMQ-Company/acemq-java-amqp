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
 * When, and how often, a failed message should be tried again.
 *
 * <p>A policy is a schedule, not a mechanism. It says a fourth attempt should happen roughly
 * twenty-five seconds after the third; it does not say how the waiting happens. That
 * separation matters, because the wait must never be a {@code Thread.sleep} inside a handler:
 * a sleeping handler holds a prefetch slot, so a handful of slow retries can stop a consumer
 * dead while the queue behind it grows. The engine instead parks the message in the broker and
 * lets it come back when it is due.
 *
 * <p>Two independent limits apply, and either one ends the retries:
 *
 * <ul>
 *   <li>{@link #maxAttempts()} — how many deliveries in total, including the first;
 *   <li>{@link #maxMessageAge()} — how old the message may get, measured from its first
 *       publish rather than from the most recent failure.
 * </ul>
 *
 * <p>The age limit is what stops a message from circulating for days after an outage: five
 * attempts with an hour between them is six hours of retrying that almost nobody intends.
 */
public final class RetryPolicy {

    private static final RetryPolicy NONE = new RetryPolicy(1, Collections.emptyList(), Duration.ofDays(365), 0.0);

    private final int maxAttempts;
    private final List<Duration> schedule;
    private final Duration maxMessageAge;
    private final double jitterFactor;

    private RetryPolicy(int maxAttempts, List<Duration> schedule, Duration maxMessageAge, double jitterFactor) {
        this.maxAttempts = maxAttempts;
        this.schedule = Collections.unmodifiableList(new ArrayList<>(schedule));
        this.maxMessageAge = maxMessageAge;
        this.jitterFactor = jitterFactor;
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
     * Retries a fixed number of times with the same delay between each.
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
        return new RetryPolicy(maxAttempts, schedule, Duration.ofDays(365), 0.0);
    }

    /**
     * Retries with an exponentially growing delay, capped.
     *
     * <p>For example {@code exponential(5, ofSeconds(1), ofMinutes(5))} waits one second, then
     * five, twenty-five, and one hundred and twenty-five seconds before giving up after the
     * fifth delivery.
     *
     * @param maxAttempts total deliveries including the first; must be at least 1
     * @param initialDelay wait before the second attempt
     * @param maxDelay ceiling for any single wait
     * @return an exponential policy with a multiplier of five
     */
    public static RetryPolicy exponential(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        return exponential(maxAttempts, initialDelay, 5.0, maxDelay);
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
            long millis = (long) Math.min(current, (double) maxDelay.toMillis());
            schedule.add(Duration.ofMillis(millis));
            current *= multiplier;
        }
        // Ten percent jitter by default. Without it, a downstream outage that fails a thousand
        // messages at once retries all thousand at the same instant, and keeps doing so.
        return new RetryPolicy(maxAttempts, schedule, Duration.ofDays(365), 0.10);
    }

    /**
     * Returns a copy that abandons messages older than the given age.
     *
     * @param maxMessageAge age measured from first publish
     * @return a policy with the age limit applied
     */
    public RetryPolicy giveUpAfter(Duration maxMessageAge) {
        Objects.requireNonNull(maxMessageAge, "maxMessageAge must not be null");
        return new RetryPolicy(maxAttempts, schedule, maxMessageAge, jitterFactor);
    }

    /**
     * Returns a copy with a different amount of randomness applied to each delay.
     *
     * @param jitterFactor fraction of the delay to vary by, between 0 and 1
     * @return a policy with that jitter
     */
    public RetryPolicy withJitter(double jitterFactor) {
        if (jitterFactor < 0.0 || jitterFactor > 1.0) {
            throw new IllegalArgumentException("jitterFactor must be between 0 and 1, was " + jitterFactor);
        }
        return new RetryPolicy(maxAttempts, schedule, maxMessageAge, jitterFactor);
    }

    /** @return total deliveries allowed, including the first */
    public int maxAttempts() {
        return maxAttempts;
    }

    /** @return how old a message may get before it is abandoned */
    public Duration maxMessageAge() {
        return maxMessageAge;
    }

    /** @return the fraction of each delay that is randomised */
    public double jitterFactor() {
        return jitterFactor;
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
     * @param attempt the attempt that just failed, starting at 1
     * @param messageAge how long ago the message was first published
     * @return the delay before the next attempt, or empty when the message should be
     *     dead-lettered because the attempts or the age limit are exhausted
     */
    public Optional<Duration> nextDelay(int attempt, Duration messageAge) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1, was " + attempt);
        }
        if (attempt >= maxAttempts) {
            return Optional.empty();
        }
        if (messageAge != null && messageAge.compareTo(maxMessageAge) >= 0) {
            return Optional.empty();
        }
        Duration base = schedule.get(Math.min(attempt - 1, schedule.size() - 1));
        return Optional.of(applyJitter(base));
    }

    /**
     * Spreads a delay by a random fraction so that a batch of failures does not retry in
     * lockstep.
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
                + ", jitter=" + jitterFactor + "}";
    }
}
