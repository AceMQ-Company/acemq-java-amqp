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

import org.acemq.amqp.api.RetryPolicy;
import org.jspecify.annotations.Nullable;

/** How a consumer should behave. */
public final class ConsumerOptions {

    private final int prefetch;
    private final boolean requeueOnFailure;
    private final @Nullable RetryPolicy retryPolicy;

    private ConsumerOptions(int prefetch, boolean requeueOnFailure, @Nullable RetryPolicy retryPolicy) {
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be at least 1, was " + prefetch);
        }
        this.prefetch = prefetch;
        this.requeueOnFailure = requeueOnFailure;
        this.retryPolicy = retryPolicy;
    }

    /**
     * @return a prefetch of 50 with failures rejected rather than requeued
     * @implNote failures are not requeued by default. Requeueing a message that just failed
     *     sends it straight back to the same consumer, which spins at full speed and starves
     *     every other message. Until the retry ladder exists, rejecting is the honest
     *     behaviour: the message goes to a dead-letter queue if one is configured.
     */
    public static ConsumerOptions defaults() {
        return new ConsumerOptions(50, false, null);
    }

    /**
     * @param prefetch maximum unacknowledged deliveries at once
     * @return options with that prefetch
     */
    public static ConsumerOptions prefetch(int prefetch) {
        return new ConsumerOptions(prefetch, false, null);
    }

    /**
     * Requeues a message when its handler fails.
     *
     * <p>Only safe when the handler is expected to succeed on another consumer, such as when
     * shedding load. Using it for ordinary failures produces a hot loop.
     *
     * @return options that requeue on failure
     */
    public ConsumerOptions requeueOnFailure() {
        return new ConsumerOptions(prefetch, true, retryPolicy);
    }

    /**
     * Retries failures on a schedule, using queues in the broker rather than a sleeping
     * handler.
     *
     * <p>Turning this on makes the consumer declare a retry rung per distinct delay, a
     * dead-letter queue and a parking lot, all derived from the policy. A failed message is
     * republished into the appropriate rung and comes back when its time-to-live expires; once
     * the attempts or the age limit are used up it lands in the dead-letter queue with the
     * reason attached.
     *
     * @param retryPolicy the schedule to follow
     * @return options with retries enabled
     */
    public ConsumerOptions withRetry(RetryPolicy retryPolicy) {
        return new ConsumerOptions(
                prefetch, requeueOnFailure, java.util.Objects.requireNonNull(retryPolicy, "retryPolicy"));
    }

    /** @return the retry schedule, when one is configured */
    public java.util.Optional<RetryPolicy> retryPolicy() {
        return java.util.Optional.ofNullable(retryPolicy);
    }

    public int prefetch() {
        return prefetch;
    }

    public boolean isRequeueOnFailure() {
        return requeueOnFailure;
    }
}
