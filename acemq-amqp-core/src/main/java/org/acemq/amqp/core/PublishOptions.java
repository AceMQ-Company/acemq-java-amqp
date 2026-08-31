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

import org.jspecify.annotations.Nullable;

/**
 * How a message should be published.
 *
 * <p>The defaults are the safe ones, and they are the defaults precisely because the unsafe
 * versions are faster: messages are written to disk, and a message nothing is bound to receive is
 * an error rather than a silence. Both can be turned off, deliberately and visibly, per publisher
 * or per message.
 *
 * <pre>{@code
 * // Telemetry: high volume, and losing some in a broker restart is fine.
 * mq.publisher("metrics", "cpu.sample", Sample.class,
 *         PublishOptions.transientDelivery().expiringAfter(Duration.ofMinutes(5)));
 *
 * // A fan-out nobody may be listening to yet.
 * mq.publisher("audit", "user.login", LoginEvent.class,
 *         PublishOptions.defaults().allowUnroutable());
 * }</pre>
 *
 * <p>Immutable: every method returns a new instance, so one held in a field cannot be changed
 * under a thread that is publishing with it.
 */
public final class PublishOptions {

    private final boolean persistent;
    private final boolean mandatory;
    private final @Nullable Duration expiration;

    private PublishOptions(boolean persistent, boolean mandatory, @Nullable Duration expiration) {
        if (expiration != null && (expiration.isNegative() || expiration.isZero())) {
            throw new IllegalArgumentException("expiration must be positive, was " + expiration);
        }
        this.persistent = persistent;
        this.mandatory = mandatory;
        this.expiration = expiration;
    }

    /**
     * @return messages written to disk, unroutable messages reported as failures, and no expiry
     */
    public static PublishOptions defaults() {
        return new PublishOptions(true, true, null);
    }

    /**
     * Messages the broker may keep in memory only.
     *
     * <p>Faster, and lost when the broker restarts — including messages already sitting in a
     * durable queue, because a transient message in a durable queue is still transient. Reasonable
     * for telemetry, samples and cache invalidations, where the next message makes the last one
     * irrelevant. Not reasonable for anything a person would notice the absence of.
     *
     * <p>Note that a publisher confirm still means the broker accepted the message. It does not
     * mean the message survived a restart, and with this set it very well might not have.
     *
     * @return options that let the broker skip the disk
     */
    public static PublishOptions transientDelivery() {
        return new PublishOptions(false, true, null);
    }

    /**
     * Stops treating an unroutable message as a failure.
     *
     * <p>By default a message that reaches the broker but matches no binding raises
     * {@code PublishFailedException}, because that is nearly always a typo in a routing key or a
     * queue nobody declared, and it is the single easiest way to lose messages while every log
     * line looks healthy.
     *
     * <p>Turn it off only where nothing being bound is genuinely expected: an audit fan-out with
     * no subscribers yet, or an event nobody is required to consume. It is a real decision, and
     * it makes the routing-key typo silent again for this publisher.
     *
     * @return options that accept unroutable messages
     */
    public PublishOptions allowUnroutable() {
        return new PublishOptions(persistent, false, expiration);
    }

    /**
     * Discards the message if it has not been consumed within the given time.
     *
     * <p>Per-message time-to-live, applied by the broker. For messages that stop being worth
     * delivering — a price quote, a session heartbeat, a cache invalidation for a key that has
     * since changed twice.
     *
     * <p>Expiry is not a guarantee of promptness: RabbitMQ removes an expired message when it
     * reaches the head of the queue, so one sitting behind a backlog can outlive its time-to-live
     * and still be discarded rather than delivered late. A consumer must not assume that whatever
     * arrives is within its window.
     *
     * @param expiration how long the message stays worth delivering; must be positive
     * @return options with that expiry
     */
    public PublishOptions expiringAfter(Duration expiration) {
        return new PublishOptions(persistent, mandatory, expiration);
    }

    /** @return whether the broker is asked to write these messages to disk */
    public boolean persistent() {
        return persistent;
    }

    /** @return whether an unroutable message is reported as a failure */
    public boolean mandatory() {
        return mandatory;
    }

    /** @return how long a message stays worth delivering, when a limit was set */
    public java.util.Optional<Duration> expiration() {
        return java.util.Optional.ofNullable(expiration);
    }

    @Override
    public String toString() {
        return "PublishOptions{persistent=" + persistent + ", mandatory=" + mandatory
                + ", expiration=" + expiration + "}";
    }
}
