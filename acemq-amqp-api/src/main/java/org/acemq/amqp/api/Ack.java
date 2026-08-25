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
import java.util.Objects;
import java.util.Optional;

/**
 * What should happen to a delivery once a handler has looked at it.
 *
 * <p>Most handlers never mention this type: returning normally accepts the message and
 * throwing decides the rest. It exists for handlers that want to state the outcome directly,
 * and because the four cases map exactly onto AMQP 1.0 dispositions, which is what keeps one
 * consumer API honest across both protocols.
 *
 * <table>
 * <caption>How each outcome is carried out</caption>
 * <tr><th>Outcome</th><th>AMQP 0-9-1</th><th>AMQP 1.0</th></tr>
 * <tr><td>{@link #accept()}</td><td>{@code basic.ack}</td><td>accepted</td></tr>
 * <tr><td>{@link #retry}</td><td>republish to the next retry tier</td><td>modified, or scheduled delivery</td></tr>
 * <tr><td>{@link #deadLetter}</td><td>{@code basic.nack} without requeue</td><td>rejected</td></tr>
 * <tr><td>{@link #release()}</td><td>{@code basic.reject} with requeue</td><td>released</td></tr>
 * </table>
 *
 * <p>This would be a sealed interface with record implementations, but the published bytecode
 * targets Java 11; see ADR-015. The constructor is private so the set of outcomes stays
 * closed, and the {@code isX} methods let callers branch without instanceof chains.
 */
public abstract class Ack {

    private static final Accept ACCEPT = new Accept();
    private static final Release RELEASE = new Release();

    private Ack() {
        // Closed hierarchy: only the nested types below may extend this.
    }

    /**
     * The message was handled successfully and must not be delivered again.
     *
     * @return the shared accept outcome
     */
    public static Ack accept() {
        return ACCEPT;
    }

    /**
     * The attempt failed but is worth repeating, so the message goes to the next retry tier
     * with its attempt counter incremented.
     *
     * <p>The delay is a request, not a guarantee. The engine honours it exactly when the
     * broker supports delayed delivery, and otherwise rounds it to the nearest generated tier.
     *
     * @param after how long to wait before the next attempt; must not be negative
     * @param reason why the attempt failed, recorded for operators
     * @return a retry outcome
     */
    public static Ack retry(Duration after, String reason) {
        return new Retry(after, reason);
    }

    /**
     * The message can never be handled by this consumer, so it goes straight to the
     * dead-letter queue without consuming retry attempts.
     *
     * @param reason why the message is being dead-lettered, recorded on the message
     * @return a dead-letter outcome
     */
    public static Ack deadLetter(String reason) {
        return new DeadLetter(reason);
    }

    /**
     * This consumer will not handle the message, but another one should be given the chance.
     *
     * <p>The attempt counter is not incremented, so this is not a retry. It suits a consumer
     * shedding load or shutting down. Beware of returning it unconditionally: the message will
     * come straight back.
     *
     * @return the shared release outcome
     */
    public static Ack release() {
        return RELEASE;
    }

    /** @return {@code true} if this outcome accepts the message */
    public boolean isAccept() {
        return this instanceof Accept;
    }

    /** @return {@code true} if this outcome schedules another attempt */
    public boolean isRetry() {
        return this instanceof Retry;
    }

    /** @return {@code true} if this outcome dead-letters the message */
    public boolean isDeadLetter() {
        return this instanceof DeadLetter;
    }

    /** @return {@code true} if this outcome returns the message for another consumer */
    public boolean isRelease() {
        return this instanceof Release;
    }

    /** @return the requested retry delay, present only for a retry outcome */
    public Optional<Duration> delay() {
        return Optional.empty();
    }

    /** @return why the message was retried or dead-lettered, when a reason was given */
    public Optional<String> reason() {
        return Optional.empty();
    }

    /** Accepts the message. */
    private static final class Accept extends Ack {

        @Override
        public String toString() {
            return "Ack.accept";
        }
    }

    /** Schedules another attempt after a delay. */
    private static final class Retry extends Ack {

        private final Duration after;
        private final String reason;

        Retry(Duration after, String reason) {
            this.after = Objects.requireNonNull(after, "retry delay must not be null");
            this.reason = reason;
            if (after.isNegative()) {
                throw new IllegalArgumentException("retry delay must not be negative, was " + after);
            }
        }

        @Override
        public Optional<Duration> delay() {
            return Optional.of(after);
        }

        @Override
        public Optional<String> reason() {
            return Optional.ofNullable(reason);
        }

        @Override
        public String toString() {
            return "Ack.retry{after=" + after + ", reason=" + reason + "}";
        }
    }

    /** Sends the message to the dead-letter queue immediately. */
    private static final class DeadLetter extends Ack {

        private final String reason;

        DeadLetter(String reason) {
            this.reason = reason;
        }

        @Override
        public Optional<String> reason() {
            return Optional.ofNullable(reason);
        }

        @Override
        public String toString() {
            return "Ack.deadLetter{reason=" + reason + "}";
        }
    }

    /** Returns the message for another consumer without counting an attempt. */
    private static final class Release extends Ack {

        @Override
        public String toString() {
            return "Ack.release";
        }
    }
}
