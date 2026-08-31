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

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

/**
 * The AceMQ metadata that travels with every message, independent of payload and broker.
 *
 * <p>An envelope is immutable. The engine derives a new one rather than mutating an existing
 * one, which is what makes an attempt counter trustworthy when the same message is delivered
 * several times.
 *
 * <p>This is a value type. It would be a {@code record}, but the published bytecode targets
 * Java 11 so that Spring Boot 2.7 applications can consume the library; see ADR-015.
 *
 * @see AceHeaders for the wire names these fields map onto
 */
public final class Envelope {

    private final String id;
    private final String type;
    private final int version;
    private final String correlationId;
    private final @Nullable String causationId;
    private final int attempt;
    private final Instant firstSeen;
    private final @Nullable String origin;
    private final @Nullable String error;
    private final @Nullable RoutingSlip route;
    private final @Nullable String replayedFrom;
    private final @Nullable Instant replayedAt;
    private final int replayCount;
    private final Map<String, Object> headers;

    private Envelope(Builder builder) {
        this.id = builder.id != null ? builder.id : UUID.randomUUID().toString();
        this.type = Objects.requireNonNull(builder.type, "type must not be null");
        this.version = builder.version;
        this.correlationId = builder.correlationId != null ? builder.correlationId : this.id;
        this.causationId = builder.causationId;
        this.attempt = builder.attempt;
        this.firstSeen = builder.firstSeen != null ? builder.firstSeen : Instant.now();
        this.origin = builder.origin;
        this.error = builder.error;
        this.route = builder.route;
        this.replayedFrom = builder.replayedFrom;
        this.replayedAt = builder.replayedAt;
        this.replayCount = builder.replayCount;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        if (this.version < 1) {
            throw new IllegalArgumentException("version must be at least 1, was " + this.version);
        }
        if (this.attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1, was " + this.attempt);
        }
        if (this.replayCount < 0) {
            throw new IllegalArgumentException("replayCount must not be negative, was " + this.replayCount);
        }
    }

    /**
     * Starts a new envelope for the given logical message type.
     *
     * @param type logical message type, for example {@code order.placed}
     * @return a builder with an identifier and timestamp already generated
     */
    public static Builder of(String type) {
        return new Builder().type(type);
    }

    /** @return the unique message identifier, also the default idempotency key */
    public String id() {
        return id;
    }

    /** @return the logical message type */
    public String type() {
        return type;
    }

    /** @return the payload schema version, at least 1 */
    public int version() {
        return version;
    }

    /** @return the business correlation identifier, defaulting to {@link #id()} */
    public String correlationId() {
        return correlationId;
    }

    /** @return the identifier of the message that caused this one, if any */
    public Optional<String> causationId() {
        return Optional.ofNullable(causationId);
    }

    /** @return the delivery attempt number, starting at 1 for a first delivery */
    public int attempt() {
        return attempt;
    }

    /** @return when this message was first published, used for age-based give-up */
    public Instant firstSeen() {
        return firstSeen;
    }

    /** @return the publishing process, conventionally {@code service@host}, if known */
    public Optional<String> origin() {
        return Optional.ofNullable(origin);
    }

    /**
     * Why this message was dead-lettered or parked.
     *
     * <p>Present only on a message read back from a dead-letter or parking queue. It is an
     * envelope field rather than an ordinary header because AceMQ owns the {@code x-acemq-}
     * namespace: were it left as a header it would be filtered out of {@link #headers()} and
     * become invisible to the very code that needs it most.
     *
     * @return the failure reason recorded when the message was given up on
     */
    public Optional<String> error() {
        return Optional.ofNullable(error);
    }

    /**
     * Where this message was replayed from, when it was.
     *
     * <p>Set by {@code mq.replay(...)} when a message is moved back out of a dead-letter queue or
     * parking lot. A first-class field rather than a header because engine-owned headers are
     * stripped before an application sees them: left as a header, the one piece of evidence that
     * this message has been round the loop before would be invisible to the code handling it.
     *
     * @return the queue it came back from, when it is a replay
     */
    public Optional<String> replayedFrom() {
        return Optional.ofNullable(replayedFrom);
    }

    /** @return when it was last replayed, when it is a replay */
    public Optional<Instant> replayedAt() {
        return Optional.ofNullable(replayedAt);
    }

    /**
     * @return how many times this message has been replayed; zero for one that never has.
     *     Distinct from {@link #attempt()}, which a replay resets — a message on its fifth trip
     *     through a dead-letter queue would otherwise look brand new
     */
    public int replayCount() {
        return replayCount;
    }

    /**
     * Where this message is going, when it is travelling a pipeline.
     *
     * <p>A field rather than a header, for the same reason the error is one: the AceMQ prefix is
     * closed to application headers, so a slip put there by hand would be silently dropped on
     * the way in. A step reads its route from here.
     *
     * @return the routing slip, or empty when the message is not part of a pipeline
     */
    public Optional<RoutingSlip> route() {
        return Optional.ofNullable(route);
    }

    /** @return application headers, excluding the AceMQ-owned ones; never {@code null} */
    public Map<String, Object> headers() {
        return headers;
    }

    /**
     * Returns the age of the message, measured from its first publish.
     *
     * <p>Retry policies use this to abandon a message that has been circulating too long,
     * regardless of how many attempts remain.
     *
     * @return elapsed time since {@link #firstSeen()}
     */
    public java.time.Duration age() {
        return java.time.Duration.between(firstSeen, Instant.now());
    }

    /**
     * Returns a copy of this envelope representing the next delivery attempt.
     *
     * <p>Everything else is preserved, so {@link #firstSeen()} continues to measure from the
     * original publish rather than from the retry.
     *
     * @return a new envelope with {@link #attempt()} incremented by one
     */
    public Envelope nextAttempt() {
        return toBuilder().attempt(attempt + 1).build();
    }

    /**
     * Returns a builder for a message caused by this one.
     *
     * <p>The correlation identifier is carried across so a whole flow can be followed, and
     * the causation identifier is set to this message's identifier so the immediate parent is
     * recorded. The attempt counter restarts, because the new message is a first publish.
     *
     * @param type logical message type of the new message
     * @return a builder pre-wired for causation tracking
     */
    public Builder causing(String type) {
        return new Builder().type(type).correlationId(correlationId).causationId(id).origin(origin);
    }

    /** @return a builder initialised with every value from this envelope */
    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .type(type)
                .version(version)
                .correlationId(correlationId)
                .causationId(causationId)
                .attempt(attempt)
                .firstSeen(firstSeen)
                .origin(origin)
                .error(error)
                .route(route)
                .replayedFrom(replayedFrom)
                .replayedAt(replayedAt)
                .replayCount(replayCount)
                .headers(headers);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Envelope)) {
            return false;
        }
        Envelope that = (Envelope) other;
        return version == that.version
                && attempt == that.attempt
                && id.equals(that.id)
                && type.equals(that.type)
                && correlationId.equals(that.correlationId)
                && Objects.equals(causationId, that.causationId)
                && firstSeen.equals(that.firstSeen)
                && Objects.equals(origin, that.origin)
                && Objects.equals(error, that.error)
                && Objects.equals(replayedFrom, that.replayedFrom)
                && Objects.equals(replayedAt, that.replayedAt)
                && replayCount == that.replayCount
                && headers.equals(that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id, type, version, correlationId, causationId, attempt, firstSeen, origin, error,
                replayedFrom, replayedAt, replayCount, headers);
    }

    @Override
    public String toString() {
        return "Envelope{id=" + id + ", type=" + type + ", version=" + version + ", correlationId=" + correlationId
                + ", attempt=" + attempt + "}";
    }

    /** Builds {@link Envelope} instances. Not thread safe; use one builder per envelope. */
    public static final class Builder {

        private @Nullable String id;
        private @Nullable String type;
        private int version = 1;
        private @Nullable String correlationId;
        private @Nullable String causationId;
        private int attempt = 1;
        private @Nullable Instant firstSeen;
        private @Nullable String origin;
        private @Nullable String error;
        private @Nullable String replayedFrom;
        private @Nullable Instant replayedAt;
        private int replayCount;
        private final Map<String, Object> headers = new LinkedHashMap<>();

        /** @param id message identifier; a random UUID is generated when left unset */
        public Builder id(@Nullable String id) {
            this.id = id;
            return this;
        }

        /** @param type logical message type; required */
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        /** @param version payload schema version, at least 1; defaults to 1 */
        public Builder version(int version) {
            this.version = version;
            return this;
        }

        /** @param correlationId flow identifier; defaults to the message identifier */
        public Builder correlationId(@Nullable String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        /** @param causationId identifier of the message that caused this one */
        public Builder causationId(@Nullable String causationId) {
            this.causationId = causationId;
            return this;
        }

        /** @param attempt delivery attempt, at least 1; defaults to 1 */
        public Builder attempt(int attempt) {
            this.attempt = attempt;
            return this;
        }

        /** @param firstSeen first publish time; defaults to now */
        public Builder firstSeen(@Nullable Instant firstSeen) {
            this.firstSeen = firstSeen;
            return this;
        }

        /** @param origin publishing process, conventionally {@code service@host} */
        public Builder origin(@Nullable String origin) {
            this.origin = origin;
            return this;
        }

        /** @param error why the message was dead-lettered or parked */
        public Builder error(@Nullable String error) {
            this.error = error;
            return this;
        }

        /** @param replayedFrom queue this message was replayed out of */
        public Builder replayedFrom(@Nullable String replayedFrom) {
            this.replayedFrom = replayedFrom;
            return this;
        }

        /** @param replayedAt when it was replayed */
        public Builder replayedAt(@Nullable Instant replayedAt) {
            this.replayedAt = replayedAt;
            return this;
        }

        /** @param replayCount how many times it has been replayed; must not be negative */
        public Builder replayCount(int replayCount) {
            this.replayCount = replayCount;
            return this;
        }

        /**
         * Adds one application header.
         *
         * @param name header name; must not use the AceMQ prefix
         * @param value header value
         * @throws IllegalArgumentException if the name is AceMQ-owned
         */
        private @Nullable RoutingSlip route;

        /**
         * @param route where the message is going, when it is travelling a pipeline
         * @return this builder
         */
        public Builder route(@Nullable RoutingSlip route) {
            this.route = route;
            return this;
        }

        public Builder header(String name, Object value) {
            if (AceHeaders.isAceHeader(name)) {
                throw new IllegalArgumentException("header '" + name
                        + "' is owned by AceMQ and is derived from the envelope, so it cannot be set directly");
            }
            this.headers.put(name, value);
            return this;
        }

        /** @param headers application headers to add; AceMQ-owned names are ignored */
        public Builder headers(@Nullable Map<String, Object> headers) {
            if (headers != null) {
                headers.forEach((name, value) -> {
                    if (!AceHeaders.isAceHeader(name)) {
                        this.headers.put(name, value);
                    }
                });
            }
            return this;
        }

        /**
         * @return the built envelope
         * @throws NullPointerException if no type was set
         * @throws IllegalArgumentException if the version or attempt is below 1
         */
        public Envelope build() {
            return new Envelope(this);
        }
    }
}
