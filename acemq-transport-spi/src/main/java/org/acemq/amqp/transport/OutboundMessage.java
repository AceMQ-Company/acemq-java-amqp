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
package org.acemq.amqp.transport;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * A message on its way to the broker, already encoded.
 *
 * <p>The transport does not know what the bytes mean, and does not add AceMQ headers: the
 * core has already flattened the envelope into {@link #headers()} before handing it over.
 */
public final class OutboundMessage {

    private final String exchange;
    private final String routingKey;
    private final byte[] body;
    private final Map<String, Object> headers;
    private final @Nullable String messageId;
    private final @Nullable String contentType;
    private final boolean persistent;
    private final boolean mandatory;
    private final @Nullable Duration expiration;
    private final @Nullable Integer priority;

    private OutboundMessage(Builder builder) {
        this.exchange = builder.exchange == null ? "" : builder.exchange;
        this.routingKey = builder.routingKey == null ? "" : builder.routingKey;
        this.body = Objects.requireNonNull(builder.body, "body must not be null");
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        this.messageId = builder.messageId;
        this.contentType = builder.contentType;
        this.persistent = builder.persistent;
        this.mandatory = builder.mandatory;
        this.expiration = builder.expiration;
        this.priority = builder.priority;
    }

    /**
     * @param body encoded payload
     * @return a builder defaulting to a persistent, mandatory publish
     */
    public static Builder body(byte[] body) {
        return new Builder().body(body);
    }

    /** @return target exchange, or the empty string to publish directly to a queue */
    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    /** @return the encoded payload; the array is shared, so callers must not modify it */
    public byte[] body() {
        return body;
    }

    public Map<String, Object> headers() {
        return headers;
    }

    public @Nullable String messageId() {
        return messageId;
    }

    public @Nullable String contentType() {
        return contentType;
    }

    /** @return whether the broker should persist the message across a restart */
    public boolean persistent() {
        return persistent;
    }

    /**
     * @return whether an unroutable message must be returned rather than dropped
     * @see #persistent()
     */
    public boolean mandatory() {
        return mandatory;
    }

    /**
     * How long the message stays worth delivering.
     *
     * <p>Per-message, and enforced by the broker rather than by anything here. A transport that
     * cannot express it must say so rather than dropping it: a message expected to expire in five
     * minutes and instead kept forever is a slow leak, and one delivered long after it stopped
     * meaning anything is worse than one never delivered.
     *
     * @return the time-to-live, when one was set
     */
    public java.util.Optional<Duration> expiration() {
        return java.util.Optional.ofNullable(expiration);
    }

    /**
     * @return the priority this message was published with, if any
     */
    public java.util.Optional<Integer> priority() {
        return java.util.Optional.ofNullable(priority);
    }

    @Override
    public String toString() {
        return "OutboundMessage{exchange=" + exchange + ", routingKey=" + routingKey + ", bytes="
                + body.length + ", messageId=" + messageId + "}";
    }

    /** Builds {@link OutboundMessage} instances. */
    public static final class Builder {

        private @Nullable String exchange;
        private @Nullable String routingKey;
        private byte @Nullable [] body;
        private final Map<String, Object> headers = new LinkedHashMap<>();
        private @Nullable String messageId;
        private @Nullable String contentType;
        private boolean persistent = true;
        private boolean mandatory = true;
        private @Nullable Duration expiration;
        private @Nullable Integer priority;

        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public Builder body(byte[] body) {
            this.body = body;
            return this;
        }

        public Builder header(String name, Object value) {
            this.headers.put(name, value);
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            if (headers != null) {
                this.headers.putAll(headers);
            }
            return this;
        }

        public Builder messageId(@Nullable String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder contentType(@Nullable String contentType) {
            this.contentType = contentType;
            return this;
        }

        /** Marks the message transient, so a broker restart may lose it. */
        public Builder transientDelivery() {
            this.persistent = false;
            return this;
        }

        /** Allows the broker to drop the message when nothing is bound to receive it. */
        public Builder allowUnroutable() {
            this.mandatory = false;
            return this;
        }

        /**
         * Asks the broker to discard the message if it is still undelivered after this long.
         *
         * @param expiration positive time-to-live, or null for none
         */
        public Builder expiration(@Nullable Duration expiration) {
            if (expiration != null && (expiration.isNegative() || expiration.isZero())) {
                throw new IllegalArgumentException("expiration must be positive, was " + expiration);
            }
            this.expiration = expiration;
            return this;
        }

        /**
         * Sets the priority this message is published with.
         *
         * <p>Only meaningful on a queue declared with a maximum priority; elsewhere the broker
         * ignores it. Higher is more urgent.
         *
         * @param priority priority to publish with, or {@code null} for none
         * @return this builder
         */
        public Builder priority(@Nullable Integer priority) {
            this.priority = priority;
            return this;
        }

        public OutboundMessage build() {
            return new OutboundMessage(this);
        }
    }
}
