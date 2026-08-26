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

    private OutboundMessage(Builder builder) {
        this.exchange = builder.exchange == null ? "" : builder.exchange;
        this.routingKey = builder.routingKey == null ? "" : builder.routingKey;
        this.body = Objects.requireNonNull(builder.body, "body must not be null");
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        this.messageId = builder.messageId;
        this.contentType = builder.contentType;
        this.persistent = builder.persistent;
        this.mandatory = builder.mandatory;
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

        public OutboundMessage build() {
            return new OutboundMessage(this);
        }
    }
}
