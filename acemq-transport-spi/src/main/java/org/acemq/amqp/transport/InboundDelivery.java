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

import org.jspecify.annotations.Nullable;

/** A message handed up from the broker, still encoded. */
public final class InboundDelivery {

    private final String queue;
    private final String exchange;
    private final String routingKey;
    private final byte[] body;
    private final Map<String, Object> headers;
    private final @Nullable String messageId;
    private final @Nullable String contentType;
    private final boolean redelivered;

    public InboundDelivery(
            String queue,
            String exchange,
            String routingKey,
            byte[] body,
            @Nullable Map<String, Object> headers,
            @Nullable String messageId,
            @Nullable String contentType,
            boolean redelivered) {
        this.queue = queue;
        this.exchange = exchange == null ? "" : exchange;
        this.routingKey = routingKey == null ? "" : routingKey;
        this.body = body;
        this.headers = headers == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.messageId = messageId;
        this.contentType = contentType;
        this.redelivered = redelivered;
    }

    public String queue() {
        return queue;
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    public byte[] body() {
        return body;
    }

    /**
     * @return the message headers
     * @implSpec values must be portable types only: {@link String}, {@link Number},
     *     {@link Boolean}, or a {@link java.util.List} or {@link Map} of those. Client
     *     libraries expose their own wrappers — the RabbitMQ client hands back
     *     {@code LongString} rather than {@code String} — and converting them is the
     *     transport's job. The core compares header values against ordinary Java types, and a
     *     client-specific wrapper that reaches it fails those comparisons in a way that is
     *     tedious to diagnose.
     */
    public Map<String, Object> headers() {
        return headers;
    }

    public @Nullable String messageId() {
        return messageId;
    }

    public @Nullable String contentType() {
        return contentType;
    }

    /**
     * @return the broker's redelivery flag
     * @implNote this is not the AceMQ attempt counter. A broker sets it after any recovery,
     *     including one unrelated to a handler failure, so it is a hint for operators rather
     *     than something retry policy should be built on.
     */
    public boolean redelivered() {
        return redelivered;
    }

    @Override
    public String toString() {
        return "InboundDelivery{queue=" + queue + ", routingKey=" + routingKey + ", bytes="
                + (body == null ? 0 : body.length) + "}";
    }
}
