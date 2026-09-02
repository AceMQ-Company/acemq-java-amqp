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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;

/** A delivery presented to a handler. */
final class ReceivedMessage<T> implements Message<T> {

    private final T payload;
    private final Envelope envelope;
    private final String queue;
    private final String routingKey;
    private final Instant receivedAt;
    private final @org.jspecify.annotations.Nullable String replyTo;
    private final @org.jspecify.annotations.Nullable String contentType;

    ReceivedMessage(T payload, Envelope envelope, String queue, String routingKey, Instant receivedAt) {
        this(payload, envelope, queue, routingKey, receivedAt, null, null);
    }

    ReceivedMessage(
            T payload,
            Envelope envelope,
            String queue,
            String routingKey,
            Instant receivedAt,
            @org.jspecify.annotations.Nullable String replyTo,
            @org.jspecify.annotations.Nullable String contentType) {
        this.payload = payload;
        this.envelope = envelope;
        this.queue = queue;
        this.routingKey = routingKey;
        this.receivedAt = receivedAt;
        this.replyTo = replyTo;
        this.contentType = contentType;
    }

    @Override
    public Optional<String> replyTo() {
        return Optional.ofNullable(replyTo);
    }

    @Override
    public Optional<String> contentType() {
        return Optional.ofNullable(contentType);
    }

    @Override
    public T payload() {
        return payload;
    }

    @Override
    public Envelope envelope() {
        return envelope;
    }

    @Override
    public Map<String, Object> headers() {
        return envelope.headers();
    }

    @Override
    public Optional<String> routingKey() {
        return Optional.ofNullable(routingKey == null || routingKey.isEmpty() ? null : routingKey);
    }

    @Override
    public String queue() {
        return queue;
    }

    @Override
    public Instant receivedAt() {
        return receivedAt;
    }

    @Override
    public <R> Message<R> withPayload(R newPayload) {
        return new ReceivedMessage<>(newPayload, envelope, queue, routingKey, receivedAt, replyTo, contentType);
    }

    @Override
    public String toString() {
        return "Message{type=" + envelope.type() + ", id=" + envelope.id() + ", attempt=" + envelope.attempt()
                + ", queue=" + queue + "}";
    }
}
