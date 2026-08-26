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
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * One message waiting in the outbox: where it is going, what it says, and how it has fared.
 *
 * <p>The payload is text rather than bytes, which is a deliberate choice with a cost. The cost
 * is that a binary codec cannot use this store as it stands. The gain is that the outbox table
 * can be read with SQL, and an operator asking "why has this order not been published?" at three
 * in the morning can answer it with a select rather than a hex dump. Every codec AceMQ ships
 * today produces text, so nothing is lost yet, and a bytes column can be added beside this one
 * when that changes.
 *
 * <p>The envelope is reduced to its identifying fields — id, type, correlation and causation —
 * rather than stored whole. Arbitrary headers would need a serialisation format, and inventing
 * one here to avoid depending on a codec would be the worse trade. Until the JSON codec lands, a
 * message published through the outbox carries these four fields and no custom headers.
 */
public final class OutboxRecord {

    private final String id;
    private final String exchange;
    private final String routingKey;
    private final String type;
    private final String payload;
    private final @Nullable String correlationId;
    private final @Nullable String causationId;
    private final Instant createdAt;
    private final int attempts;
    private final @Nullable String lastError;

    /**
     * Full constructor, used when reading a record back out of storage.
     *
     * @param id message identifier, unique across the outbox
     * @param exchange exchange to publish to; empty string for the default exchange
     * @param routingKey routing key to publish with
     * @param type logical message type
     * @param payload encoded message body
     * @param correlationId correlation identifier, or {@code null}
     * @param causationId causation identifier, or {@code null}
     * @param createdAt when the record was written
     * @param attempts how many publish attempts have been made
     * @param lastError why the last attempt failed, or {@code null}
     */
    public OutboxRecord(
            String id,
            String exchange,
            String routingKey,
            String type,
            String payload,
            @Nullable String correlationId,
            @Nullable String causationId,
            Instant createdAt,
            int attempts,
            @Nullable String lastError) {
        this.id = Objects.requireNonNull(id, "id");
        this.exchange = Objects.requireNonNull(exchange, "exchange");
        this.routingKey = Objects.requireNonNull(routingKey, "routingKey");
        this.type = Objects.requireNonNull(type, "type");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.correlationId = correlationId;
        this.causationId = causationId;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        this.attempts = attempts;
        this.lastError = lastError;
    }

    /**
     * A new record for a message that has not been attempted yet.
     *
     * @param exchange exchange to publish to; empty string for the default exchange
     * @param routingKey routing key to publish with
     * @param envelope envelope the message will carry; its id becomes the record's id
     * @param payload encoded message body
     * @return the record to hand to {@link OutboxStore#add}
     */
    public static OutboxRecord of(String exchange, String routingKey, Envelope envelope, String payload) {
        Objects.requireNonNull(envelope, "envelope");
        return new OutboxRecord(
                envelope.id(),
                exchange,
                routingKey,
                envelope.type(),
                payload,
                envelope.correlationId(),
                envelope.causationId().orElse(null),
                Instant.now(),
                0,
                null);
    }

    /**
     * Rebuilds the envelope this record should be published with.
     *
     * <p>The identifier is carried through rather than regenerated, so a relay that publishes
     * the same record twice — which it will, if it dies between the broker's confirm and the
     * database commit — sends the same identifier both times and an idempotent consumer can
     * recognise the second as a duplicate.
     *
     * @return the envelope to publish with
     */
    public Envelope envelope() {
        return Envelope.of(type)
                .id(id)
                .correlationId(correlationId)
                .causationId(causationId)
                .build();
    }

    /** @return message identifier, unique across the outbox */
    public String id() {
        return id;
    }

    /** @return exchange to publish to; empty string for the default exchange */
    public String exchange() {
        return exchange;
    }

    /** @return routing key to publish with */
    public String routingKey() {
        return routingKey;
    }

    /** @return logical message type */
    public String type() {
        return type;
    }

    /** @return encoded message body */
    public String payload() {
        return payload;
    }

    /** @return correlation identifier, if the message carries one */
    public Optional<String> correlationId() {
        return Optional.ofNullable(correlationId);
    }

    /** @return causation identifier, if the message carries one */
    public Optional<String> causationId() {
        return Optional.ofNullable(causationId);
    }

    /** @return when the record was written */
    public Instant createdAt() {
        return createdAt;
    }

    /** @return how many publish attempts have been made */
    public int attempts() {
        return attempts;
    }

    /** @return why the last attempt failed, if one has */
    public Optional<String> lastError() {
        return Optional.ofNullable(lastError);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OutboxRecord)) {
            return false;
        }
        OutboxRecord that = (OutboxRecord) other;
        return id.equals(that.id) && attempts == that.attempts;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, attempts);
    }

    @Override
    public String toString() {
        return "OutboxRecord{id=" + id + ", to=" + exchange + "/" + routingKey + ", type=" + type + ", attempts="
                + attempts + "}";
    }
}
