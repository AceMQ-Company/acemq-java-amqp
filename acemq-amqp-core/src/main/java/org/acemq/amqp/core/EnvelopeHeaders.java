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
import java.util.LinkedHashMap;
import java.util.Map;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.Envelope;

/**
 * Flattens an {@link Envelope} into wire headers and reads it back.
 *
 * <p>This is the cross-language contract expressed as code. A Go publisher and an Elixir
 * consumer interoperate because every port performs exactly this mapping, so the rules here
 * are deliberately strict and forgiving in specific ways:
 *
 * <ul>
 *   <li>values are written as strings and integers only, since those survive every AMQP
 *       client's type mapping intact;
 *   <li>reading tolerates missing headers, because messages published by something other than
 *       AceMQ must still be consumable;
 *   <li>reading tolerates the wrong numeric type, because some brokers and clients hand back
 *       a {@code Long} where an {@code Integer} was written.
 * </ul>
 */
final class EnvelopeHeaders {

    private EnvelopeHeaders() {
        throw new AssertionError("EnvelopeHeaders is a utility and must not be instantiated");
    }

    /**
     * Writes an envelope into a header map, alongside the application's own headers.
     *
     * @param envelope envelope to flatten
     * @return headers ready to publish
     */
    static Map<String, Object> toHeaders(Envelope envelope) {
        Map<String, Object> headers = new LinkedHashMap<>(envelope.headers());
        headers.put(AceHeaders.ID, envelope.id());
        headers.put(AceHeaders.TYPE, envelope.type());
        headers.put(AceHeaders.VERSION, envelope.version());
        headers.put(AceHeaders.CORRELATION, envelope.correlationId());
        headers.put(AceHeaders.ATTEMPT, envelope.attempt());
        headers.put(AceHeaders.FIRST_SEEN, envelope.firstSeen().toEpochMilli());
        envelope.causationId().ifPresent(value -> headers.put(AceHeaders.CAUSATION, value));
        envelope.origin().ifPresent(value -> headers.put(AceHeaders.ORIGIN, value));
        return headers;
    }

    /**
     * Reconstructs an envelope from received headers.
     *
     * <p>A message with no AceMQ headers at all is still readable: it is treated as a first
     * attempt of an unknown type, which is what allows AceMQ consumers to be introduced to an
     * existing system one service at a time.
     *
     * @param headers headers as received, may be {@code null}
     * @param fallbackMessageId message id from the broker's own properties, used when the
     *     AceMQ identifier header is absent
     * @param fallbackType type to assume when the type header is absent
     * @return the reconstructed envelope
     */
    static Envelope fromHeaders(Map<String, Object> headers, String fallbackMessageId, String fallbackType) {
        Map<String, Object> source = headers == null ? new LinkedHashMap<>() : headers;

        String type = string(source.get(AceHeaders.TYPE));
        Envelope.Builder builder = Envelope.of(type != null ? type : fallbackType);

        String id = string(source.get(AceHeaders.ID));
        builder.id(id != null ? id : fallbackMessageId);

        Integer version = integer(source.get(AceHeaders.VERSION));
        if (version != null && version >= 1) {
            builder.version(version);
        }

        Integer attempt = integer(source.get(AceHeaders.ATTEMPT));
        if (attempt != null && attempt >= 1) {
            builder.attempt(attempt);
        }

        String correlation = string(source.get(AceHeaders.CORRELATION));
        if (correlation != null) {
            builder.correlationId(correlation);
        }

        String causation = string(source.get(AceHeaders.CAUSATION));
        if (causation != null) {
            builder.causationId(causation);
        }

        String origin = string(source.get(AceHeaders.ORIGIN));
        if (origin != null) {
            builder.origin(origin);
        }

        Long firstSeen = epochMillis(source.get(AceHeaders.FIRST_SEEN));
        if (firstSeen != null) {
            builder.firstSeen(Instant.ofEpochMilli(firstSeen));
        }

        // Application headers only: the AceMQ ones are already represented as fields, and
        // copying them twice would let the two representations drift apart.
        source.forEach((name, value) -> {
            if (!AceHeaders.isAceHeader(name)) {
                builder.header(name, value);
            }
        });

        return builder.build();
    }

    /**
     * Reads a header as text.
     *
     * <p>The RabbitMQ client hands back {@code LongString} rather than {@code String} for
     * anything long, and it is not a {@code CharSequence}, so {@code toString()} is the only
     * portable way to read it.
     */
    private static String string(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text.isEmpty() ? null : text;
    }

    private static Integer integer(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value == null) {
            return null;
        }
        try {
            return Integer.valueOf(value.toString().trim());
        } catch (NumberFormatException e) {
            // A malformed counter must not stop the message being delivered; the engine
            // falls back to treating it as a first attempt.
            return null;
        }
    }

    private static Long epochMillis(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
