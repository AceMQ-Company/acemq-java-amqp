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
import org.jspecify.annotations.Nullable;

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
        envelope.route().ifPresent(slip -> headers.putAll(slip.toHeaders()));
        envelope.causationId().ifPresent(value -> headers.put(AceHeaders.CAUSATION, value));
        envelope.origin().ifPresent(value -> headers.put(AceHeaders.ORIGIN, value));
        envelope.error().ifPresent(value -> headers.put(AceHeaders.ERROR, value));
        // The replay three live in the shared namespace, so they are already in the map above if
        // the envelope came off the wire. Written over the top rather than around, so the field
        // and the header cannot say different things about the same message.
        envelope.replayedFrom().ifPresent(value -> headers.put(AceHeaders.REPLAYED_FROM, value));
        // RFC 3339, which is what Go, Python and Ruby write. Java wrote epoch milliseconds and
        // was alone in it: a header with one name and two encodings is a header every consumer
        // has to guess at, and the guess is only ever right by luck. Both are still read — see
        // instantMillis — because a message published by an older Java service is still out
        // there and still has to be understood.
        envelope.replayedAt().ifPresent(value -> headers.put(AceHeaders.REPLAYED_AT, rfc3339(value)));
        if (envelope.replayCount() > 0) {
            // Omitted when zero, so an ordinary message carries no evidence of a loop it was
            // never in and the header only appears where it means something.
            headers.put(AceHeaders.REPLAY_COUNT, envelope.replayCount());
        }
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
    static Envelope fromHeaders(
            @Nullable Map<String, Object> headers, @Nullable String fallbackMessageId, String fallbackType) {
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

        String replayedFrom = string(source.get(AceHeaders.REPLAYED_FROM));
        if (replayedFrom != null) {
            builder.replayedFrom(replayedFrom);
        }

        Long replayedAt = instantMillis(source.get(AceHeaders.REPLAYED_AT));
        if (replayedAt != null) {
            builder.replayedAt(Instant.ofEpochMilli(replayedAt));
        }

        Integer replayCount = integer(source.get(AceHeaders.REPLAY_COUNT));
        if (replayCount != null && replayCount > 0) {
            builder.replayCount(replayCount);
        }

        String error = string(source.get(AceHeaders.ERROR));
        if (error != null) {
            builder.error(error);
        }

        Long firstSeen = epochMillis(source.get(AceHeaders.FIRST_SEEN));
        if (firstSeen != null) {
            builder.firstSeen(Instant.ofEpochMilli(firstSeen));
        }

        // Application headers only: the AceMQ ones are already represented as fields, and
        // copying them twice would let the two representations drift apart.
        source.forEach((name, value) -> {
            // Engine-owned headers become fields on the envelope, so copying them here as well
            // would let the two representations drift apart. The routing slip is the exception:
            // it shares the prefix but belongs to the application's message rather than to the
            // engine, and a step that could not read where it was going would be no step at all.
            if (!AceHeaders.isAceHeader(name)) {
                builder.header(name, value);
            }
        });

        org.acemq.amqp.api.RoutingSlip.from(source).ifPresent(builder::route);

        return builder.build();
    }

    /**
     * Reads a header as text.
     *
     * <p>The RabbitMQ client hands back {@code LongString} rather than {@code String} for
     * anything long, and it is not a {@code CharSequence}, so {@code toString()} is the only
     * portable way to read it.
     */
    private static @Nullable String string(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text.isEmpty() ? null : text;
    }

    private static @Nullable Integer integer(@Nullable Object value) {
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

    /**
     * Reads a replay timestamp, as epoch milliseconds or as an ISO-8601 instant.
     *
     * <p>Java writes the number and the Go, Python and Ruby replays write the text, and now that
     * all four write it under the same name a Java consumer meets both. Refusing the text would
     * mean a message replayed by another language arriving with {@code replayedAt} empty and no
     * indication why — the one field an operator looks at to see when a message was put back.
     */
    /**
     * Renders an instant the way the other four libraries render one.
     *
     * <p>{@code Instant.toString} is RFC 3339 and prints sub-second digits only when there are
     * any, so a whole second comes out as {@code 2026-02-03T04:05:06Z} — byte for byte what Go's
     * {@code time.RFC3339} and Ruby's {@code strftime} produce for the same moment.
     */
    static String rfc3339(Instant at) {
        return at.toString();
    }

    private static @Nullable Long instantMillis(@Nullable Object value) {
        Long millis = epochMillis(value);
        if (millis != null || value == null) {
            return millis;
        }
        String text = value.toString().trim();
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (java.time.format.DateTimeParseException e) {
            // Not a Z-terminated instant. Python writes an explicit +00:00 offset instead, which
            // is equally valid RFC 3339 and which Instant.parse refuses on a Java 11 runtime.
            // Parsing it as an offset date-time is the one reading that covers all four.
            try {
                return java.time.OffsetDateTime.parse(text).toInstant().toEpochMilli();
            } catch (java.time.format.DateTimeParseException stillNot) {
                // Neither form. Dropping it loses an audit field; failing here would lose the
                // message, which is the worse of the two.
                return null;
            }
        }
    }

    private static @Nullable Long epochMillis(@Nullable Object value) {
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
