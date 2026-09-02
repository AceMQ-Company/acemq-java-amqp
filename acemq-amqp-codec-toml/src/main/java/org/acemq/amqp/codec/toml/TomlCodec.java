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
package org.acemq.amqp.codec.toml;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.toml.TomlFactory;
import com.fasterxml.jackson.dataformat.toml.TomlMapper;

/**
 * Reads and writes TOML.
 *
 * <p>The same audience as YAML — a message a person will read and edit — with the ambiguity
 * removed. TOML has one way to write a string, no significant indentation, and no Norway problem:
 * {@code country = NO} is a bare key error rather than a boolean that used to be a country. Where
 * a human edits the message and a machine acts on it, that matters more than terseness.
 *
 * <p>Reach for it for configuration broadcast to a fleet, feature flags, deployment instructions
 * and anything replayed by hand from a dead-letter queue. It is a poor choice for high volume:
 * it is text, it is larger than JSON, and it parses more slowly.
 *
 * <p><strong>The shape of the data has to suit it.</strong> TOML is a table format, so a message
 * body must be an object at the top level — a bare list or a bare number is not a TOML document
 * and this codec will say so rather than inventing a wrapper. Deep nesting reads poorly too;
 * where the payload is a tree rather than a table, JSON is the honest answer.
 *
 * <p>Like the YAML codec, this one <strong>never volunteers for a message whose sender set no
 * content type</strong>. Guessing wrong here would record a TOML message arriving where a JSON
 * one did.
 */
public final class TomlCodec implements Codec {

    /** The registered type, as of the TOML specification's IANA entry. */
    private static final String CONTENT_TYPE = "application/toml";

    private final ObjectMapper mapper;

    /** Uses a mapper that ignores unknown keys and writes dates as text. */
    public TomlCodec() {
        this(defaultMapper());
    }

    /**
     * @param mapper the mapper to use; it must be built on a {@link TomlFactory}, or this codec
     *     will announce TOML and write something else
     */
    public TomlCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * @return a mapper that tolerates keys it does not know and writes readable timestamps
     */
    public static ObjectMapper defaultMapper() {
        ObjectMapper mapper = TomlMapper.builder().build();
        // A consumer on an older version of the message must not fail on a field somebody
        // added. This is the single most important setting for a format people hand-edit.
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // Numbers-as-dates in a file a person reads defeats the point of choosing TOML.
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.findAndRegisterModules();
        return mapper;
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        // Checked rather than left to Jackson, because Jackson does not fail here -- it writes
        // a key-less assignment. A list comes out as " = ['a', 'b']", which is not TOML, and
        // its own parser refuses it with "Got KEY_VAL_SEP, expected key or table". Publishing
        // that produces a message nothing can read, discovered by the consumer rather than by
        // the publisher, which is the wrong end.
        if (!mapper.valueToTree(payload).isObject()) {
            throw new AceMqException("cannot encode a " + payload.getClass().getName() + " as TOML: a TOML"
                    + " document is a table, so the top level has to be an object. A list, a string or a"
                    + " number has no TOML representation -- wrap it in an object with a named field, or"
                    + " use JSON.");
        }
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (IOException e) {
            throw new AceMqException("could not encode a " + payload.getClass().getName() + " as TOML", e);
        }
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target) {
        try {
            return mapper.readValue(body, target);
        } catch (IOException e) {
            throw new AceMqException("could not decode a message as " + target.getName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean canDecode(@Nullable String contentType) {
        if (contentType == null) {
            // Same reasoning as YAML: a sender that said nothing is almost always sending JSON,
            // and answering for it would be right about the value and wrong about the format.
            return false;
        }
        String type = contentType.toLowerCase(Locale.ROOT);
        return type.startsWith(CONTENT_TYPE)
                // Predates the registration and is still what a lot of tooling writes.
                || type.startsWith("text/toml")
                || type.contains("+toml");
    }

    @Override
    public String toString() {
        return "TomlCodec";
    }
}
