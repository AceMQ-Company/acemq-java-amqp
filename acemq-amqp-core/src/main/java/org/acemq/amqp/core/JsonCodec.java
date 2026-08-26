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

import java.io.IOException;
import java.util.Objects;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Reads and writes JSON, using Jackson.
 *
 * <p>Opt-in rather than automatic. Jackson is on nearly every classpath by accident, so a core
 * that detected it and switched would change the format on the wire for applications that had
 * asked for nothing — a silent change to a contract other services depend on. The default stays
 * {@link StringCodec}; this is used when it is named.
 *
 * <p>Jackson itself is an optional dependency, which means this class exists in the jar whether
 * or not the application brings Jackson. That is safe because nothing else in the engine
 * mentions it: the class is loaded only when an application refers to it, and an application
 * that refers to it has Jackson.
 *
 * <p>Two settings of the default mapper are decisions rather than taste, and both are about
 * messages outliving the code that wrote them.
 *
 * <p>Unknown properties are ignored. A producer that adds a field must not break every consumer
 * deployed before it; refusing the message would make every additive change a coordinated
 * release across teams, which is the coupling messaging exists to avoid.
 *
 * <p>Dates are written as ISO-8601 text rather than as numbers. A timestamp of {@code 1735689600}
 * needs a shared assumption about units and epoch to read; {@code 2025-01-01T00:00:00Z} does not,
 * which matters when the consumer is in Go or Python and reading a millisecond count as seconds
 * puts a message fifty thousand years in the past.
 *
 * <p>An application with its own configured mapper should pass it, so the messages match what the
 * rest of the service produces.
 */
public final class JsonCodec implements Codec {

    private static final String CONTENT_TYPE = "application/json";

    private final ObjectMapper mapper;

    /** Uses a mapper configured as described above. */
    public JsonCodec() {
        this(defaultMapper());
    }

    /**
     * @param mapper the mapper to use; take care that it tolerates unknown properties, or a
     *     producer adding a field will start rejecting messages at every consumer
     */
    public JsonCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * @return a mapper configured for messages that outlive the code that wrote them
     */
    public static ObjectMapper defaultMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Picks up jackson-datatype-jsr310 and anything else on the classpath, so an Instant
        // serialises as a date rather than as the innards of the class.
        mapper.findAndRegisterModules();
        return mapper;
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (IOException e) {
            throw new AceMqException("could not encode a " + payload.getClass().getName() + " as JSON", e);
        }
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target) {
        try {
            return mapper.readValue(body, target);
        } catch (IOException e) {
            // Wrapped rather than propagated: the engine distinguishes a payload that will never
            // decode from a handler that failed once, and only the first is worth refusing
            // outright instead of retrying.
            throw new AceMqException("could not decode a message as " + target.getName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean canDecode(@Nullable String contentType) {
        if (contentType == null) {
            // The sender said nothing, which is usual outside the JVM and from anything
            // publishing through a management console. Refusing on that basis would make this
            // codec useless in the interoperating case it mostly exists for.
            return true;
        }
        String type = contentType.toLowerCase(java.util.Locale.ROOT);
        return type.startsWith(CONTENT_TYPE) || type.startsWith("application/") && type.contains("+json");
    }

    @Override
    public String toString() {
        return "JsonCodec";
    }
}
