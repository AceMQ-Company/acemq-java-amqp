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
package org.acemq.amqp.codec.yaml;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/**
 * Reads and writes YAML.
 *
 * <p>Chosen when a message is meant to be read by a person as much as by a program: a
 * configuration change broadcast to a fleet, a deployment instruction, a command replayed by hand
 * from a dead-letter queue. It is worth saying plainly that YAML costs more to parse than JSON
 * and is a poor choice for high volume; it earns its place where somebody will actually look at
 * the message.
 *
 * <p>Written in block style rather than flow style, which is the whole reason to pick YAML.
 * Flow style would produce something very close to JSON and would leave nothing to justify the
 * cost.
 *
 * <p><strong>This codec never volunteers for a message whose sender set no content type.</strong>
 * YAML is a superset of JSON, so its parser accepts JSON bytes quite happily and would answer for
 * messages meant for the JSON codec. It would even give the right value — while recording that a
 * YAML message had arrived, which is the sort of wrong that is discovered much later. So it
 * claims only content types that say YAML.
 */
public final class YamlCodec implements Codec {

    private static final String CONTENT_TYPE = "application/yaml";

    private final ObjectMapper mapper;

    /** Uses a mapper that writes block-style YAML and ignores unknown keys. */
    public YamlCodec() {
        this(defaultMapper());
    }

    /**
     * @param mapper the mapper to use; it must be built on a {@link YAMLFactory}, or this codec
     *     will announce YAML and write something else
     */
    public YamlCodec(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * @return a mapper that writes readable YAML and tolerates keys it does not know
     */
    public static ObjectMapper defaultMapper() {
        YAMLFactory factory = new YAMLFactory();
        // No leading "---". It is valid and it is noise, and a message body is a single document
        // by definition, so the marker separates nothing from nothing.
        factory.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
        // Quote only where quoting changes the meaning. Off, this quotes every string and gives
        // up exactly the readability that was the reason for choosing YAML.
        factory.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);

        ObjectMapper mapper = new ObjectMapper(factory);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
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
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (IOException e) {
            throw new AceMqException("could not encode a " + payload.getClass().getName() + " as YAML", e);
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
            return false;
        }
        String type = contentType.toLowerCase(Locale.ROOT);
        // application/yaml is the registered one as of RFC 9512; the other three predate it and
        // are what most senders still write.
        return type.startsWith(CONTENT_TYPE)
                || type.startsWith("text/yaml")
                || type.startsWith("application/x-yaml")
                || type.startsWith("text/x-yaml")
                || type.contains("+yaml");
    }

    @Override
    public String toString() {
        return "YamlCodec";
    }
}
