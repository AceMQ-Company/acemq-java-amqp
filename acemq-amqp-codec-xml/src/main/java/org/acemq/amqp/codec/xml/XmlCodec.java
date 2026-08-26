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
package org.acemq.amqp.codec.xml;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import javax.xml.stream.XMLInputFactory;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

/**
 * Reads and writes XML.
 *
 * <p>Here because most estates have something that speaks XML and will not be rewritten, and a
 * messaging library that cannot talk to it forces a translation layer nobody wants to own. New
 * services should publish JSON; this exists so that the ones that cannot are not a special case.
 *
 * <p>Also the second implementation of {@link org.acemq.amqp.api.CodecProvider}, which is the
 * point. One implementation never proves a seam: it proves that a class compiles against an
 * interface written around it. Two, in separate modules, discovered the same way, is what shows
 * that a third can be added by somebody who does not work on this library.
 *
 * <p><strong>External entities are disabled.</strong> An XML parser that resolves them will read
 * files off the machine and open connections on behalf of whoever sent the message, and a queue
 * is exactly the sort of place a message from somewhere unexpected arrives. This is not
 * configurable, because the configuration would only ever be wrong.
 */
public final class XmlCodec implements Codec {

    private static final String CONTENT_TYPE = "application/xml";

    private final XmlMapper mapper;

    /** Uses a mapper that ignores unknown elements and refuses external entities. */
    public XmlCodec() {
        this(defaultMapper());
    }

    /**
     * @param mapper the mapper to use; disable external entity resolution on its factory, or a
     *     message from anywhere can read files off the machine it is handled on
     */
    public XmlCodec(XmlMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * @return a mapper that tolerates unknown elements and cannot be talked into resolving
     *     external entities
     */
    public static XmlMapper defaultMapper() {
        XMLInputFactory input = XMLInputFactory.newFactory();
        // Both of these, not one. Disabling DTD support alone still leaves parsers that will
        // follow an external general entity, and disabling entities alone leaves the billion
        // laughs expansion, which needs no network access to take a service down.
        input.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        input.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);

        XmlMapper mapper = new XmlMapper(new XmlFactory(input));
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
            throw new AceMqException("could not encode a " + payload.getClass().getName() + " as XML", e);
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
            // Unlike JSON, this does not volunteer for messages whose sender said nothing. XML
            // is rarely what arrives unannounced, and a codec that guesses wrong here turns a
            // readable message into a rejected one.
            return false;
        }
        String type = contentType.toLowerCase(Locale.ROOT);
        return type.startsWith(CONTENT_TYPE) || type.startsWith("text/xml") || type.contains("+xml");
    }

    @Override
    public String toString() {
        return "XmlCodec";
    }
}
