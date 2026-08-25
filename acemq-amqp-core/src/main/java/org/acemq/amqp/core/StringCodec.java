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

import java.nio.charset.StandardCharsets;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;

/**
 * Reads and writes UTF-8 text.
 *
 * <p>Deliberately dependency free, so the core has a working codec without pulling in a
 * serialisation library. Structured payloads should use a JSON or schema-aware codec.
 */
public final class StringCodec implements Codec {

    private static final String CONTENT_TYPE = "text/plain; charset=utf-8";

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        return String.valueOf(payload).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] body, Class<T> target) {
        String text = new String(body, StandardCharsets.UTF_8);
        if (target.isAssignableFrom(String.class)) {
            return (T) text;
        }
        throw new AceMqException(
                "StringCodec can only decode to String, but " + target.getName() + " was requested");
    }

    @Override
    public boolean canDecode(String contentType) {
        return contentType == null || contentType.startsWith("text/");
    }
}
