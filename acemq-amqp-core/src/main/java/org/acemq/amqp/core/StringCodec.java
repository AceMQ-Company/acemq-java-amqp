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
import org.jspecify.annotations.Nullable;

/**
 * Reads and writes UTF-8 text.
 *
 * <p>Dependency free, so the core has a working codec whatever else is on the classpath.
 * Structured payloads belong in JSON or a schema-aware format.
 *
 * <p>An object that is not text is refused rather than written out. This used to call
 * {@code String.valueOf}, which turned an object with no {@code toString} into
 * {@code OrderPlaced@4b1210ee} on the wire: published, confirmed, and useless to whoever read
 * it, with nothing anywhere reporting a problem. A publish that cannot mean what the caller
 * intended should fail at the publish.
 */
public final class StringCodec implements Codec {

    private static final String CONTENT_TYPE = "text/plain; charset=utf-8";

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        if (payload instanceof CharSequence) {
            return payload.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (payload instanceof Number || payload instanceof Boolean || payload instanceof Character) {
            // These have a toString that means what it says, and refusing them would be pedantry.
            return payload.toString().getBytes(StandardCharsets.UTF_8);
        }
        throw new AceMqException("the text codec publishes text and was given a "
                + payload.getClass().getName() + ", whose toString would go on the wire as-is."
                + " Publish with asJson() to have the object encoded, or send a String.");
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
    public boolean canDecode(@Nullable String contentType) {
        return contentType == null || contentType.startsWith("text/");
    }
}
