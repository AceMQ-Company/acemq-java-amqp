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

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

/**
 * Passes bytes through untouched.
 *
 * <p>For payloads that are already encoded by something else: an image, a file, a message
 * serialised by a library the application would rather call itself. Encoding twice is a common
 * and quiet mistake, and this is the way to say that it has been done once already.
 *
 * <p>Never offered to a message that did not ask for it. It would accept any bytes at all and
 * hand a consumer a {@code byte[]} where an object was expected, so it sits at the very back of
 * the order and only claims {@code application/octet-stream}.
 */
public final class BytesCodec implements Codec {

    private static final String CONTENT_TYPE = "application/octet-stream";

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        if (payload instanceof byte[]) {
            return (byte[]) payload;
        }
        throw new AceMqException("the bytes codec publishes byte[] and was given a "
                + payload.getClass().getName() + ". Publish with asJson() to have the object encoded,"
                + " or encode it yourself and publish the bytes.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] body, Class<T> target) {
        if (target.isAssignableFrom(byte[].class)) {
            return (T) body;
        }
        throw new AceMqException("the bytes codec decodes to byte[], and " + target.getName() + " was asked for");
    }

    @Override
    public boolean canDecode(@Nullable String contentType) {
        return contentType != null && contentType.startsWith(CONTENT_TYPE);
    }

    @Override
    public String toString() {
        return "BytesCodec";
    }
}
