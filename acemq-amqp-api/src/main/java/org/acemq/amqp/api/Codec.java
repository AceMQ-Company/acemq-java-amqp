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

import org.jspecify.annotations.Nullable;

/**
 * Turns payloads into bytes and back.
 *
 * <p>Selected by content type, so a queue can carry more than one representation and a
 * consumer can be migrated to a new format without a flag day.
 */
public interface Codec {

    /** @return the content type this codec produces, for example {@code application/json} */
    String contentType();

    /**
     * @param payload object to encode; never {@code null}
     * @return encoded bytes
     * @throws AceMqException if the payload cannot be encoded
     */
    byte[] encode(Object payload);

    /**
     * @param body encoded bytes
     * @param target type to produce
     * @param <T> target type
     * @return the decoded payload
     * @throws AceMqException if the bytes cannot be decoded, which the engine treats as a
     *     poison message rather than a retryable failure
     */
    <T> T decode(byte[] body, Class<T> target);

    /**
     * Decodes a received message, told what the sender said it was.
     *
     * <p>The engine calls this one, because a codec that dispatches between several formats
     * cannot choose without knowing the content type. A codec that handles a single format has
     * no use for it and need not override this.
     *
     * @param body encoded bytes
     * @param target type to produce
     * @param contentType content type the message arrived with, or {@code null} when the sender
     *     did not set one
     * @param <T> target type
     * @return the decoded payload
     * @throws AceMqException if the bytes cannot be decoded
     */
    default <T> T decode(byte[] body, Class<T> target, @Nullable String contentType) {
        return decode(body, target);
    }

    /**
     * @param contentType content type of a received message, or {@code null} when the sender did
     *     not set one
     * @return whether this codec can decode it
     */
    boolean canDecode(@Nullable String contentType);
}
