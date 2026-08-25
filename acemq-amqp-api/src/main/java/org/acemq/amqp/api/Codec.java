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
     * @param contentType content type of a received message, possibly {@code null}
     * @return whether this codec can decode it
     */
    boolean canDecode(String contentType);
}
