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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * A message as a handler sees it: the decoded payload plus everything known about how it
 * arrived.
 *
 * <p>Implementations are immutable and safe to hand to another thread. Nothing here exposes
 * the underlying broker object, because a handler that reaches for a channel is a handler
 * that stops working when the transport changes.
 *
 * @param <T> decoded payload type
 */
public interface Message<T> {

    /** @return the decoded payload */
    T payload();

    /** @return the AceMQ metadata that travelled with this message */
    Envelope envelope();

    /** @return application headers; AceMQ-owned headers are reflected in {@link #envelope()} */
    Map<String, Object> headers();

    /** @return the routing key the message arrived with, when the broker supplies one */
    Optional<String> routingKey();

    /** @return the queue this delivery came from */
    String queue();

    /** @return when this process received the delivery */
    Instant receivedAt();

    /**
     * Convenience accessor for the delivery attempt.
     *
     * @return attempt number, starting at 1
     */
    default int attempt() {
        return envelope().attempt();
    }

    /**
     * Reports whether this is the first time the message has been delivered.
     *
     * <p>Handlers that are expensive to run but cheap to check may use this to decide whether
     * an idempotency lookup is worth doing.
     *
     * @return {@code true} when the attempt counter is still 1
     */
    default boolean isFirstAttempt() {
        return attempt() == 1;
    }

    /**
     * Returns a view of this message with a different payload, preserving all metadata.
     *
     * <p>Used by decoding and upcasting, which change the payload while leaving the delivery
     * untouched.
     *
     * @param payload replacement payload
     * @param <R> replacement payload type
     * @return a new message sharing this message's metadata
     */
    <R> Message<R> withPayload(R payload);
}
