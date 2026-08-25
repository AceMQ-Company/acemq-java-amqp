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
 * Handles one message.
 *
 * <p>The outcome is expressed by returning or throwing, which keeps the common case free of
 * ceremony:
 *
 * <ul>
 *   <li>return normally to accept the message;
 *   <li>throw {@link AceRetryableException} to send it to the next retry tier;
 *   <li>throw {@link AceFatalException} to dead-letter it without spending retries;
 *   <li>throw anything else and the configured error classifier decides, defaulting to retry.
 * </ul>
 *
 * <p>Handlers that want to state the outcome directly should implement
 * {@link AckAwareHandler} instead.
 *
 * <p>A handler must never sleep in order to retry. Retries are scheduled by the broker so
 * that a waiting message does not hold a delivery slot.
 *
 * @param <T> decoded payload type
 */
@FunctionalInterface
public interface MessageHandler<T> {

    /**
     * @param message the delivery to handle
     * @throws Exception to signal failure; the type decides whether it is retried
     */
    void handle(Message<T> message) throws Exception;
}
