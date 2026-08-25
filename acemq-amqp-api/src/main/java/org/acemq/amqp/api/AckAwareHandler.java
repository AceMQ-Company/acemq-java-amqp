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
 * Handles one message and states the outcome explicitly.
 *
 * <p>Preferable to {@link MessageHandler} when the decision is data rather than an error: a
 * handler that wants to retry after a broker-supplied delay, or to dead-letter with a reason
 * without manufacturing an exception, says so directly.
 *
 * @param <T> decoded payload type
 */
@FunctionalInterface
public interface AckAwareHandler<T> {

    /**
     * @param message the delivery to handle
     * @return what should happen to the delivery; must not be {@code null}
     */
    Ack handle(Message<T> message);
}
