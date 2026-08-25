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
 * Sends messages of one type to one destination.
 *
 * <p>Publishers are thread safe and meant to be long lived: build one per message type at
 * startup and reuse it.
 *
 * @param <T> payload type
 */
public interface Publisher<T> extends AutoCloseable {

    /**
     * Publishes a message and waits for the broker to confirm it.
     *
     * @param payload the payload to send
     * @return the confirmed result
     * @throws PublishFailedException if the broker rejected the message, could not route it,
     *     or did not confirm in time
     */
    PublishResult send(T payload);

    /**
     * Publishes a message built on an explicit envelope.
     *
     * <p>Used when correlation or causation must be carried from a message being handled, so
     * that a whole flow can be followed in traces and logs.
     *
     * @param payload the payload to send
     * @param envelope metadata to publish it with
     * @return the confirmed result
     * @throws PublishFailedException as for {@link #send(Object)}
     */
    PublishResult send(T payload, Envelope envelope);

    @Override
    void close();
}
