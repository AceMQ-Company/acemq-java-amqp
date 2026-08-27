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
package org.acemq.amqp.transport;

import java.util.Map;

/**
 * An open connection to one broker.
 *
 * <p>Implementations must be safe to use from several threads. Whatever multiplexing the
 * protocol requires — AMQP 0-9-1 channels, AMQP 1.0 sessions and links — is the transport's
 * business and is never exposed, because a caller holding a channel is a caller who breaks
 * when the transport changes.
 */
public interface TransportConnection extends AutoCloseable {

    /**
     * Declares an exchange, or verifies that an equivalent one exists.
     *
     * @param name exchange name
     * @param type routing behaviour: {@code direct}, {@code topic}, {@code fanout} or
     *     {@code headers}
     * @param durable whether it survives a broker restart
     * @throws TransportException if an incompatible exchange already exists
     */
    void declareExchange(String name, String type, boolean durable);

    /**
     * Declares a queue, or verifies that an equivalent one exists.
     *
     * @param name queue name
     * @param type queue implementation to request
     * @param durable whether it survives a broker restart
     * @param arguments broker-specific queue arguments, may be empty
     * @throws TransportException if an incompatible queue already exists. AMQP forbids
     *     changing most arguments in place, so this is reported rather than worked around.
     */
    void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments);

    /**
     * Binds a queue to an exchange.
     *
     * @param queue queue name
     * @param exchange exchange name
     * @param routingKey routing key or pattern
     */
    void bindQueue(String queue, String exchange, String routingKey);

    /**
     * Publishes one message and waits for the broker to confirm it.
     *
     * <p>Blocking is deliberate. An asynchronous publish that nobody waits on is the most
     * common way to lose messages, so the safe shape is the default one; batching and
     * pipelining belong to the core, which knows when they are appropriate.
     *
     * @param message the message to publish
     * @return what the broker said, including whether anything was bound to receive it
     * @throws TransportException if the connection failed or the confirm timed out
     */
    ConfirmResult send(OutboundMessage message);

    /**
     * Starts consuming a queue.
     *
     * @param queue queue to consume
     * @param prefetch maximum unsettled deliveries allowed at once; must be at least 1, and is
     *     the only backpressure that exists, so it is required rather than defaulted
     * @param listener receives each delivery
     * @return a handle that stops delivery when closed
     * @throws TransportException if the queue cannot be consumed
     */
    Subscription subscribe(String queue, int prefetch, DeliveryListener listener);

    /**
     * Starts consuming a queue with broker-specific consumer arguments.
     *
     * <p>Exists for streams. A stream consumer says where in the log to start with an
     * {@code x-stream-offset} argument, and there is nowhere else to put it: it belongs to the
     * subscription rather than to the queue, because two consumers of one stream read from
     * different positions.
     *
     * <p>The default refuses rather than ignores. Dropping the arguments would leave a consumer
     * that asked to replay a stream from the beginning quietly reading only new messages —
     * working, plausible, and missing everything it was written to process.
     *
     * @param queue queue to consume
     * @param prefetch maximum unsettled deliveries allowed at once
     * @param consumerArguments broker-specific arguments for this subscription
     * @param listener receives each delivery
     * @return a handle that stops delivery when closed
     * @throws TransportException if the queue cannot be consumed, or if this transport cannot
     *     honour the arguments given
     */
    default Subscription subscribe(
            String queue, int prefetch, Map<String, Object> consumerArguments, DeliveryListener listener) {
        if (consumerArguments == null || consumerArguments.isEmpty()) {
            return subscribe(queue, prefetch, listener);
        }
        throw new TransportException("this transport does not support consumer arguments, and was asked for "
                + consumerArguments.keySet() + " on queue '" + queue + "'. Consuming without them would read"
                + " from the wrong place rather than fail, so it is refused.");
    }

    /**
     * Removes a queue and everything in it.
     *
     * @param name queue to delete
     * @implNote provided for test fixtures and topology migration. Ordinary application code
     *     should not be deleting queues.
     */
    void deleteQueue(String name);

    /**
     * Reports whether a queue exists, without creating or altering it.
     *
     * <p>Separate from declaring because a planner has to be able to look before it
     * touches anything. Declaring a queue that already exists with different arguments
     * does not merely fail: on AMQP 0-9-1 it kills the channel, which is why a topology
     * applied blindly at start-up fails in production rather than in review.
     *
     * @param name queue name
     * @return {@code true} if the queue is already present
     */
    boolean queueExists(String name);

    /** @return whether the connection is usable */
    boolean isOpen();

    /** Closes the connection, letting in-flight deliveries settle first. */
    @Override
    void close();
}
